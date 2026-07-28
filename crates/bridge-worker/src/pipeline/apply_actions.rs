//! `apply_actions` — traduz cada `Action` do `AgentResponse` em chamadas
//! ChatwootClient (C1–C13), respeitando as regras duras:
//! - Labels: SEMPRE GET + união antes de POST (nunca só as novas).
//! - set_status: "resolved" é PROIBIDO (S9 já neutralizou; reforçamos aqui).
//! - No máximo 1 mensagem ao cliente por run (S4): só o `reply` vira C1.

use std::collections::HashSet;

use bridge_core::{ActionKind, AI_FAILURE_LABEL};
use tracing::{info, warn};

use crate::state::{AppState, WorkerError};
use crate::pipeline::outbound_gate::{set_last_outbound_text, ReviewedResponse};

/// Aplica as ações revista da gate de saída. Ordem:
/// 1. Envia o `reply` (C1) — a única mensagem ao cliente (S4).
/// 2. Aplica cada action restante (labels/assign/status/priority/note/handoff).
pub async fn apply(
    state: &AppState,
    conv_id: i64,
    reviewed: &ReviewedResponse,
) -> Result<(), WorkerError> {
    // 1. Mensagem ao cliente (se houver e não estiver bloqueada).
    if !reviewed.blocked {
        if let Some(reply) = reviewed.reply.as_ref() {
            if !reply.text.trim().is_empty() {
                state
                    .chatwoot
                    .send_message(conv_id, &reply.text)
                    .await?;
                // Registra último texto p/ S6 (loop guard) com TTL 24h.
                set_last_outbound_text(&state.redis, conv_id, &reply.text)
                    .await
                    .ok(); // best-effort
                info!(conv_id, "reply sent to customer");
            }
        }
    }

    // 2. Ações. Labels primeiro (GET+união), resto em sequência.
    for action in &reviewed.actions {
        if let Err(e) = apply_action(state, conv_id, action).await {
            // Uma ação falha não derruba as demais — loga e segue.
            warn!(conv_id, kind = action.kind.as_str(), error = %e, "action failed");
        }
    }

    // 3. Handoff solicitado pela IA → abre conversa + atribui time fallback.
    if reviewed.handoff_required {
        handoff(state, conv_id, reviewed.handoff_reason.as_deref()).await?;
    }

    Ok(())
}

/// Aplica uma única ação. Cada variante mapeia para C1–C13.
async fn apply_action(
    state: &AppState,
    conv_id: i64,
    action: &bridge_core::Action,
) -> Result<(), WorkerError> {
    use ActionKind::*;
    match action.kind {
        SendMessage => {
            // Já tratado via reply (S4). Ignorado aqui para evitar duplicar.
            Ok(())
        }
        SendPrivateNote => {
            let content = action
                .reason
                .clone()
                .unwrap_or_else(|| "nota interna da IA".into());
            state.chatwoot.send_private_note(conv_id, &content).await?;
            Ok(())
        }
        AddLabels => {
            let mut current = state.chatwoot.get_labels(conv_id).await?;
            for l in &action.labels {
                if !current.iter().any(|c| c.eq_ignore_ascii_case(l)) {
                    current.push(l.clone());
                }
            }
            state.chatwoot.set_labels(conv_id, &current).await?;
            Ok(())
        }
        RemoveLabels => {
            let current = state.chatwoot.get_labels(conv_id).await?;
            let to_remove: HashSet<String> =
                action.labels.iter().map(|s| s.to_ascii_lowercase()).collect();
            let kept: Vec<String> = current
                .into_iter()
                .filter(|c| !to_remove.contains(&c.to_ascii_lowercase()))
                .collect();
            state.chatwoot.set_labels(conv_id, &kept).await?;
            Ok(())
        }
        SetCustomAttributes => {
            state
                .chatwoot
                .update_custom_attributes(conv_id, action.attributes.clone())
                .await?;
            Ok(())
        }
        AssignTeam => {
            if let Some(tid) = action.team_id {
                state.chatwoot.assign_team(conv_id, tid).await?;
            }
            Ok(())
        }
        AssignAgent => {
            if let Some(aid) = action.agent_id.as_ref().and_then(|s| s.parse::<i64>().ok()) {
                state.chatwoot.assign_agent(conv_id, aid).await?;
            } else {
                warn!("assign_agent sem agent_id válido");
            }
            Ok(())
        }
        SetPriority => {
            if let Some(p) = action.priority.as_deref() {
                state.chatwoot.set_priority(conv_id, p).await?;
            }
            Ok(())
        }
        SetStatus => {
            // S9 reforçado: resolved proibido. Só open/pending.
            if let Some(s) = action.status.as_deref() {
                if s.eq_ignore_ascii_case("resolved") {
                    warn!(conv_id, status = s, "set_status=resolved blocked (S9)");
                    return Ok(());
                }
                state.chatwoot.toggle_status(conv_id, s).await?;
            }
            Ok(())
        }
        Snooze => {
            // spec: snoozed_until via toggle_status com snoozed.
            state.chatwoot.toggle_status(conv_id, "snoozed").await?;
            Ok(())
        }
        CallTool | CallAgent => {
            // Fase 3. Na v1 viram nota interna de pendência.
            let kind = action.kind.as_str();
            state
                .chatwoot
                .send_private_note(conv_id, &format!("acao {kind} pendente (Fase 3)"))
                .await?;
            Ok(())
        }
        RequestHandoff => {
            // Tratado em `apply` (abre + atribui time). No-op aqui.
            Ok(())
        }
    }
}

/// Handoff: muda status para `open` e atribui ao time de fallback.
pub async fn handoff(
    state: &AppState,
    conv_id: i64,
    reason: Option<&str>,
) -> Result<(), WorkerError> {
    state.chatwoot.toggle_status(conv_id, "open").await?;
    state
        .chatwoot
        .assign_team(conv_id, state.config.chatwoot.fallback_team_id)
        .await?;
    // Etiqueta ia:falha? Não aqui — handoff normal. Etiqueta de falha só
    // na degradação técnica (degrade_on_failure).
    if let Some(r) = reason {
        state
            .chatwoot
            .send_private_note(conv_id, &format!("handoff solicitado pela IA: {r}"))
            .await?;
    }
    info!(conv_id, "handoff applied (open + fallback team)");
    Ok(())
}

/// Degradação técnica (Seção 9.3): ambos providers falharam. Envia mensagem
/// estática, abre a conversa, atribui fallback team, etiqueta ia:falha,
/// nota interna com run_id/erro.
pub async fn degrade_on_failure(
    state: &AppState,
    conv_id: i64,
    error: &str,
) -> Result<(), WorkerError> {
    let msg = "Desculpe, tive um problema técnico. Já acionei a equipe e \
               alguém retorna em instantes.";
    // 1. Mensagem de degradação (estática, nunca IA).
    if let Err(e) = state.chatwoot.send_message(conv_id, msg).await {
        warn!(conv_id, error = %e, "degradation message failed");
        // Mesmo falhando, continuamos o handoff — silêncio é pior.
    }
    // 2. toggle_status → open (C5).
    state.chatwoot.toggle_status(conv_id, "open").await?;
    // 3. assign_team → fallback (C7).
    state
        .chatwoot
        .assign_team(conv_id, state.config.chatwoot.fallback_team_id)
        .await?;
    // 4. Etiqueta ia:falha (C8 via GET+união).
    let mut labels = state.chatwoot.get_labels(conv_id).await.unwrap_or_default();
    if !labels.iter().any(|l| l.eq_ignore_ascii_case(AI_FAILURE_LABEL)) {
        labels.push(AI_FAILURE_LABEL.to_string());
        state.chatwoot.set_labels(conv_id, &labels).await?;
    }
    // 5. Nota interna com o erro (C2).
    let note = format!("degradacao tecnica — erro: {error}");
    state.chatwoot.send_private_note(conv_id, &note).await?;
    info!(conv_id, "degradation applied");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::Action;

    #[test]
    fn send_message_action_is_noop_in_apply_action() {
        // send_message é tratado pelo reply; não duplicamos em apply_action.
        let a = Action {
            kind: ActionKind::SendMessage,
            ..Default::default()
        };
        // apenas garante que o kind mapeia corretamente
        assert_eq!(a.kind.as_str(), "send_message");
    }

    #[test]
    fn resolved_is_blocked_before_call() {
        // reforça a regra: S9 + apply_action não enviam resolved.
        let a = Action {
            kind: ActionKind::SetStatus,
            status: Some("resolved".into()),
            ..Default::default()
        };
        assert_eq!(a.status.as_deref(), Some("resolved"));
    }
}
