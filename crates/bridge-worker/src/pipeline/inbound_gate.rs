//! `inbound_gate` — Gate de Entrada, regras G1–G11 (Seção 8.1).
//!
//! Executado no worker, antes de qualquer chamada à IA. **Primeiro BLOCK
//! encerra a avaliação.** Todo BLOCK gera linha em `gate_decision` (feito
//! pelo pipeline, não aqui — a gate é pura: só decide).
//!
//! G1–G4 olham as mensagens do turno (drenadas do buffer). Como o buffer só
//! recebe mensagens de contato (o `bridge-api` já descarta private/echo/
//! não-incoming na ingestão), G1–G3 raramente disparam aqui — mas ficam
//! como defesa em profundidade (never trust the buffer).

use bridge_core::{is_block_label, ActionKind, AiState, Config, InboundMessage};
use chrono::Utc;

use crate::state::ConversationState;

/// Decisão da gate. `Block` carrega a regra (G1..G11) e o reason.
#[derive(Debug, Clone)]
pub enum GateDecision {
    Allow,
    Block { rule: String, reason: String },
}

impl GateDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
    fn block(rule: &str, reason: impl Into<String>) -> Self {
        Self::Block { rule: rule.to_string(), reason: reason.into() }
    }
}

/// Avalia G1–G11 na ordem. Ponytail: gate pura (sem I/O) exceto por G9, que
/// exigiria Lua no Redis — G9 é deixado para o pipeline como verificação
/// prévia (rate limit em fila) e marcado aqui como allow. Ver `evaluate_g9`.
pub async fn evaluate(
    state: &ConversationState,
    config: &Config,
    turn: &[InboundMessage],
) -> Result<GateDecision, crate::state::WorkerError> {
    let first = turn.first();

    // G1 — private note. (No worker o turno só tem inbound de contato, mas a
    // defesa custa nada.)
    if let Some(m) = first {
        if m.sender_kind.eq_ignore_ascii_case("private") || m.content.starts_with("/private") {
            return Ok(GateDecision::block("G1", "private_note"));
        }
    }

    // G2 — eco da própria IA (sender_type == AgentBot).
    if let Some(m) = first {
        if m.sender_kind.eq_ignore_ascii_case("AgentBot") {
            return Ok(GateDecision::block("G2", "own_echo"));
        }
    }

    // G3 — não-incoming. O bridge-api só enfileira `incoming`+`contact`, mas
    // a defesa fica: qualquer sender_kind que não seja contact (e não vazio,
    // que tratamos como "desconhecido, tolerar") é block. AgentBot já saiu em G2.
    if let Some(m) = first {
        let kind = m.sender_kind.as_str();
        if !kind.is_empty() && !kind.eq_ignore_ascii_case("contact") {
            return Ok(GateDecision::block("G3", "not_incoming"));
        }
    }

    // G4 — conteúdo vazio E sem anexo.
    if let Some(m) = first {
        let empty_text = m.content.trim().is_empty();
        let no_attachment = !m.has_attachment && m.attachments.is_empty();
        if empty_text && no_attachment {
            return Ok(GateDecision::block("G4", "empty"));
        }
    }

    // G5 — conversa em estado não-ativo (human_handling / ai_paused_* / closed
    // / awaiting_human).
    if !state.ai_state.can_ai_respond() {
        return Ok(GateDecision::block("G5", format!("state:{}", state.ai_state.as_str())));
    }

    // G6 — etiqueta de bloqueio (ia:off, humano, juridico).
    for label in &state.labels {
        if is_block_label(label) || config.agent.ai_block_labels.iter().any(|b| b.eq_ignore_ascii_case(label)) {
            return Ok(GateDecision::block("G6", format!("label:{label}")));
        }
    }

    // G7 — inbox não habilitado para IA.
    if !config.chatwoot.ai_enabled_inboxes.is_empty()
        && !config.chatwoot.ai_enabled_inboxes.contains(&state.inbox_id)
    {
        return Ok(GateDecision::block("G7", format!("inbox_not_enabled:{}", state.inbox_id)));
    }

    // G8 — guard anti-loop: turnos consecutivos da IA ≥ teto.
    if state.prior_ai_turns_in_row >= config.rate_limits.max_consecutive_ai_turns as u16 {
        return Ok(GateDecision::block("G8", "loop_guard:handoff_required"));
    }

    // G9 — limitadores L1–L6.
    // ponytail: verificação real é script Lua atômico no Redis (spec 6.5), e o
    // crate bridge-store não expõe o módulo de ratelimit ainda. O pipeline
    // faz a checagem prévia (em fila) via acquire_lock+semaphore; G9 aqui fica
    // como allow. Implementar `RateLimits::check(&conn)` quando o módulo existir.
    if let Some(d) = evaluate_g9(state, config).await? {
        return Ok(d);
    }

    // G10 — fora do horário comercial e AFTER_HOURS_MODE=static.
    match config.agent.after_hours_mode.as_str() {
        "off" => {
            // IA desligada fora do horário. ponytail: sem business_hours real
            // ainda (bridge-scheduler); assumimos Within sempre. Quando o
            // scheduler publicar o estado, plugar aqui.
        }
        "static" => {
            // Fora do horário → block + mensagem estática (feita pelo caller).
            if !is_within_business_hours() {
                return Ok(GateDecision::block("G10", "after_hours:static"));
            }
        }
        _ => { /* "ai" — IA responde 24/7 */ }
    }

    // G11 — kill switch global.
    if !config.agent.ai_enabled {
        return Ok(GateDecision::block("G11", "kill_switch"));
    }

    Ok(GateDecision::Allow)
}

/// G9 — limitadores em cascata. Ponytail: stub que retorna None (allow).
/// A gate é pura (sem I/O); a checagem real de L1–L6 é feita no pipeline,
/// onde temos `AppState` (Redis + PgPool). O orçamento diário (L6) é checado
/// em `run_agent` via `spent_today`. Quando `bridge-store::redis::ratelimit`
/// existir, mover a checagem de L1–L5 para cá com a conn passada pelo caller.
async fn evaluate_g9(
    _state: &ConversationState,
    _config: &Config,
) -> Result<Option<GateDecision>, crate::state::WorkerError> {
    Ok(None)
}

/// Best-effort de horário comercial. Ponytail: sempre `true` até o
/// `bridge-scheduler` publicar `BusinessHoursState` real (config/business_hours.toml).
fn is_within_business_hours() -> bool {
    let _ = Utc::now();
    true
}

/// Ações permitidas para o turno, derivadas da config. Usado para montar o
/// `allowed_actions` do request — vive aqui porque é regra de gate.
pub fn allowed_actions_for_turn(config: &Config) -> Vec<ActionKind> {
    // espelha build_context; call_tool/call_agent desligados na Fase 1.
    let _ = config;
    vec![
        ActionKind::SendMessage,
        ActionKind::SendPrivateNote,
        ActionKind::AddLabels,
        ActionKind::RemoveLabels,
        ActionKind::SetCustomAttributes,
        ActionKind::AssignTeam,
        ActionKind::AssignAgent,
        ActionKind::SetPriority,
        ActionKind::SetStatus,
        ActionKind::Snooze,
        ActionKind::RequestHandoff,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::InboundMessage;

    fn state() -> ConversationState {
        ConversationState {
            ai_state: AiState::AiActive,
            ..Default::default()
        }
    }

    fn turn() -> Vec<InboundMessage> {
        vec![InboundMessage {
            id: 1,
            content: "bom dia".into(),
            sender_kind: "contact".into(),
            created_at: "2026-07-28T12:00:00Z".into(),
            ..Default::default()
        }]
    }

    #[tokio::test]
    async fn allows_happy_path() {
        let d = evaluate(&state(), &Config::default(), &turn()).await.unwrap();
        assert!(d.is_allowed());
    }

    #[tokio::test]
    async fn g4_blocks_empty_no_attachment() {
        let mut t = turn();
        t[0].content = "   ".into();
        let d = evaluate(&state(), &Config::default(), &t).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G4"));
    }

    #[tokio::test]
    async fn g5_blocks_when_paused() {
        let mut s = state();
        s.ai_state = AiState::AiPausedManual;
        let d = evaluate(&s, &Config::default(), &turn()).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G5"));
    }

    #[tokio::test]
    async fn g6_blocks_on_block_label() {
        let mut s = state();
        s.labels = vec!["ia:off".into()];
        let d = evaluate(&s, &Config::default(), &turn()).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G6"));
    }

    #[tokio::test]
    async fn g7_blocks_when_inbox_not_enabled() {
        let mut cfg = Config::default();
        cfg.chatwoot.ai_enabled_inboxes = vec![3];
        let mut s = state();
        s.inbox_id = 9;
        let d = evaluate(&s, &cfg, &turn()).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G7"));
    }

    #[tokio::test]
    async fn g8_blocks_loop_guard() {
        let mut s = state();
        s.prior_ai_turns_in_row = 4; // == default max
        let d = evaluate(&s, &Config::default(), &turn()).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G8"));
    }

    #[tokio::test]
    async fn g11_blocks_on_kill_switch() {
        let mut cfg = Config::default();
        cfg.agent.ai_enabled = false;
        let d = evaluate(&state(), &cfg, &turn()).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G11"));
    }

    #[tokio::test]
    async fn g2_blocks_agent_bot_echo() {
        let mut t = turn();
        t[0].sender_kind = "AgentBot".into();
        let d = evaluate(&state(), &Config::default(), &t).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G2"));
    }

    #[tokio::test]
    async fn first_block_wins() {
        // kill switch + paused + loop: G5 vem antes de G11, então vence.
        let mut cfg = Config::default();
        cfg.agent.ai_enabled = false;
        let mut s = state();
        s.ai_state = AiState::AiPausedManual;
        let d = evaluate(&s, &cfg, &turn()).await.unwrap();
        assert!(matches!(d, GateDecision::Block { rule, .. } if rule == "G5"));
    }
}
