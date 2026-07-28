//! `outbound_gate` — Gate de Saída, validações S1–S12 (Seção 8.2).
//!
//! Nada sai da ponte sem passar por aqui. A gate **modifica** a resposta
//! (trunca, redige, remove ações inválidas) e registra cada decisão em
//! `gate_decision`. Diferente da gate de entrada, raramente bloqueia — só
//! em caso de loop evidente (S6) ou envelope irrecuperável (S1).

use std::sync::OnceLock;

use bridge_core::{Action, ActionKind, AgentResponse, Config, ConversationId, Reply};
use redis::AsyncCommands;
use regex::Regex;
use strsim::jaro_winkler;
use tracing::warn;

use crate::state::{record_gate_decision, ConversationState, WorkerError};

/// Resposta revista pela gate.
#[derive(Debug, Clone)]
pub struct ReviewedResponse {
    pub run_id: Option<String>,
    pub reply: Option<Reply>,
    pub actions: Vec<Action>,
    pub handoff_required: bool,
    pub handoff_reason: Option<String>,
    pub modifications: Vec<&'static str>,
    pub blocked: bool,
    pub block_reason: Option<String>,
}

impl ReviewedResponse {
    fn from_response(r: AgentResponse) -> Self {
        Self {
            run_id: r.run_id,
            reply: r.reply,
            actions: r.actions,
            handoff_required: r.handoff.required,
            handoff_reason: r.handoff.reason,
            modifications: Vec::new(),
            blocked: false,
            block_reason: None,
        }
    }
}

/// Marcadores internos que não podem vazar (S7).
const INTERNAL_MARKERS: &[&str] = &[
    "<thinking>",
    "</thinking>",
    "<system>",
    "</system>",
    "system:",
    "assistant:",
    "tool:",
    "<tool_call>",
];

/// Regex de PII proibida (S5): CPF, CNPJ, cartão, senha/token.
fn pii_regex() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        // ponytail: padrões brasileiros. CPF (11 dígitos, separadores opt.),
        // CNPJ (14 dígitos), cartão (13–16 dígitos), e keywords de segredo.
        Regex::new(
            r"(?i)\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b|\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b|\b(?:\d[ -]?){13,16}\b|\b(?:senha|token|api[_-]?key)\b\s*[:=]\s*\S+",
        ).expect("valid pii regex")
    })
}

/// Avalia a resposta da IA. Não faz I/O direto — a similaridade S6 recebe o
/// último texto via parâmetro, lido pelo pipeline no Redis.
pub fn evaluate(
    response: &AgentResponse,
    state: &ConversationState,
    config: &Config,
    last_reply: Option<&str>,
) -> ReviewedResponse {
    let mut reviewed = ReviewedResponse::from_response(response.clone());

    // S2 — actions[].kind ∈ enum fechado (já tipado na desserialização).
    // Aqui filtramos ações cujo kind não está na whitelist da v1.
    let before = reviewed.actions.len();
    reviewed.actions.retain(|a| is_allowed_action(a.kind));
    if reviewed.actions.len() != before {
        reviewed.modifications.push("S2");
    }

    // S9 — set_status != resolved. Neutraliza status=resolved.
    for a in reviewed.actions.iter_mut() {
        if a.kind == ActionKind::SetStatus {
            if let Some(s) = a.status.as_deref() {
                if s.eq_ignore_ascii_case("resolved") {
                    warn!(status = s, "S9: dropping set_status=resolved");
                    a.status = None;
                    reviewed.modifications.push("S9");
                }
            }
        }
    }

    // S1 — envelope válido. Reply vazio + sem actions + sem handoff = vazio.
    let has_reply = reviewed
        .reply
        .as_ref()
        .map(|r| !r.text.trim().is_empty())
        .unwrap_or(false);
    if !has_reply && reviewed.actions.is_empty() && !reviewed.handoff_required {
        reviewed.blocked = true;
        reviewed.block_reason = Some("S1:empty_envelope".into());
        reviewed.modifications.push("S1");
        return reviewed;
    }

    // S4 — no máximo 1 mensagem ao cliente. send_message é implícito via
    // reply; ações explícitas de send_message são descartadas.
    let send_msg_count = reviewed
        .actions
        .iter()
        .filter(|a| a.kind == ActionKind::SendMessage)
        .count();
    if send_msg_count > 0 {
        reviewed.actions.retain(|a| a.kind != ActionKind::SendMessage);
        reviewed.modifications.push("S4");
    }

    if let Some(reply) = reviewed.reply.as_mut() {
        // S3 — reply.text ≤ max_output_chars. Trunca em fronteira de frase.
        let max = config.agent.max_output_chars;
        if reply.text.chars().count() > max {
            reply.text = truncate_to_sentence(&reply.text, max);
            reviewed.modifications.push("S3");
        }

        // S5 — PII proibida: redige com [REDIGIDO].
        if pii_regex().is_match(&reply.text) {
            reply.text = pii_regex()
                .replace_all(&reply.text, "[REDIGIDO]")
                .to_string();
            reviewed.modifications.push("S5");
            warn!(conv_id = state.conversation_id, "S5: PII redacted");
        }

        // S7 — marcadores internos vazados: remove.
        let mut leaked = false;
        for marker in INTERNAL_MARKERS {
            if reply.text.contains(marker) {
                reply.text = reply.text.replace(marker, "");
                leaked = true;
            }
        }
        if leaked {
            reviewed.modifications.push("S7");
        }

        // S8 — links fora de ALLOWED_LINK_DOMAINS. Ponytail: checagem de
        // host real exige URL parser; deixamos a cargo do caller quando
        // houver allowlist configurada explicitamente. Stub não-remove.
        let _ = config.agent.allowed_link_domains.clone();

        // S6 — similaridade > 0.95 com a última mensagem enviada (loop).
        if let Some(last) = last_reply {
            let sim = jaro_winkler(
                &normalize_for_cmp(&reply.text),
                &normalize_for_cmp(last),
            );
            if sim > 0.95 {
                reviewed.blocked = true;
                reviewed
                    .block_reason
                    .replace(format!("S6:loop similarity={sim:.3}"));
                reviewed.modifications.push("S6");
                warn!(conv_id = state.conversation_id, sim, "S6: reply too similar");
            }
        }
    }

    // S10 — ação irreversível (call_tool). Na v1 não executamos (Fase 3);
    // removemos. apply_actions cria nota interna de aprovação.
    let irreversible = reviewed
        .actions
        .iter()
        .any(|a| a.kind == ActionKind::CallTool);
    if irreversible {
        reviewed.actions.retain(|a| a.kind != ActionKind::CallTool);
        reviewed.modifications.push("S10");
        warn!(conv_id = state.conversation_id, "S10: call_tool deferred");
    }

    // S11 (L3) e S12 (WhatsApp 24h) são checados no apply_actions, que fala
    // com Redis/Chatwoot. Aqui só auditoria.

    reviewed
}

/// Registra as decisões da gate de saída em `gate_decision` (best-effort).
pub async fn record(
    pool: &sqlx::PgPool,
    conv_id: ConversationId,
    reviewed: &ReviewedResponse,
) -> Result<(), WorkerError> {
    if reviewed.blocked {
        let reason = reviewed.block_reason.clone().unwrap_or_default();
        let rule = reviewed.modifications.last().copied().unwrap_or("S?");
        record_gate_decision(
            pool,
            conv_id,
            "outbound",
            rule,
            "block",
            Some(&serde_json::json!({ "reason": reason })),
        )
        .await?;
    } else if !reviewed.modifications.is_empty() {
        let mods = serde_json::json!({ "applied": reviewed.modifications });
        let rule = reviewed.modifications.last().copied().unwrap_or("S?");
        record_gate_decision(pool, conv_id, "outbound", rule, "modify", Some(&mods)).await?;
    }
    Ok(())
}

/// Lê o último texto enviado ao cliente (para S6). Armazenado em
/// `lastout:{conv_id}` com TTL 24h, escrito pelo apply_actions após C1.
pub async fn last_outbound_text(
    redis: &deadpool_redis::Pool,
    conv_id: ConversationId,
) -> Result<Option<String>, WorkerError> {
    let mut conn = redis.get().await?;
    let v: Option<String> = conn.get(format!("lastout:{conv_id}")).await?;
    Ok(v)
}

/// Persiste o último texto enviado (chamado pelo apply_actions após C1).
pub async fn set_last_outbound_text(
    redis: &deadpool_redis::Pool,
    conv_id: ConversationId,
    text: &str,
) -> Result<(), WorkerError> {
    let mut conn = redis.get().await?;
    let _: () = conn.set_ex(format!("lastout:{conv_id}"), text, 86_400).await?;
    Ok(())
}

// ---- helpers ----

fn is_allowed_action(kind: ActionKind) -> bool {
    // ponytail: whitelist da v1. call_tool/call_agent fora (Fase 3).
    matches!(
        kind,
        ActionKind::SendMessage
            | ActionKind::SendPrivateNote
            | ActionKind::AddLabels
            | ActionKind::RemoveLabels
            | ActionKind::SetCustomAttributes
            | ActionKind::AssignTeam
            | ActionKind::AssignAgent
            | ActionKind::SetPriority
            | ActionKind::SetStatus
            | ActionKind::Snooze
            | ActionKind::RequestHandoff
    )
}

fn truncate_to_sentence(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().take(max_chars).collect();
    let mut end = chars.len();
    for (i, c) in chars.iter().enumerate().rev() {
        if matches!(*c, '.' | '?' | '!' | '\n') {
            end = i + 1;
            break;
        }
    }
    let mut out: String = chars[..end].iter().collect();
    if !out.ends_with("[...]") {
        out.push_str("[...]");
    }
    out
}

fn normalize_for_cmp(s: &str) -> String {
    s.to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::{Action, AgentResponse, Reply};

    fn resp(text: &str) -> AgentResponse {
        AgentResponse {
            reply: Some(Reply { text: text.into(), content_type: Some("text".into()) }),
            actions: vec![],
            ..Default::default()
        }
    }

    fn st() -> ConversationState {
        ConversationState::default()
    }

    #[test]
    fn s3_truncates_long_reply() {
        let cfg = Config::default();
        let long = "a".repeat(5000);
        let r = evaluate(&resp(&long), &st(), &cfg, None);
        assert!(r.reply.unwrap().text.ends_with("[...]"));
        assert!(r.modifications.contains(&"S3"));
    }

    #[test]
    fn s5_redacts_cpf() {
        let cfg = Config::default();
        let r = evaluate(&resp("meu cpf e 123.456.789-09"), &st(), &cfg, None);
        let t = r.reply.unwrap().text;
        assert!(t.contains("[REDIGIDO]"));
        assert!(!t.contains("123.456.789-09"));
    }

    #[test]
    fn s9_drops_resolved_status() {
        let cfg = Config::default();
        let mut s = resp("ok");
        s.actions = vec![Action {
            kind: ActionKind::SetStatus,
            status: Some("resolved".into()),
            ..Default::default()
        }];
        let r = evaluate(&s, &st(), &cfg, None);
        assert!(r.actions[0].status.is_none());
        assert!(r.modifications.contains(&"S9"));
    }

    #[test]
    fn s6_blocks_identical_reply() {
        let cfg = Config::default();
        let r = evaluate(&resp("bom dia"), &st(), &cfg, Some("bom dia"));
        assert!(r.blocked);
        assert!(r.block_reason.as_deref().unwrap().starts_with("S6"));
    }

    #[test]
    fn s6_passes_different_reply() {
        let cfg = Config::default();
        let r = evaluate(
            &resp("sua guia do DAS vence dia 20"),
            &st(),
            &cfg,
            Some("bom dia, tudo bem?"),
        );
        assert!(!r.blocked);
    }

    #[test]
    fn s4_drops_explicit_send_message() {
        let cfg = Config::default();
        let mut s = resp("oi");
        s.actions = vec![Action {
            kind: ActionKind::SendMessage,
            ..Default::default()
        }];
        let r = evaluate(&s, &st(), &cfg, None);
        assert!(r.actions.iter().all(|a| a.kind != ActionKind::SendMessage));
    }

    #[test]
    fn s7_strips_thinking_tags() {
        let cfg = Config::default();
        let r = evaluate(&resp("<thinking>plan</thinking>resposta"), &st(), &cfg, None);
        assert!(!r.reply.unwrap().text.contains("thinking"));
    }

    #[test]
    fn s1_blocks_empty_envelope() {
        let cfg = Config::default();
        let s = AgentResponse::default();
        let r = evaluate(&s, &st(), &cfg, None);
        assert!(r.blocked);
    }

    #[test]
    fn s10_defers_call_tool() {
        let cfg = Config::default();
        let mut s = resp("ok");
        s.actions = vec![Action {
            kind: ActionKind::CallTool,
            tool: Some("erp.emitir_nf".into()),
            ..Default::default()
        }];
        let r = evaluate(&s, &st(), &cfg, None);
        assert!(r.actions.is_empty());
        assert!(r.modifications.contains(&"S10"));
    }

    #[test]
    fn truncate_keeps_sentence_boundary() {
        let t = truncate_to_sentence("uma frase. outra coisa que continua", 15);
        assert!(t.ends_with("[...]"));
        assert!(t.contains("frase."));
    }
}
