//! `build_context` — monta `ConversationContext` + `AgentRequest` (Seção 5.2).
//!
//! Aqui o histórico truncado (`history_digest`) viria do `message_log`. O
//! crate `bridge-store` ainda não expõe leitura de histórico, então a v1
//! constrói o contexto **sem** histórico — a continuidade vem de
//! `provider_session_id` (encadeamento de turno, spec 5.4) e das mensagens
//! do próprio turno. ponytail: preencher `history_digest` quando o
//! `bridge-store` ganhar `message_log::recent_for_conv`.

use bridge_core::{
    AgentRequest, BusinessHoursState, Config, ContactSummary, ConversationContext,
    ConversationId, RunId,
};

use crate::state::ConversationState;
use crate::pipeline::collect_turn::CollectedTurn;

/// Limite de mensagens no `history_digest` (spec 5.3 / 10.4: default 20).
const HISTORY_LIMIT: usize = 20;

/// Monta o `AgentRequest` pronto para o provider.
pub fn build_agent_request(
    config: &Config,
    run_id: RunId,
    conv_id: ConversationId,
    state: &ConversationState,
    turn: &CollectedTurn,
    _trigger_reason: Option<&str>,
) -> AgentRequest {
    let context = build_context(config, conv_id, state, turn);

    // Whitelist de ações permitidas para o agente de triagem na v1 (spec 5.9):
    // call_agent está no enum mas desabilitado (Fase 1). call_tool idem.
    let allowed_actions = vec![
        bridge_core::ActionKind::SendMessage,
        bridge_core::ActionKind::SendPrivateNote,
        bridge_core::ActionKind::AddLabels,
        bridge_core::ActionKind::RemoveLabels,
        bridge_core::ActionKind::SetCustomAttributes,
        bridge_core::ActionKind::AssignTeam,
        bridge_core::ActionKind::AssignAgent,
        bridge_core::ActionKind::SetPriority,
        bridge_core::ActionKind::SetStatus,
        bridge_core::ActionKind::Snooze,
        bridge_core::ActionKind::RequestHandoff,
        // call_tool / call_agent: proibidos na Fase 1 (registry desligado).
    ];

    AgentRequest {
        run_id,
        session_key: bridge_core::session_key(state.account_id, conv_id),
        agent_id: Some(config.agent.openclaw_agent_id.clone()),
        turn: turn.messages.clone(),
        context,
        allowed_actions,
        deadline_ms: config.agent.timeout_ms,
        max_output_chars: config.agent.max_output_chars,
        locale: "pt-BR".to_string(),
    }
}

fn build_context(
    _config: &Config,
    conv_id: ConversationId,
    state: &ConversationState,
    _turn: &CollectedTurn,
) -> ConversationContext {
    ConversationContext {
        conversation_id: conv_id,
        inbox_channel: normalize_channel(&state.channel),
        contact: Some(ContactSummary {
            id: state.contact_id,
            name: String::new(), // preenchido via Chatwoot/CRM na Fase 3
            phone_masked: String::new(),
            email_masked: String::new(),
        }),
        client: None, // vinculação ERP — Fase 2 (10.6)
        labels: state.labels.clone(),
        assignee: state.assignee_id.map(|id| bridge_core::AgentSummary {
            id,
            name: String::new(),
            email: String::new(),
        }),
        business_hours: current_business_hours(),
        history_digest: Vec::with_capacity(HISTORY_LIMIT),
        prior_ai_turns_in_row: state.prior_ai_turns_in_row as u8,
    }
}

/// Normaliza o canal do Chatwoot (`Channel::Whatsapp` → `whatsapp`).
fn normalize_channel(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    let channel = lower
        .split("::")
        .last()
        .unwrap_or(&lower)
        .trim_start_matches("channel::");
    match channel {
        "whatsapp" | "api" => "whatsapp".to_string(),
        "instagram" => "instagram".to_string(),
        "email" => "email".to_string(),
        "webwidget" | "widget" => "widget".to_string(),
        "" => "whatsapp".to_string(), // default sensato
        other => other.to_string(),
    }
}

/// Horário comercial corrente. ponytail: cálculo real (business_hours.toml +
/// feriados) fica no `bridge-scheduler`. Aqui assumimos `Within` e deixamos
/// o gate G10 decidir com base em `AFTER_HOURS_MODE`. Trocar por consulta ao
/// scheduler quando ele estiver pronto.
fn current_business_hours() -> BusinessHoursState {
    BusinessHoursState::Within
}

/// Trunca o conteúdo de entrada ao teto L7 e devolve um novo turno.
/// Usado quando o conteúdo somado excede o teto — aplicado antes de montar o
/// request.
pub fn truncate_turn(turn: &mut CollectedTurn, max_chars: usize) {
    super::collect_turn::apply_input_limit(&mut turn.messages, max_chars);
}

/// Helper de teste: monta um turno mínimo a partir de mensagens.
#[cfg(test)]
pub fn turn_from(messages: Vec<InboundMessage>) -> CollectedTurn {
    CollectedTurn { messages }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::InboundMessage;

    #[test]
    fn normalizes_chatwoot_channel_enum() {
        assert_eq!(normalize_channel("Channel::Whatsapp"), "whatsapp");
        assert_eq!(normalize_channel("Channel::Instagram::Direct"), "instagram");
        assert_eq!(normalize_channel("Email::Channel"), "email");
        assert_eq!(normalize_channel("Channel::WebWidget"), "widget");
        assert_eq!(normalize_channel(""), "whatsapp");
    }

    #[test]
    fn request_has_session_key_and_allowed_actions() {
        let cfg = Config::default();
        let mut state = ConversationState::default();
        state.account_id = 1;
        state.contact_id = 88;
        let turn = turn_from(vec![InboundMessage {
            id: 1,
            content: "oi".into(),
            created_at: "2026-07-28T12:00:00Z".into(),
            ..Default::default()
        }]);
        let req = build_agent_request(&cfg, RunId::default(), 523, &state, &turn, None);
        assert_eq!(req.session_key, "cw:1:523");
        assert!(req.allowed_actions.contains(&ActionKind::SendMessage));
        // call_agent proibido na Fase 1
        assert!(!req.allowed_actions.contains(&ActionKind::CallAgent));
        assert_eq!(req.locale, "pt-BR");
    }

    use bridge_core::ActionKind;
}
