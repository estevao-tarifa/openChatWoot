// Tests de auto-verificação — extraídos do módulo `tests` original.
// ponytail: testes inline no mesmo arquivo; frameworks chegam só se o projeto crescer.
// Para rodar: cargo test -p bridge-core

#[cfg(test)]
mod unit {
    use crate::model::agent::*;
    use crate::model::context::*;
    use crate::state_machine::*;
    use crate::ids::*;
    use crate::ratelimit::RateLimits;
    use crate::{is_block_label, session_key, lock_key, dedup_key};
    use crate::error::StateError;
    use std::str::FromStr;

    #[test]
    fn test_run_id() {
        let r1 = RunId::new();
        let r2 = RunId::new();
        assert_ne!(r1, r2);
        assert!(!r1.to_string().is_empty());
    }

    #[test]
    fn test_action_kind_roundtrip() {
        for kind in [
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
            ActionKind::CallTool,
            ActionKind::CallAgent,
            ActionKind::RequestHandoff,
        ] {
            let s = kind.as_str();
            let parsed = ActionKind::from_str(s).unwrap();
            assert_eq!(kind, parsed);
        }
        assert!(ActionKind::from_str("invalid").is_err());
    }

    #[test]
    fn test_state_machine_happy_path() {
        let s = AiState::AiActive;
        let s = s.transition(&StateEvent::ContactMessage).unwrap();
        assert_eq!(s, AiState::AiThinking);
        let s = s.transition(&StateEvent::AiResponded).unwrap();
        assert_eq!(s, AiState::AiActive);
    }

    #[test]
    fn test_state_machine_handoff_path() {
        let s = AiState::AiActive;
        let s = s.transition(&StateEvent::AiRequestedHandoff).unwrap();
        assert_eq!(s, AiState::AwaitingHuman);
        let s = s.transition(&StateEvent::HumanAssigned).unwrap();
        assert_eq!(s, AiState::HumanHandling);
        let s = s.transition(&StateEvent::HumanResolved).unwrap();
        assert_eq!(s, AiState::Closed);
    }

    #[test]
    fn test_state_machine_label_pause() {
        let s = AiState::AiActive;
        let s = s.transition(&StateEvent::LabelAdded("ia:off".into())).unwrap();
        assert_eq!(s, AiState::AiPausedManual);
        let s = s.transition(&StateEvent::LabelRemoved("ia:off".into())).unwrap();
        assert_eq!(s, AiState::AiActive);
    }

    #[test]
    fn test_state_machine_closed_reopens() {
        let s = AiState::Closed;
        let s = s.transition(&StateEvent::ContactReplied).unwrap();
        assert_eq!(s, AiState::AiActive);
    }

    #[test]
    fn test_state_machine_limit_exceeded() {
        let s = AiState::AiActive;
        let s = s.transition(&StateEvent::LimitExceeded).unwrap();
        assert_eq!(s, AiState::AiPausedLimit);
        let s = s.transition(&StateEvent::LimitWindowExpired).unwrap();
        assert_eq!(s, AiState::AiActive);
    }

    #[test]
    fn test_state_machine_invalid_transition() {
        let s = AiState::Closed;
        assert!(s.transition(&StateEvent::AiStarted).is_err());
    }

    #[test]
    fn test_state_machine_ai_failed() {
        let s = AiState::AiActive;
        let s = s.transition(&StateEvent::AiFailed).unwrap();
        assert_eq!(s, AiState::AwaitingHuman);
    }

    #[test]
    fn test_block_labels() {
        assert!(is_block_label("ia:off"));
        assert!(is_block_label("humano"));
        assert!(is_block_label("juridico"));
        assert!(!is_block_label("fiscal"));
    }

    #[test]
    fn test_key_formats() {
        assert_eq!(session_key(1, 523), "cw:1:523");
        assert_eq!(lock_key(523), "lock:conv:523");
        assert_eq!(dedup_key(1, 9021), "seen:msg:1:9021");
    }

    #[test]
    fn test_rate_limits_defaults() {
        let rl = RateLimits::default();
        assert_eq!(rl.conv_runs_per_min, 6);
        assert_eq!(rl.daily_budget_usd, 25.0);
    }

    #[test]
    fn test_business_hours_roundtrip() {
        let b = BusinessHoursState::Within;
        assert_eq!(b.to_string(), "within");
        let p = BusinessHoursState::from_str("within").unwrap();
        assert_eq!(p, BusinessHoursState::Within);
        let p = BusinessHoursState::from_str("fora").unwrap();
        assert_eq!(p, BusinessHoursState::Outside);
    }

    #[test]
    fn test_action_kind_as_str() {
        assert_eq!(ActionKind::SendMessage.as_str(), "send_message");
        assert_eq!(ActionKind::RequestHandoff.as_str(), "request_handoff");
    }

    #[test]
    fn test_conversation_context_default() {
        let ctx = ConversationContext::default();
        assert_eq!(ctx.conversation_id, 0);
        assert_eq!(ctx.business_hours, BusinessHoursState::Within);
    }
}
