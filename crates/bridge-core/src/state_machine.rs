use crate::is_block_label;
use serde::{Deserialize, Serialize};
use std::fmt;

    /// Estados internos da conversa (coluna `conversation_state.ai_state`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
    #[serde(rename_all = "snake_case")]
    pub enum AiState {
        /// IA no comando.
        #[default]
        AiActive,
        /// Run em andamento (buffer acumula).
        AiThinking,
        /// Handoff pedido, ninguém assumiu.
        AwaitingHuman,
        /// Humano atribuído e ativo.
        HumanHandling,
        /// Pausada por etiqueta (`ia:off`/`humano`/`juridico`) ou comando.
        AiPausedManual,
        /// Pausada por limitador (L1–L6).
        AiPausedLimit,
        /// Conversa resolvida.
        Closed,
    }

    impl AiState {
        /// `true` somente em `ai_active` — único estado onde a IA responde.
        pub fn can_ai_respond(&self) -> bool {
            matches!(self, Self::AiActive)
        }

        /// `true` se a IA está pausada por qualquer motivo (manual ou limitador).
        pub fn is_paused(&self) -> bool {
            matches!(self, Self::AiPausedManual | Self::AiPausedLimit)
        }

        /// Nome canônico em snake_case (espelha o valor persistido em BD).
        pub fn as_str(&self) -> &'static str {
            match self {
                Self::AiActive => "ai_active",
                Self::AiThinking => "ai_thinking",
                Self::AwaitingHuman => "awaiting_human",
                Self::HumanHandling => "human_handling",
                Self::AiPausedManual => "ai_paused_manual",
                Self::AiPausedLimit => "ai_paused_limit",
                Self::Closed => "closed",
            }
        }

        /// Aplica `event` partindo de `self`. Transições conforme Seção 7.2.
        /// Erro quando o evento não é válido no estado atual.
        pub fn transition(self, event: &StateEvent) -> Result<Self, crate::error::StateError> {
            use AiState::*;
            use StateEvent::*;

            // ponytail: rejeita eventos com label de bloqueio que não são block
            // labels —LabelAdded de label neutro não muda estado; LabelRemoved
            // de block label reativa (assume que era a única block label; o worker
            // real deve checar o conjunto atual de labels antes de reativar).
            let res: Result<AiState, _> = match (self, event) {
                // ---- AiActive ----
                (AiActive, ContactMessage) => Ok(AiThinking), // 7.2: contato fala -> thinking
                (AiActive, AiStarted) => Ok(AiThinking),
                (AiActive, AiResponded) => Ok(AiActive),
                (AiActive, AiFailed) => Ok(AwaitingHuman),
                (AiActive, AiRequestedHandoff) => Ok(AwaitingHuman),
                (AiActive, LimitExceeded) => Ok(AiPausedLimit),
                (AiActive, HumanMessage) => Ok(HumanHandling),
                (AiActive, HumanAssigned) => Ok(HumanHandling),
                (AiActive, HumanResolved) => Ok(Closed),
                (AiActive, LabelAdded(l)) if is_block_label(l) => Ok(AiPausedManual),
                (AiActive, LabelAdded(_)) => Ok(AiActive),   // etiqueta neutra: no-op
                (AiActive, LabelRemoved(_)) => Ok(AiActive), // remover etiqueta em ativo: no-op
                (AiActive, _) => Err(()),

                // ---- AiThinking ----
                (AiThinking, AiResponded) => Ok(AiActive),
                (AiThinking, AiFailed) => Ok(AwaitingHuman),
                (AiThinking, AiRequestedHandoff) => Ok(AwaitingHuman),
                (AiThinking, LimitExceeded) => Ok(AiPausedLimit),
                (AiThinking, HumanMessage) => Ok(HumanHandling),
                (AiThinking, HumanAssigned) => Ok(HumanHandling),
                (AiThinking, HumanResolved) => Ok(Closed),
                (AiThinking, LabelAdded(l)) if is_block_label(l) => Ok(AiPausedManual),
                (AiThinking, LabelAdded(_)) => Ok(AiThinking),   // neutra: no-op
                (AiThinking, LabelRemoved(_)) => Ok(AiThinking), // neutra: no-op
                (AiThinking, ContactMessage) => Ok(AiThinking), // buffer acumula
                (AiThinking, AiStarted) => Ok(AiThinking),
                (AiThinking, _) => Err(()),

                // ---- AwaitingHuman ----
                (AwaitingHuman, HumanAssigned) => Ok(HumanHandling),
                (AwaitingHuman, HumanMessage) => Ok(HumanHandling),
                (AwaitingHuman, HumanResolved) => Ok(Closed),
                (AwaitingHuman, HumanSetPending) => Ok(AiActive), // atendente devolve p/ IA
                // SLA estourado escala mas permanece em awaiting_human (7.2).
                (AwaitingHuman, LimitWindowExpired) => Ok(AwaitingHuman),
                (AwaitingHuman, LabelAdded(_)) => Ok(AwaitingHuman),   // no-op
                (AwaitingHuman, LabelRemoved(_)) => Ok(AwaitingHuman), // no-op
                (AwaitingHuman, _) => Err(()),

                // ---- HumanHandling ----
                (HumanHandling, HumanResolved) => Ok(Closed),
                (HumanHandling, HumanSetPending) => Ok(AiActive), // status=pending -> bot
                (HumanHandling, HumanMessage) => Ok(HumanHandling),
                (HumanHandling, ContactReplied) => Ok(HumanHandling),
                (HumanHandling, ContactMessage) => Ok(HumanHandling),
                (HumanHandling, LabelAdded(_)) => Ok(HumanHandling),   // no-op
                (HumanHandling, LabelRemoved(_)) => Ok(HumanHandling), // no-op
                (HumanHandling, _) => Err(()),

                // ---- AiPausedLimit ----
                (AiPausedLimit, LimitWindowExpired) => Ok(AiActive),
                (AiPausedLimit, HumanAssigned) => Ok(HumanHandling),
                (AiPausedLimit, HumanMessage) => Ok(HumanHandling),
                (AiPausedLimit, HumanResolved) => Ok(Closed),
                (AiPausedLimit, LabelAdded(_)) => Ok(AiPausedLimit),   // no-op
                (AiPausedLimit, LabelRemoved(_)) => Ok(AiPausedLimit), // no-op
                (AiPausedLimit, _) => Err(()),

                // ---- AiPausedManual ----
                (AiPausedManual, LabelRemoved(l)) if is_block_label(l) => Ok(AiActive),
                (AiPausedManual, LabelRemoved(_)) => Ok(AiPausedManual), // neutra: permanece pausado
                (AiPausedManual, LabelAdded(_)) => Ok(AiPausedManual),    // no-op
                (AiPausedManual, HumanAssigned) => Ok(HumanHandling),
                (AiPausedManual, HumanMessage) => Ok(HumanHandling),
                (AiPausedManual, HumanResolved) => Ok(Closed),
                (AiPausedManual, _) => Err(()),

                // ---- Closed ----
                (Closed, ContactReplied) => Ok(AiActive),
                (Closed, ContactMessage) => Ok(AiActive),
                (Closed, HumanResolved) => Ok(Closed),
                (Closed, LabelAdded(_)) => Ok(Closed),   // no-op
                (Closed, LabelRemoved(_)) => Ok(Closed), // no-op
                (Closed, _) => Err(()),
            };

            res.map_err(|_| crate::error::StateError {
                from: self,
                event: event.clone(),
            })
        }
    }

    impl fmt::Display for AiState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }

    /// Eventos que movem a máquina de estados. Spec Seção 7 + lista do ticket.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum StateEvent {
        /// Contato enviou mensagem (alimenta o buffer).
        ContactMessage,
        /// Buffer descarregou e o run do agente começou.
        AiStarted,
        /// Run do agente finalizou com sucesso.
        AiResponded,
        /// Run do agente falhou ou estourou timeout.
        AiFailed,
        /// IA pediu handoff (ação `request_handoff`).
        AiRequestedHandoff,
        /// Etiqueta adicionada à conversa.
        LabelAdded(String),
        /// Limitador em cascata estourou (L1–L6).
        LimitExceeded,
        /// Humano enviou mensagem outgoing.
        HumanMessage,
        /// Humano assumiu a conversa (`assignee_id` definido).
        HumanAssigned,
        /// Humano resolveu a conversa.
        HumanResolved,
        /// Humano colocou status `pending` (devolve para a IA).
        HumanSetPending,
        /// Janela do limitador expirou.
        LimitWindowExpired,
        /// Etiqueta removida da conversa.
        LabelRemoved(String),
        /// Contato respondeu (reabre conversa fechada).
        ContactReplied,
    }

    impl fmt::Display for StateEvent {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::ContactMessage => f.write_str("contact_message"),
                Self::AiStarted => f.write_str("ai_started"),
                Self::AiResponded => f.write_str("ai_responded"),
                Self::AiFailed => f.write_str("ai_failed"),
                Self::AiRequestedHandoff => f.write_str("ai_requested_handoff"),
                Self::LabelAdded(l) => write!(f, "label_added:{l}"),
                Self::LimitExceeded => f.write_str("limit_exceeded"),
                Self::HumanMessage => f.write_str("human_message"),
                Self::HumanAssigned => f.write_str("human_assigned"),
                Self::HumanResolved => f.write_str("human_resolved"),
                Self::HumanSetPending => f.write_str("human_set_pending"),
                Self::LimitWindowExpired => f.write_str("limit_window_expired"),
                Self::LabelRemoved(l) => write!(f, "label_removed:{l}"),
                Self::ContactReplied => f.write_str("contact_replied"),
            }
        }
    }
