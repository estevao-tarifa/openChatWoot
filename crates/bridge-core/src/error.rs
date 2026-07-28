use crate::state_machine::{AiState, StateEvent};
use thiserror::Error;

    /// Erro genérico do bridge-core.
    #[derive(Debug, Error)]
    pub enum CoreError {
        #[error("invalid state transition: {from} + {event}")]
        InvalidTransition {
            from: AiState,
            event: StateEvent,
        },
        #[error("config error: {0}")]
        Config(String),
        #[error("invalid action kind: {0}")]
        InvalidActionKind(String),
        #[error("serde error: {0}")]
        Serde(String),
    }

    impl From<figment::Error> for CoreError {
        fn from(e: figment::Error) -> Self {
            Self::Config(e.to_string())
        }
    }

    impl From<serde_json::Error> for CoreError {
        fn from(e: serde_json::Error) -> Self {
            Self::Serde(e.to_string())
        }
    }

    /// Erro de transição de estado (Seção 7).
    #[derive(Debug, Clone, Error)]
    #[error("invalid state transition from {from} on event {event}")]
    pub struct StateError {
        pub from: AiState,
        pub event: StateEvent,
    }

    // ponytail: `StateError::InvalidTransition { .. }` construtor no estilo
    // do enum do ticket é emulado por um struct — só o par (from, event) é
    // necessário. Trocar para enum quando surgir um segundo kind de erro.
    impl StateError {
        pub fn invalid_transition(from: AiState, event: StateEvent) -> Self {
            Self { from, event }
        }
    }

    // Reconstructor esperado pelo módulo machine (forma struct).
    impl From<(AiState, StateEvent)> for StateError {
        fn from((from, event): (AiState, StateEvent)) -> Self {
            Self { from, event }
        }
    }

    /// Erros do `AgentProvider` (Seção 5.1).
    #[derive(Debug, Error)]
    pub enum AgentError {
        #[error("agent timeout")]
        Timeout,
        #[error("rate limited")]
        RateLimited,
        #[error("provider error: {0}")]
        ProviderError(String),
        #[error("invalid response: {0}")]
        InvalidResponse(String),
        #[error("auth error")]
        AuthError,
        #[error("budget exceeded")]
        BudgetExceeded,
    }

    /// Erros do cliente Chatwoot (Seção 4).
    #[derive(Debug, Error)]
    pub enum ChatwootError {
        #[error("api error: {0}")]
        ApiError(String),
        #[error("not found")]
        NotFound,
        #[error("auth error")]
        AuthError,
        #[error("rate limited")]
        RateLimited,
        #[error("timeout")]
        Timeout,
}
