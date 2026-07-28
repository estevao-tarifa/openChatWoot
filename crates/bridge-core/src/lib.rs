//! bridge-core — domínio puro da ponte Chatwoot-IA.
//! Nenhuma dependência de I/O. Apenas tipos, regras e contratos.
//!
//! Spec normativa: `ESPECchatwootaibridge.md`.

pub mod config;
pub mod error;
pub mod ids;
pub mod ratelimit;
pub mod secrets;
pub mod state_machine;

pub mod model;

// Re-exports públicos — superfície única do crate.
pub use config::{
    AgentConfig, BufferConfig, ChatwootConfig, Config, InfraConfig, NotificationConfig,
};
pub use error::{AgentError, ChatwootError, CoreError, StateError};
pub use ids::{AccountId, ContactId, ConversationId, InboxId, RunId};
pub use ratelimit::RateLimits;
pub use secrets::SecretString;
pub use state_machine::{AiState, StateEvent};

pub use model::agent::{
    Action, ActionKind, AgentRequest, AgentResponse, Attachment, HandoffInfo, InboundMessage, Reply,
    Usage,
};
pub use model::context::{
    AgentSummary, BusinessHoursState, ClientSummary, ContactSummary, ConversationContext, HistoryItem,
};

// ---- Constantes de controle (Seção 7.4 / 14) ----
pub const AI_BLOCK_LABELS: &[&str] = &["ia:off", "humano", "juridico"];
pub const AI_SILENT_LABEL: &str = "ia:silencio";
pub const AI_LIMITED_LABEL: &str = "ia:limitado";
pub const AI_FAILURE_LABEL: &str = "ia:falha";
pub const SLA_VIOLATED_LABEL: &str = "sla:violado";

/// `true` se a etiqueta bloqueia a IA (comparação insensível a caixa).
pub fn is_block_label(label: &str) -> bool {
    AI_BLOCK_LABELS.iter().any(|b| b.eq_ignore_ascii_case(label))
}

// ---- Helpers de chaves Redis (Seções 5.2, 6.2, 6.3, 6.4) ----
pub fn session_key(account: AccountId, conversation: ConversationId) -> String {
    format!("cw:{account}:{conversation}")
}
pub fn lock_key(conversation: ConversationId) -> String {
    format!("lock:conv:{conversation}")
}
pub fn dedup_key(account: AccountId, msg_id: i64) -> String {
    format!("seen:msg:{account}:{msg_id}")
}
pub fn buffer_key(conversation: ConversationId) -> String {
    format!("buf:{conversation}")
}
pub const DEBOUNCE_ZSET: &str = "debounce:zset";
pub const QUEUE_AGENT_RUNS: &str = "queue:agent_runs";

// ---- Métricas (Seção 15.1) ----
pub mod metrics {
    #[cfg_attr(feature = "no-auto-describe", allow(dead_code))]
    pub const WEBHOOK_RECEIVED: &str = "bridge_webhook_received_total";
    pub const WEBHOOK_SIGNATURE_FAILURES: &str = "bridge_webhook_signature_failures_total";
    pub const BUFFER_FLUSH: &str = "bridge_buffer_flush_total";
    pub const BUFFER_MSGS_PER_TURN: &str = "bridge_buffer_messages_per_turn";
    pub const GATE_BLOCK: &str = "bridge_gate_block_total";
    pub const AGENT_RUN: &str = "bridge_agent_run_total";
    pub const AGENT_RUN_DURATION: &str = "bridge_agent_run_duration_seconds";
    pub const AGENT_COST: &str = "bridge_agent_cost_usd_total";
    pub const CHATWOOT_API: &str = "bridge_chatwoot_api_total";
    pub const OUTBOUND_MSGS: &str = "bridge_outbound_messages_total";
    pub const SLA_ESCALATION: &str = "bridge_sla_escalation_total";
    pub const LOCK_CONTENTION: &str = "bridge_lock_contention_total";
    pub const AI_RESOLUTION_RATE: &str = "bridge_ai_resolution_rate";
}

#[cfg(test)]
mod tests;