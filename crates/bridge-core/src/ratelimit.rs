use crate::ids::{AccountId, ContactId, ConversationId};
use serde::{Deserialize, Serialize};

    /// Limitadores em cascata (L1–L8). Valores padrão da Spec Seção 6.5 / 14.
    /// Implementação real é script Lua atômico no Redis — aqui só os parâmetros.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    #[serde(default)]
    pub struct RateLimits {
        /// L1 — runs por conversa por minuto.
        pub conv_runs_per_min: u32,
        /// L2 — runs por contato por hora.
        pub contact_runs_per_hour: u32,
        /// L3 — mensagens de saída por conversa por hora.
        pub out_msgs_per_conv_per_hour: u32,
        /// L4 — runs por conta (global) por minuto.
        pub account_runs_per_min: u32,
        /// L5 — concorrência global de IA (semáforo).
        pub max_concurrent_runs: u32,
        /// G8 — turnos consecutivos da IA sem fala de contato/humano.
        pub max_consecutive_ai_turns: u8,
        /// L6 — orçamento diário em USD (freio de emergência financeiro).
        pub daily_budget_usd: f64,
    }

    impl Default for RateLimits {
        fn default() -> Self {
            Self {
                conv_runs_per_min: 6,
                contact_runs_per_hour: 30,
                out_msgs_per_conv_per_hour: 15,
                account_runs_per_min: 300,
                max_concurrent_runs: 20,
                max_consecutive_ai_turns: 4,
                daily_budget_usd: 25.0,
            }
        }
    }

    impl RateLimits {
        /// L7 — tamanho máximo de mensagem de entrada (chars). Spec 6.5.
        pub const MAX_INPUT_CHARS: usize = 4000;
        /// L8 — anexos por turno. Spec 6.5.
        pub const MAX_ATTACHMENTS_PER_TURN: u32 = 5;
        /// L8 — tamanho total de anexos por turno.
        pub const MAX_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;

        /// Chaves Redis dos limitadores.
        pub fn conv_key(conv: ConversationId) -> String {
            format!("rl:conv:{conv}")
        }
        pub fn contact_key(contact: ContactId) -> String {
            format!("rl:contact:{contact}")
        }
        pub fn out_key(conv: ConversationId) -> String {
            format!("rl:out:{conv}")
        }
        pub fn account_key(account: AccountId) -> String {
            format!("rl:account:{account}")
        }
        pub const SEMAPHORE_KEY: &'static str = "sem:agent";
        pub fn budget_key(day_ymd: &str) -> String {
            format!("budget:{day_ymd}")
        }
    }

