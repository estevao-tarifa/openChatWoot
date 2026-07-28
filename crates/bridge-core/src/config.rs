use crate::SecretString;
use figment::{providers::Env, Figment};
    use serde::Deserialize;

    // ponytail: carregamos UMA Fatia plana `RawConfig` lendo TODAS as env vars
    // via `Env::raw()` — figment lowerecase as chaves. O wrapper `AnyString`
    // aceita tanto números como strings do ambiente, já que `Env::raw()` pode
    // interpretar valores puramente numéricos como inteiros.

    /// Tipo que aceita string **ou** número na deserialização (figment quirk).
    #[derive(Debug, Default)]
    struct AnyString(Option<String>);

    impl From<AnyString> for Option<String> {
        fn from(v: AnyString) -> Self { v.0 }
    }

    impl<'de> serde::Deserialize<'de> for AnyString {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = AnyString;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a string or number")
                }
                fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<AnyString, E> {
                    Ok(AnyString(Some(v.to_string())))
                }
                fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<AnyString, E> {
                    Ok(AnyString(Some(v.to_string())))
                }
                fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<AnyString, E> {
                    Ok(AnyString(Some(v.to_string())))
                }
            }
            d.deserialize_any(V)
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    struct RawConfig {
        // Chatwoot
        chatwoot_base_url: Option<String>,
        chatwoot_account_id: AnyString,
        chatwoot_bot_token: Option<String>,
        chatwoot_platform_token: Option<String>,
        webhook_secrets: Option<String>,
        ai_enabled_inboxes: Option<String>,
        fallback_team_id: AnyString,

        // Identidade / provider
        agent_display_name: Option<String>,
        ai_disclosure_text: Option<String>,
        agent_provider: Option<String>,
        agent_provider_fallback: Option<String>,
        openclaw_base_url: Option<String>,
        openclaw_token: Option<String>,
        openclaw_agent_id: Option<String>,
        hermes_shim_url: Option<String>,
        anthropic_api_key: Option<String>,
        agent_timeout_ms: AnyString,
        agent_max_output_chars: AnyString,

        // Buffer
        buffer_debounce_ms: AnyString,
        buffer_max_wait_ms: AnyString,
        buffer_max_messages: AnyString,
        buffer_max_chars: AnyString,
        buffer_media_debounce_ms: AnyString,

        // Limitadores
        rl_conv_runs_per_min: AnyString,
        rl_contact_runs_per_hour: AnyString,
        rl_out_msgs_per_conv_per_hour: AnyString,
        rl_account_runs_per_min: AnyString,
        max_concurrent_agent_runs: AnyString,
        max_consecutive_ai_turns: AnyString,
        daily_budget_usd: AnyString,

        // Controle
        ai_enabled: AnyString,
        ai_block_labels: Option<String>,
        ai_silent_label: Option<String>,
        after_hours_mode: Option<String>,
        ack_threshold_ms: AnyString,
        ack_cooldown_ms: AnyString,
        allowed_link_domains: Option<String>,

        // Notificações
        notify_channels: Option<String>,
        telegram_bot_token: Option<String>,
        whatsapp_cloud_token: Option<String>,
        whatsapp_template_name: Option<String>,
        notify_quiet_hours: Option<String>,
        notify_max_per_agent_per_hour: AnyString,

        // Infra
        database_url: Option<String>,
        redis_url: Option<String>,
        tools_service_url: Option<String>,
        tools_service_token: Option<String>,
        data_retention_days: AnyString,
        log_level: Option<String>,
        log_redact_pii: AnyString,
        otel_exporter_otlp_endpoint: Option<String>,
    }

    // ---- Config tipado ----

    #[derive(Debug, Clone)]
    pub struct Config {
        pub chatwoot: ChatwootConfig,
        pub agent: AgentConfig,
        pub buffer: BufferConfig,
        pub rate_limits: crate::RateLimits,
        pub notification: NotificationConfig,
        pub infra: InfraConfig,
    }

    #[derive(Debug, Clone)]
    pub struct ChatwootConfig {
        pub base_url: String,
        pub account_id: crate::AccountId,
        pub bot_token: SecretString,
        pub platform_token: Option<SecretString>,
        /// Lista de secrets para rotação sem downtime (Seção 4.4 regra 5).
        pub webhook_secrets: Vec<SecretString>,
        /// Inboxes onde a IA atua (G7). Vazio = todas.
        pub ai_enabled_inboxes: Vec<crate::InboxId>,
        pub fallback_team_id: i64,
    }

    #[derive(Debug, Clone)]
    pub struct AgentConfig {
        pub display_name: String,
        pub disclosure_text: String,
        pub provider: String,
        pub provider_fallback: String,
        pub openclaw_base_url: String,
        pub openclaw_token: SecretString,
        pub openclaw_agent_id: String,
        pub hermes_shim_url: String,
        pub anthropic_api_key: SecretString,
        pub timeout_ms: u64,
        pub max_output_chars: usize,

        // Controle (Seção 7.4 / 8.1 / 14)
        pub ai_enabled: bool, // kill switch global (G11)
        pub ai_block_labels: Vec<String>,
        pub ai_silent_label: String,
        pub after_hours_mode: String, // "ai" | "static" | "off"
        pub ack_threshold_ms: u64,
        pub ack_cooldown_ms: u64,
        pub allowed_link_domains: Vec<String>,
    }

    #[derive(Debug, Clone, Copy)]
    pub struct BufferConfig {
        pub debounce_ms: u64,
        pub max_wait_ms: u64,
        pub max_messages: u32,
        pub max_chars: u32,
        pub media_debounce_ms: u64,
    }

    #[derive(Debug, Clone)]
    pub struct NotificationConfig {
        pub channels: Vec<String>,
        pub telegram_bot_token: SecretString,
        pub whatsapp_cloud_token: SecretString,
        pub whatsapp_template_name: String,
        pub quiet_hours: String,
        pub max_per_agent_per_hour: u32,
    }

    #[derive(Debug, Clone)]
    pub struct InfraConfig {
        pub database_url: SecretString,
        pub redis_url: SecretString,
        pub tools_service_url: String,
        pub tools_service_token: SecretString,
        pub data_retention_days: u32,
        pub log_level: String,
        pub log_redact_pii: bool,
        pub otel_endpoint: Option<String>,
    }

    // ---- Defaults de spec (Seção 14) ----

    impl Default for Config {
        fn default() -> Self {
            Self {
                chatwoot: ChatwootConfig::default(),
                agent: AgentConfig::default(),
                buffer: BufferConfig::default(),
                rate_limits: crate::RateLimits::default(),
                notification: NotificationConfig::default(),
                infra: InfraConfig::default(),
            }
        }
    }

    impl Default for ChatwootConfig {
        fn default() -> Self {
            Self {
                base_url: String::new(),
                account_id: 1,
                bot_token: SecretString::default(),
                platform_token: None,
                webhook_secrets: Vec::new(),
                ai_enabled_inboxes: Vec::new(),
                fallback_team_id: 0,
            }
        }
    }

    impl Default for AgentConfig {
        fn default() -> Self {
            Self {
                display_name: "Íris".to_string(),
                disclosure_text:
                    "Oi! Sou a Íris, assistente digital do escritório. Posso te ajudar agora \
                     mesmo — e se precisar, chamo alguém da equipe."
                        .to_string(),
                provider: "openclaw".to_string(),
                provider_fallback: "anthropic".to_string(),
                openclaw_base_url: "http://127.0.0.1:18789".to_string(),
                openclaw_token: SecretString::default(),
                openclaw_agent_id: "iris-triagem".to_string(),
                hermes_shim_url: "http://127.0.0.1:18800".to_string(),
                anthropic_api_key: SecretString::default(),
                timeout_ms: 45_000,
                max_output_chars: 1200,
                ai_enabled: true,
                ai_block_labels: crate::AI_BLOCK_LABELS
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                ai_silent_label: crate::AI_SILENT_LABEL.to_string(),
                after_hours_mode: "ai".to_string(),
                ack_threshold_ms: 3_000,
                ack_cooldown_ms: 600_000,
                allowed_link_domains: vec!["escritorio.com.br".to_string(), "gov.br".to_string()],
            }
        }
    }

    impl Default for BufferConfig {
        fn default() -> Self {
            Self {
                debounce_ms: 6_000,
                max_wait_ms: 25_000,
                max_messages: 12,
                max_chars: 6_000,
                media_debounce_ms: 10_000,
            }
        }
    }

    impl Default for NotificationConfig {
        fn default() -> Self {
            Self {
                channels: vec!["telegram".to_string(), "whatsapp".to_string()],
                telegram_bot_token: SecretString::default(),
                whatsapp_cloud_token: SecretString::default(),
                whatsapp_template_name: "atendimento_pendente".to_string(),
                quiet_hours: "22:00-07:00".to_string(),
                max_per_agent_per_hour: 10,
            }
        }
    }

    impl Default for InfraConfig {
        fn default() -> Self {
            Self {
                database_url: SecretString::default(),
                redis_url: SecretString::from("redis://127.0.0.1:6379/0".to_string()),
                tools_service_url: "http://127.0.0.1:18900".to_string(),
                tools_service_token: SecretString::default(),
                data_retention_days: 180,
                log_level: "info".to_string(),
                log_redact_pii: true,
                otel_endpoint: None,
            }
        }
    }

    // ---- Carregamento ----

    impl Config {
        /// Carrega config do ambiente via figment. Variáveis ausentes usam
        /// os defaults de spec (Seção 14). Nunca pânico.
        pub fn load() -> Result<Self, figment::Error> {
            let raw: RawConfig = Figment::new().merge(Env::raw()).extract()?;
            Ok(Self::from_raw(raw))
        }

        fn from_raw(r: RawConfig) -> Self {
            let mut cfg = Self::default();

            // Chatwoot
            if let Some(v) = r.chatwoot_base_url {
                cfg.chatwoot.base_url = v;
            }
            if let Some(v) = r.chatwoot_account_id.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.chatwoot.account_id = n;
                }
            }
            if let Some(v) = r.chatwoot_bot_token {
                cfg.chatwoot.bot_token = SecretString::from(v);
            }
            if let Some(v) = r.chatwoot_platform_token {
                if !v.trim().is_empty() {
                    cfg.chatwoot.platform_token = Some(SecretString::from(v));
                }
            }
            if let Some(v) = r.webhook_secrets {
                cfg.chatwoot.webhook_secrets = split_csv(&v)
                    .into_iter()
                    .map(SecretString::from)
                    .collect();
            }
            if let Some(v) = r.ai_enabled_inboxes {
                cfg.chatwoot.ai_enabled_inboxes = split_csv(&v)
                    .into_iter()
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
            }
            if let Some(v) = r.fallback_team_id.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.chatwoot.fallback_team_id = n;
                }
            }

            // Agent / controle
            if let Some(v) = r.agent_display_name {
                cfg.agent.display_name = v;
            }
            if let Some(v) = r.ai_disclosure_text {
                cfg.agent.disclosure_text = v;
            }
            if let Some(v) = r.agent_provider {
                cfg.agent.provider = v;
            }
            if let Some(v) = r.agent_provider_fallback {
                cfg.agent.provider_fallback = v;
            }
            if let Some(v) = r.openclaw_base_url {
                cfg.agent.openclaw_base_url = v;
            }
            if let Some(v) = r.openclaw_token {
                cfg.agent.openclaw_token = SecretString::from(v);
            }
            if let Some(v) = r.openclaw_agent_id {
                cfg.agent.openclaw_agent_id = v;
            }
            if let Some(v) = r.hermes_shim_url {
                cfg.agent.hermes_shim_url = v;
            }
            if let Some(v) = r.anthropic_api_key {
                cfg.agent.anthropic_api_key = SecretString::from(v);
            }
            if let Some(v) = r.agent_timeout_ms.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.agent.timeout_ms = n;
                }
            }
            if let Some(v) = r.agent_max_output_chars.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.agent.max_output_chars = n;
                }
            }
            if let Some(v) = r.ai_enabled.0 {
                cfg.agent.ai_enabled = parse_bool(&v).unwrap_or(true);
            }
            if let Some(v) = r.ai_block_labels {
                let parsed = split_csv(&v);
                if !parsed.is_empty() {
                    cfg.agent.ai_block_labels = parsed;
                }
            }
            if let Some(v) = r.ai_silent_label {
                cfg.agent.ai_silent_label = v;
            }
            if let Some(v) = r.after_hours_mode {
                cfg.agent.after_hours_mode = v;
            }
            if let Some(v) = r.ack_threshold_ms.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.agent.ack_threshold_ms = n;
                }
            }
            if let Some(v) = r.ack_cooldown_ms.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.agent.ack_cooldown_ms = n;
                }
            }
            if let Some(v) = r.allowed_link_domains {
                let parsed = split_csv(&v);
                if !parsed.is_empty() {
                    cfg.agent.allowed_link_domains = parsed;
                }
            }

            // Buffer
            if let Some(v) = r.buffer_debounce_ms.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.buffer.debounce_ms = n;
                }
            }
            if let Some(v) = r.buffer_max_wait_ms.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.buffer.max_wait_ms = n;
                }
            }
            if let Some(v) = r.buffer_max_messages.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.buffer.max_messages = n;
                }
            }
            if let Some(v) = r.buffer_max_chars.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.buffer.max_chars = n;
                }
            }
            if let Some(v) = r.buffer_media_debounce_ms.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.buffer.media_debounce_ms = n;
                }
            }

            // Rate limits
            if let Some(v) = r.rl_conv_runs_per_min.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.rate_limits.conv_runs_per_min = n;
                }
            }
            if let Some(v) = r.rl_contact_runs_per_hour.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.rate_limits.contact_runs_per_hour = n;
                }
            }
            if let Some(v) = r.rl_out_msgs_per_conv_per_hour.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.rate_limits.out_msgs_per_conv_per_hour = n;
                }
            }
            if let Some(v) = r.rl_account_runs_per_min.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.rate_limits.account_runs_per_min = n;
                }
            }
            if let Some(v) = r.max_concurrent_agent_runs.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.rate_limits.max_concurrent_runs = n;
                }
            }
            if let Some(v) = r.max_consecutive_ai_turns.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.rate_limits.max_consecutive_ai_turns = n;
                }
            }
            if let Some(v) = r.daily_budget_usd.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.rate_limits.daily_budget_usd = n;
                }
            }

            // Notificações
            if let Some(v) = r.notify_channels {
                let parsed = split_csv(&v);
                if !parsed.is_empty() {
                    cfg.notification.channels = parsed;
                }
            }
            if let Some(v) = r.telegram_bot_token {
                cfg.notification.telegram_bot_token = SecretString::from(v);
            }
            if let Some(v) = r.whatsapp_cloud_token {
                cfg.notification.whatsapp_cloud_token = SecretString::from(v);
            }
            if let Some(v) = r.whatsapp_template_name {
                cfg.notification.whatsapp_template_name = v;
            }
            if let Some(v) = r.notify_quiet_hours {
                cfg.notification.quiet_hours = v;
            }
            if let Some(v) = r.notify_max_per_agent_per_hour.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.notification.max_per_agent_per_hour = n;
                }
            }

            // Infra
            if let Some(v) = r.database_url {
                cfg.infra.database_url = SecretString::from(v);
            }
            if let Some(v) = r.redis_url {
                cfg.infra.redis_url = SecretString::from(v);
            }
            if let Some(v) = r.tools_service_url {
                cfg.infra.tools_service_url = v;
            }
            if let Some(v) = r.tools_service_token {
                cfg.infra.tools_service_token = SecretString::from(v);
            }
            if let Some(v) = r.data_retention_days.0 {
                if let Ok(n) = v.trim().parse() {
                    cfg.infra.data_retention_days = n;
                }
            }
            if let Some(v) = r.log_level {
                cfg.infra.log_level = v;
            }
            if let Some(v) = r.log_redact_pii.0 {
                cfg.infra.log_redact_pii = parse_bool(&v).unwrap_or(true);
            }
            if let Some(v) = r.otel_exporter_otlp_endpoint {
                let t = v.trim();
                if !t.is_empty() {
                    cfg.infra.otel_endpoint = Some(t.to_string());
                }
            }

            cfg
        }
    }

    fn split_csv(s: &str) -> Vec<String> {
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    }

    fn parse_bool(s: &str) -> Option<bool> {
        match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        }
    }

