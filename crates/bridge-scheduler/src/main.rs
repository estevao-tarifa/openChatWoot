//! bridge-scheduler — binário 3: timers de SLA, escalonamento, reconciliação e
//! purga de dados.
//!
//! Spec normativa: `ESPECchatwootaibridge.md`, Seções 11 (SLA/notificações),
//! 10.4 (retenção), 14 (config/horário comercial).
//!
//! Roda três jobs em loops `tokio::time::interval`:
//! - `check_sla_timers` a cada 30s (Seção 11.2) — dispara a escada de escalonamento.
//! - `reconcile` a cada 5min (Seção 11.4) — corrige divergências vs Chatwoot.
//! - `purge_old_data` a cada 24h (Seção 10.4) — retenção LGPD.
//!
//! ponytail: `tokio::time::interval` em vez de `tokio-cron-scheduler` — um
//! scheduler de 3 jobs fixos não justifica o crate. Trocar quando houver
//! horários customizados por job.

// Este binário define a superfície de spec (SlaKind/SlaStatus/EscalationStep/
// ensure_sla_timers) que outros crates consumiriam; o scheduler em si não
// exercita tudo. Allow global evita ruído sem esconder lógica viva.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use thiserror::Error;
use time::Duration;
use time::OffsetDateTime;
use time::Weekday;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use bridge_core::{Config, SLA_VIOLATED_LABEL};

// ====================================================================
// Cliente Chatwoot (mínimo para o scheduler)
// ====================================================================
// ponytail: o scheduler carrega um `ChatwootClient` mínimo em vez de
// depender de `bridge-chatwoot` — o scheduler usa uma fração da API (labels,
// status, listagem). Se `bridge-chatwoot` crescer features que o scheduler
// precise, refatorar para usá-lo. Até lá, este módulo inline evita carregar
// código não usado.

mod chatwoot {
    #![allow(dead_code)] // superfície mínima da spec (C1–C13); alguns métodos
    // ainda não têm caller no scheduler v1, mas compõem o cliente completo.
    use bridge_core::{ChatwootError, ChatwootConfig};
    use reqwest::Client;
    use serde::Deserialize;

    /// Cliente mínimo da Application API do Chatwoot (Seção 4.6).
    /// Header obrigatório: `api_access_token: {bot_token}`.
    pub struct ChatwootClient {
        base: String,
        account_id: i64,
        token: String,
        http: Client,
    }

    impl ChatwootClient {
        pub fn new(cfg: &ChatwootConfig, http: Client) -> Self {
            Self {
                base: cfg.base_url.trim_end_matches('/').to_string(),
                account_id: cfg.account_id,
                token: cfg.bot_token.expose().to_string(),
                http,
            }
        }

        fn api(&self, path: &str) -> String {
            format!("{}/api/v1/accounts/{}{path}", self.base, self.account_id)
        }

        /// Acessores expostos para a reconciliação montar URLs de reenvio.
        pub fn base(&self) -> &str {
            &self.base
        }
        pub fn account_id(&self) -> i64 {
            self.account_id
        }

        async fn get_json<T: for<'de> Deserialize<'de>>(
            &self,
            url: &str,
        ) -> Result<T, ChatwootError> {
            let resp = self
                .http
                .get(url)
                .header("api_access_token", &self.token)
                .send()
                .await
                .map_err(|e| ChatwootError::ApiError(e.to_string()))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(map_status(status));
            }
            resp.json::<T>()
                .await
                .map_err(|e| ChatwootError::ApiError(e.to_string()))
        }

        async fn post_json<B: serde::Serialize, T: for<'de> Deserialize<'de>>(
            &self,
            url: &str,
            body: &B,
        ) -> Result<T, ChatwootError> {
            let resp = self
                .http
                .post(url)
                .header("api_access_token", &self.token)
                .json(body)
                .send()
                .await
                .map_err(|e| ChatwootError::ApiError(e.to_string()))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(map_status(status));
            }
            resp.json::<T>()
                .await
                .map_err(|e| ChatwootError::ApiError(e.to_string()))
        }

        /// C11 — `GET /conversations/{id}`. Devolve só os campos que o scheduler lê.
        pub async fn get_conversation(&self, conv_id: i64) -> Result<CwConversation, ChatwootError> {
            self.get_json(&self.api(&format!("/conversations/{conv_id}"))).await
        }

        /// `GET /conversations?status=open&assignee_type=all` (Seção 11.4).
        pub async fn list_open_conversations(
            &self,
        ) -> Result<Vec<i64>, ChatwootError> {
            let url = self.api("/conversations?status=open&assignee_type=all");
            let page: CwConversationPage = self.get_json(&url).await?;
            Ok(page
                .data
                .into_iter()
                .filter_map(|c| c.id)
                .collect())
        }

        /// C9 — `GET /conversations/{id}/labels`.
        pub async fn get_labels(&self, conv_id: i64) -> Result<Vec<String>, ChatwootError> {
            let url = self.api(&format!("/conversations/{conv_id}/labels"));
            let labels: CwLabels = self.get_json(&url).await?;
            Ok(labels.payload.unwrap_or_default())
        }

        /// C8 — `POST /conversations/{id}/labels` (substitui o conjunto inteiro).
        /// O caller DEVE fazer GET+união antes — esta função não une.
        pub async fn set_labels(
            &self,
            conv_id: i64,
            labels: &[String],
        ) -> Result<(), ChatwootError> {
            let url = self.api(&format!("/conversations/{conv_id}/labels"));
            let body = serde_json::json!({ "labels": labels });
            let _: serde_json::Value = self.post_json(&url, &body).await?;
            Ok(())
        }

        /// Adiciona uma etiqueta preservando as existentes (C9 + união + C8).
        /// Idempotente: não reenvia se já presente.
        pub async fn add_label(&self, conv_id: i64, label: &str) -> Result<(), ChatwootError> {
            let mut current = self.get_labels(conv_id).await?;
            if current.iter().any(|l| l.eq_ignore_ascii_case(label)) {
                return Ok(());
            }
            current.push(label.to_string());
            self.set_labels(conv_id, &current).await
        }

        /// C5 — `POST /conversations/{id}/toggle_status`.
        pub async fn toggle_status(
            &self,
            conv_id: i64,
            status: &str,
        ) -> Result<(), ChatwootError> {
            let url = self.api(&format!("/conversations/{conv_id}/toggle_status"));
            let body = serde_json::json!({ "status": status });
            let _: serde_json::Value = self.post_json(&url, &body).await?;
            Ok(())
        }

        /// C7 — `POST /conversations/{id}/assignments` (team).
        pub async fn assign_team(&self, conv_id: i64, team_id: i64) -> Result<(), ChatwootError> {
            let url = self.api(&format!("/conversations/{conv_id}/assignments"));
            let body = serde_json::json!({ "team_id": team_id });
            let _: serde_json::Value = self.post_json(&url, &body).await?;
            Ok(())
        }

        /// C2 — nota interna (`message_type=outgoing, private=true`).
        pub async fn send_private_note(
            &self,
            conv_id: i64,
            content: &str,
        ) -> Result<(), ChatwootError> {
            let url = self.api(&format!("/conversations/{conv_id}/messages"));
            let body = serde_json::json!({
                "content": content,
                "message_type": "outgoing",
                "private": true,
            });
            let _: serde_json::Value = self.post_json(&url, &body).await?;
            Ok(())
        }

        /// C12 — `GET /contacts/{id}` (para o nome na notificação).
        pub async fn get_contact_name(&self, contact_id: i64) -> Result<String, ChatwootError> {
            let url = self.api(&format!("/contacts/{contact_id}"));
            let c: CwContact = self.get_json(&url).await?;
            Ok(c.payload.and_then(|p| p.name).unwrap_or_default())
        }

        /// Reconciliação de outbound (Seção 11.4 passo 3): verifica se a
        /// mensagem existe na conversa buscando por `id` nas últimas mensagens.
        pub async fn conversation_has_message(
            &self,
            conv_id: i64,
            chatwoot_msg_id: i64,
        ) -> Result<bool, ChatwootError> {
            let url = self.api(&format!("/conversations/{conv_id}/messages"));
            let page: CwMessagesPage = self.get_json(&url).await?;
            Ok(page
                .payload
                .unwrap_or_default()
                .into_iter()
                .any(|m| m.id == Some(chatwoot_msg_id)))
        }

        /// Reenvio de outbound (Seção 11.4 passo 3 / C1): POSTa o payload
        /// original em `{url}` e devolve o `id` da mensagem criada no Chatwoot.
        pub async fn repost_message(
            &self,
            url: &str,
            payload: &serde_json::Value,
        ) -> Result<i64, ChatwootError> {
            let resp = self
                .http
                .post(url)
                .header("api_access_token", &self.token)
                .json(payload)
                .send()
                .await
                .map_err(|e| ChatwootError::ApiError(e.to_string()))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(map_status(status));
            }
            let v: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| ChatwootError::ApiError(e.to_string()))?;
            Ok(v.get("id")
                .and_then(|i| i.as_i64())
                .unwrap_or(0))
        }
    }

    fn map_status(s: reqwest::StatusCode) -> ChatwootError {
        match s.as_u16() {
            401 | 403 => ChatwootError::AuthError,
            404 => ChatwootError::NotFound,
            429 => ChatwootError::RateLimited,
            408 => ChatwootError::Timeout,
            _ => ChatwootError::ApiError(format!("http {}", s.as_u16())),
        }
    }

    // ---- Shapes mínimos (tolerantes a campos extras, Seção 4.5) ----

    #[derive(Debug, Deserialize)]
    pub struct CwConversationPage {
        #[serde(default)]
        pub data: Vec<CwConversation>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub struct CwConversation {
        pub id: Option<i64>,
        pub status: Option<String>,
        // `meta` traz assignee/team/sender; só precisamos dos ids aqui.
        pub meta: Option<CwMeta>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub struct CwMeta {
        pub assignee: Option<CwAssignee>,
        pub team: Option<CwTeam>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub struct CwAssignee {
        pub id: Option<i64>,
        pub name: Option<String>,
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default)]
    pub struct CwTeam {
        pub id: Option<i64>,
        pub name: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct CwLabels {
        payload: Option<Vec<String>>,
    }

    #[derive(Debug, Deserialize)]
    struct CwContact {
        payload: Option<CwContactPayload>,
    }

    #[derive(Debug, Deserialize)]
    struct CwContactPayload {
        name: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct CwMessagesPage {
        payload: Option<Vec<CwMessage>>,
    }

    #[derive(Debug, Deserialize)]
    struct CwMessage {
        id: Option<i64>,
    }
}

// ====================================================================
// SLA — tipos e escada de escalonamento (Seção 11)
// ====================================================================

/// Tipo de timer de SLA (Seção 11.1). Serializado como texto em `sla_timer.kind`.
#[allow(dead_code)] // superfície de spec; o scheduler opera sobre `kind: &str`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlaKind {
    /// Inicia na criação/1ª msg; cancela quando humano OU IA responde.
    FirstResponse,
    /// Inicia quando estado vira `awaiting_human`; cancela quando humano responde.
    HumanResponse,
    /// Inicia quando conversa criada sem assignee; cancela ao definir assignee.
    Assignment,
    /// Inicia quando conversa vira `open`; cancela ao virar `resolved`.
    Resolution,
}

impl SlaKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FirstResponse => "first_response",
            Self::HumanResponse => "human_response",
            Self::Assignment => "assignment",
            Self::Resolution => "resolution",
        }
    }

    /// Atraso base (minutos) até o nível 0 da escada, por tipo.
    /// ponytail: todos usam a escada de 3/10/20/40/60; só o ponto de partida
    /// difere. Se um tipo precisar de outra cadência, parametrizar aqui.
    fn base_minutes(&self) -> i16 {
        match self {
            Self::FirstResponse | Self::HumanResponse | Self::Assignment => 3,
            Self::Resolution => 60,
        }
    }
}

/// Status do timer (`sla_timer.status`).
#[allow(dead_code)] // superfície de spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlaStatus {
    Armed,
    Fired,
    Cancelled,
}

impl SlaStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Armed => "armed",
            Self::Fired => "fired",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Linha de `sla_timer` lida do banco.
#[derive(sqlx::FromRow)]
struct SlaTimer {
    id: i64,
    conversation_id: i64,
    kind: String,
    due_at: OffsetDateTime,
    escalation_level: i16,
}

/// Passo da escada de escalonamento (Seção 11.2).
#[allow(dead_code)] // `recipient` reservado para quando houver mapa agent→canal.
struct EscalationStep {
    level: i16,
    delay_accumulated_minutes: i16,
    recipient: &'static str, // "assignee" | "team_member" | "supervisor" | "partner"
    channels: &'static [&'static str],
}

/// Escada padrão (Seção 11.2). Fixa em v1; `config/escalation.toml` entra depois.
const ESCALATION_STEPS: &[EscalationStep] = &[
    EscalationStep { level: 0, delay_accumulated_minutes: 3,  recipient: "assignee",    channels: &["whatsapp"] },
    EscalationStep { level: 1, delay_accumulated_minutes: 10, recipient: "assignee",    channels: &["whatsapp", "telegram"] },
    EscalationStep { level: 2, delay_accumulated_minutes: 20, recipient: "team_member", channels: &["whatsapp"] },
    EscalationStep { level: 3, delay_accumulated_minutes: 40, recipient: "supervisor",  channels: &["whatsapp", "email"] },
    EscalationStep { level: 4, delay_accumulated_minutes: 60, recipient: "partner",     channels: &["whatsapp", "email"] },
];

const MAX_LEVEL: i16 = 4;

/// `NOTIFY_QUIET_HOURS` (22:00–07:00): só nível ≥ 3 notifica (Seção 11.2).
fn is_quiet_hours(local_hour: u8) -> bool {
    local_hour >= 22 || local_hour < 7
}

// ====================================================================
// Horário comercial (Seção 14)
// ====================================================================

/// Config de horário comercial. ponytail: feriados hardcoded mínimos (BR),
/// sem arquivo `business_hours.toml` na v1. Trocar por load do TOML quando a
/// lista municipal crescer.
#[derive(Debug, Clone)]
pub struct BusinessHoursConfig {
    pub start_hour: u8,
    pub end_hour: u8,
    /// Feriados nacionais fixos (mês, dia).
    pub holidays: &'static [(u8, u8)],
    pub recess_start: (u8, u8), // 23/12
    pub recess_end: (u8, u8),   // 6/1
}

impl Default for BusinessHoursConfig {
    fn default() -> Self {
        Self {
            start_hour: 8,
            end_hour: 18,
            // ponytail: feriados nacionais brasileiros. Adicionar municipais
            // quando houver arquivo TOML.
            holidays: &[
                (1, 1),   // Confraternização
                (4, 21),  // Tiradentes
                (5, 1),   // Trabalho
                (9, 7),   // Independência
                (10, 12), // N. Sra. Aparecida
                (11, 2),  // Finados
                (11, 15), // Proclamação da República
                (12, 25), // Natal
            ],
            recess_start: (12, 23),
            recess_end: (1, 6),
        }
    }
}

/// `true` se estamos dentro do horário comercial em America/Sao_Paulo.
///
/// ponytail: offset fixo UTC−3 (São Paulo não usa horário de verão desde
/// 2019). Para suportar DST ou outro fuso, trocar por `time-tz` ou
/// `chrono-tz`. O relógio de SLA só corre quando isto é `true` (Seção 11.2).
fn is_business_hours(cfg: &BusinessHoursConfig, now_utc: OffsetDateTime) -> bool {
    let local = now_utc - Duration::hours(3);
    let date = local.date();
    let wd = date.weekday();
    if matches!(wd, Weekday::Saturday | Weekday::Sunday) {
        return false;
    }
    let hour = local.hour() as u8;
    if hour < cfg.start_hour || hour >= cfg.end_hour {
        return false;
    }
    let m: u8 = date.month().into();
    let d = date.day();
    if cfg.holidays.iter().any(|(hm, hd)| *hm == m && *hd == d) {
        return false;
    }
    // Recesso: de recess_start (dez) até recess_end (jan), ano-agnóstico.
    let in_dec = m == cfg.recess_start.0 && d >= cfg.recess_start.1;
    let in_jan = m == cfg.recess_end.0 && d <= cfg.recess_end.1;
    if in_dec || in_jan {
        return false;
    }
    true
}

// ====================================================================
// Notifier (Seção 11.3)
// ====================================================================

/// Conteúdo de uma notificação a enviar.
pub struct Notification {
    pub conversation_id: i64,
    pub recipient: String,
    pub level: i16,
    pub message: String,
    pub deep_link: String,
}

#[derive(Debug, Error)]
pub enum NotificationError {
    #[error("channel error: {0}")]
    Channel(String),
    #[error("not configured")]
    NotConfigured,
}

/// Canal de notificação plugável (Seção 11.3). WhatsApp/Email entram depois.
#[async_trait]
pub trait Notifier: Send + Sync {
    async fn send(&self, n: &Notification) -> Result<(), NotificationError>;
    fn channel_name(&self) -> &'static str;
}

/// `TelegramNotifier` — primeiro canal por ser o mais simples (sem template,
/// Seção 11.3). POSTa em `https://api.telegram.org/bot{token}/sendMessage`.
///
/// ponytail: um único `chat_id` para todos os destinatários (v1: um
/// plantonista). Quando houver mapa agent→chat, trocar `default_chat_id` por
/// `HashMap<String, i64>` (o campo `chat_id_map` já está previsto no ticket).
pub struct TelegramNotifier {
    bot_token: String,
    default_chat_id: Option<i64>,
    chat_id_map: HashMap<String, i64>,
    http: reqwest::Client,
}

impl TelegramNotifier {
    pub fn new(bot_token: String, default_chat_id: Option<i64>, http: reqwest::Client) -> Self {
        Self {
            bot_token,
            default_chat_id,
            chat_id_map: HashMap::new(),
            http,
        }
    }

    fn resolve_chat_id(&self, recipient: &str) -> Option<i64> {
        self.chat_id_map.get(recipient).copied().or(self.default_chat_id)
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    fn channel_name(&self) -> &'static str {
        "telegram"
    }

    async fn send(&self, n: &Notification) -> Result<(), NotificationError> {
        let chat_id = self
            .resolve_chat_id(&n.recipient)
            .ok_or(NotificationError::NotConfigured)?;
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        // HTML escapado o mínimo: `<` e `&` no corpo da notificação são
        // improváveis (deep link e texto fixo), mas protegemos contra quebra.
        let esc = n.message.replace('&', "&amp;").replace('<', "&lt;");
        let body = serde_json::json!({
            "chat_id": chat_id,
            "text": esc,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| NotificationError::Channel(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(NotificationError::Channel(format!(
                "telegram http {}",
                resp.status().as_u16()
            )));
        }
        Ok(())
    }
}

// ====================================================================
// Estado local (leitura de `conversation_state`)
// ====================================================================

#[derive(sqlx::FromRow)]
struct ConvStateRow {
    conversation_id: i64,
    account_id: i64,
    contact_id: i64,
    channel: String,
    ai_state: String,
    chatwoot_status: String,
    assignee_id: Option<i64>,
    team_id: Option<i64>,
    labels: Vec<String>,
}

/// Formata a mensagem no padrão da Seção 11.3.
fn format_notification(
    conv: &ConvStateRow,
    contact_name: &str,
    step: &EscalationStep,
    deep_link: &str,
) -> String {
    let canal = pretty_channel(&conv.channel);
    let fila = conv
        .team_id
        .map(|t| t.to_string())
        .unwrap_or_else(|| "—".to_string());
    let assunto = conv
        .labels
        .iter()
        .find(|l| !l.starts_with("ia:") && !l.starts_with("sla:"))
        .cloned()
        .unwrap_or_else(|| "Atendimento pendente".to_string());
    let cliente = if contact_name.is_empty() {
        "—".to_string()
    } else {
        contact_name.to_string()
    };
    format!(
        "🔔 Atendimento pendente há {min} min\n\n\
         Cliente: {cliente}\n\
         Assunto: {assunto}\n\
         Canal: {canal}\n\
         Fila: {fila}\n\n\
         Abrir: {link}",
        min = step.delay_accumulated_minutes,
        cliente = cliente,
        assunto = assunto,
        canal = canal,
        fila = fila,
        link = deep_link,
    )
}

fn pretty_channel(c: &str) -> &'static str {
    let c = c.to_ascii_lowercase();
    if c.contains("whatsapp") {
        "WhatsApp"
    } else if c.contains("instagram") {
        "Instagram"
    } else if c.contains("email") {
        "E-mail"
    } else if c.contains("widget") || c.contains("web") {
        "Widget"
    } else {
        "Chat"
    }
}

fn deep_link(base_url: &str, account_id: i64, conv_id: i64) -> String {
    format!(
        "{}/app/accounts/{}/conversations/{}",
        base_url.trim_end_matches('/'),
        account_id,
        conv_id
    )
}

// ====================================================================
// Anti-flood (Seção 11.2)
// ====================================================================

/// `true` se o `recipient` ainda pode ser notificado nesta hora.
/// Conta `notification_log` na última 1h; excedente vira digest (só log).
async fn can_notify(
    pool: &PgPool,
    recipient: &str,
    max_per_hour: u32,
) -> bool {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification_log \
         WHERE recipient = $1 AND created_at > now() - interval '1 hour'",
    )
    .bind(recipient)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    (count as u32) < max_per_hour
}

// ====================================================================
// Job: check_sla_timers (a cada 30s, Seção 11.2)
// ====================================================================

async fn check_sla_timers(
    pool: PgPool,
    chatwoot: Arc<chatwoot::ChatwootClient>,
    notifier: Arc<dyn Notifier>,
    config: Arc<Config>,
    bh: Arc<BusinessHoursConfig>,
) {
    let now_utc = OffsetDateTime::now_utc();
    if !is_business_hours(&bh, now_utc) {
        // ponytail: relógio de SLA só corre em horário comercial. Fora dele,
        // nem varremos — timers permanecem armed e disparam quando voltar.
        return;
    }
    let local_hour = (now_utc - Duration::hours(3)).hour() as u8;
    let quiet = is_quiet_hours(local_hour);

    let timers: Vec<SlaTimer> = match sqlx::query_as(
        "SELECT id, conversation_id, kind, due_at, escalation_level \
         FROM sla_timer WHERE status = 'armed' AND due_at <= now()",
    )
    .fetch_all(&pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            error!(error = %e, "sla_timer query failed");
            return;
        }
    };

    for t in timers {
        if let Err(e) = process_timer(&t, &pool, &chatwoot, &notifier, &config, &bh, quiet).await {
            warn!(conversation_id = t.conversation_id, error = %e, "sla timer processing failed");
        }
    }
}

async fn process_timer(
    t: &SlaTimer,
    pool: &PgPool,
    chatwoot: &chatwoot::ChatwootClient,
    notifier: &Arc<dyn Notifier>,
    config: &Config,
    bh: &BusinessHoursConfig,
    quiet: bool,
) -> Result<()> {
    // Recheca horário comercial por chamada (recupera se a janela mudou entre
    // o tick e aqui). Fora do horário comercial, o relógio não corre.
    if !is_business_hours(bh, OffsetDateTime::now_utc()) {
        return Ok(());
    }

    let level = t.escalation_level.clamp(0, MAX_LEVEL);
    let step = &ESCALATION_STEPS[level as usize];

    // Quiet hours: só nível >= 3 notifica (Seção 11.2).
    if quiet && level < 3 {
        return Ok(());
    }

    // Carrega estado local.
    let state: ConvStateRow = match sqlx::query_as::<_, ConvStateRow>(
        "SELECT conversation_id, account_id, contact_id, channel, ai_state, \
                chatwoot_status, assignee_id, team_id, labels \
         FROM conversation_state WHERE conversation_id = $1",
    )
    .bind(t.conversation_id)
    .fetch_optional(pool)
    .await?
    {
        Some(s) => s,
        None => {
            // Estado sumiu (conversa purgada?): cancela o timer.
            cancel_timer(pool, t.id, "state_missing").await?;
            return Ok(());
        }
    };

    // Humano resolveu/assumiu → cancela todos os timers da conversa (Seção 11.1).
    if state.ai_state == "closed" || state.ai_state == "human_handling" {
        cancel_timer(pool, t.id, "human_handling").await?;
        return Ok(());
    }

    // Idempotência (Seção 11.2): já notificado para este nível?
    let already: Option<i64> = sqlx::query_scalar(
        "SELECT 1::bigint FROM notification_log \
         WHERE conversation_id = $1 AND sla_kind = $2 AND level = $3 LIMIT 1",
    )
    .bind(t.conversation_id)
    .bind(&t.kind)
    .bind(level)
    .fetch_optional(pool)
    .await?;
    if already.is_some() {
        // Já notificado (recuperação de crash): só avança.
        return advance_timer(pool, chatwoot, config, t, level).await;
    }

    // Anti-flood: recipient = assignee (id) ou "unassigned".
    let recipient = state
        .assignee_id
        .map(|i| i.to_string())
        .unwrap_or_else(|| "unassigned".to_string());
    if !can_notify(pool, &recipient, config.notification.max_per_agent_per_hour).await {
        warn!(conversation_id = t.conversation_id, recipient = %recipient, "anti-flood: notificação virou digest");
        return Ok(());
    }

    // Monta e envia a notificação.
    let contact_name = chatwoot
        .get_contact_name(state.contact_id)
        .await
        .unwrap_or_default();
    let link = deep_link(&config.chatwoot.base_url, state.account_id, t.conversation_id);
    let message = format_notification(&state, &contact_name, step, &link);

    let n = Notification {
        conversation_id: t.conversation_id,
        recipient: recipient.clone(),
        level,
        message: message.clone(),
        deep_link: link.clone(),
    };

    // Envia só pelos canais implementados. v1: Telegram (Seção 11.3).
    // WhatsApp/Email: logado como pendente até seus notifiers existirem.
    let mut any_sent = false;
    for ch in step.channels {
        if *ch == notifier.channel_name() {
            match notifier.send(&n).await {
                Ok(()) => {
                    any_sent = true;
                    log_notification(pool, t, level, ch, &recipient, "sent").await;
                }
                Err(e) => {
                    warn!(conversation_id = t.conversation_id, channel = ch, error = %e, "notify failed");
                    log_notification(pool, t, level, ch, &recipient, "failed").await;
                }
            }
        } else {
            // Canal ainda não implementado em v1: registra como "pending".
            log_notification(pool, t, level, ch, &recipient, "pending").await;
        }
    }

    if !any_sent {
        // Nada foi efetivamente enviado; não avança para não pular nível sem
        // avisar ninguém. Próximo tick tenta de novo (idempotência segura).
        return Ok(());
    }

    advance_timer(pool, chatwoot, config, t, level).await
}

/// Avança o timer para o próximo nível da escada, ou finaliza no nível 4.
async fn advance_timer(
    pool: &PgPool,
    chatwoot: &chatwoot::ChatwootClient,
    _config: &Config,
    t: &SlaTimer,
    level: i16,
) -> Result<()> {
    if level >= MAX_LEVEL {
        // Nível 4: dispara e etiqueta `sla:violado` (C8, Seção 11.2).
        let _ = sqlx::query("UPDATE sla_timer SET status = 'fired' WHERE id = $1")
            .bind(t.id)
            .execute(pool)
            .await;
        if let Err(e) = chatwoot.add_label(t.conversation_id, SLA_VIOLATED_LABEL).await {
            warn!(conversation_id = t.conversation_id, error = %e, "failed to add sla:violado label");
        }
        info!(conversation_id = t.conversation_id, "SLA violado (nível 4)");
        return Ok(());
    }
    let next = &ESCALATION_STEPS[(level + 1) as usize];
    let delta_minutes = next.delay_accumulated_minutes - ESCALATION_STEPS[level as usize].delay_accumulated_minutes;
    let new_due = OffsetDateTime::now_utc() + Duration::minutes(delta_minutes as i64);
    sqlx::query(
        "UPDATE sla_timer SET escalation_level = $1, due_at = $2 WHERE id = $3",
    )
    .bind(level + 1)
    .bind(new_due)
    .bind(t.id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn cancel_timer(pool: &PgPool, id: i64, reason: &str) -> Result<()> {
    sqlx::query(
        "UPDATE sla_timer SET status = 'cancelled', cancelled_reason = $1 WHERE id = $2",
    )
    .bind(reason)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Registra em `notification_log` (UNIQUE idempotente — Seção 11.2).
async fn log_notification(
    pool: &PgPool,
    t: &SlaTimer,
    level: i16,
    channel: &str,
    recipient: &str,
    state: &str,
) {
    // ponytail: ON CONFLICT DO NOTHING garante no-máximo-uma por
    // (conv, sla_kind, level, channel). O `state` de uma linha já existente
    // não é sobrescrito — primeira escrita vence.
    if let Err(e) = sqlx::query(
        "INSERT INTO notification_log \
            (conversation_id, sla_kind, level, recipient, channel, state) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (conversation_id, sla_kind, level, channel) DO NOTHING",
    )
    .bind(t.conversation_id)
    .bind(&t.kind)
    .bind(level)
    .bind(recipient)
    .bind(channel)
    .bind(state)
    .execute(pool)
    .await
    {
        warn!(conversation_id = t.conversation_id, error = %e, "notification_log insert failed");
    }
}

// ====================================================================
// Timers SLA — UPSERT (Seção 11.1)
// ====================================================================

/// Garante que existe um timer `armed` para `(conv_id, kind)`.
/// `due_at = now + base_minutes(kind)`. Idempotente por UNIQUE(conversation_id, kind).
///
/// ponytail: `due_at` em relógio de parede (não pula feriados). O check de
/// `is_business_hours` no `check_sla_timers` impede disparo fora do horário
/// comercial — o efeito prático é o mesmo: mensagem das 23h não escala 23h03.
#[allow(dead_code)] // chamado por bridge-api/worker ao criar/transicionar conversa.
pub async fn ensure_sla_timers(pool: &PgPool, conv_id: i64, kind: &str) -> Result<()> {
    let mins = match kind {
        "first_response" | "human_response" | "assignment" => 3_i64,
        "resolution" => 60_i64,
        _ => 3_i64,
    };
    let due_at = OffsetDateTime::now_utc() + Duration::minutes(mins);
    sqlx::query(
        "INSERT INTO sla_timer (conversation_id, kind, due_at, escalation_level, status) \
         VALUES ($1, $2, $3, 0, 'armed') \
         ON CONFLICT (conversation_id, kind) DO NOTHING",
    )
    .bind(conv_id)
    .bind(kind)
    .bind(due_at)
    .execute(pool)
    .await?;
    Ok(())
}

// ====================================================================
// Job: reconciliação (a cada 5min, Seção 11.4)
// ====================================================================

async fn reconcile(pool: PgPool, chatwoot: Arc<chatwoot::ChatwootClient>, redis: deadpool_redis::Pool) {
    // 1. Espelha status `open` do Chatwoot no estado local (webhook perdido).
    if let Err(e) = reconcile_open_status(&pool, &chatwoot).await {
        warn!(error = %e, "reconcile: open status failed");
    }
    // 2. Conversas em `ai_thinking` há > 5 min → unlock + run stale.
    if let Err(e) = reconcile_stale_thinking(&pool, &redis).await {
        warn!(error = %e, "reconcile: stale thinking failed");
    }
    // 3. outbound_message pending há > 2 min → verifica no Chatwoot.
    if let Err(e) = reconcile_outbound(&pool, &chatwoot).await {
        warn!(error = %e, "reconcile: outbound failed");
    }
}

async fn reconcile_open_status(
    pool: &PgPool,
    chatwoot: &chatwoot::ChatwootClient,
) -> Result<()> {
    let remote_ids = chatwoot.list_open_conversations().await.unwrap_or_default();
    if remote_ids.is_empty() {
        return Ok(());
    }
    // ponytail: sincronização unidirecional Chatwoot→local (Chatwoot é fonte
    // da verdade, Seção 1.2). Marcamos como `open` tudo que CW diz `open`;
    // divergências locais (webhook perdido) são corrigidas aqui.
    for id in &remote_ids {
        let _ = sqlx::query(
            "UPDATE conversation_state SET chatwoot_status = 'open', updated_at = now() \
             WHERE conversation_id = $1 AND chatwoot_status <> 'open'",
        )
        .bind(id)
        .execute(pool)
        .await;
    }
    Ok(())
}

async fn reconcile_stale_thinking(
    pool: &PgPool,
    redis: &deadpool_redis::Pool,
) -> Result<()> {
    // ponytail: conversas travadas em ai_thinking > 5 min indicam worker
    // morto no meio de um run. Forçamos unlock (DEL lock:conv) e marcamos o
    // run como stale para a máquina de estados voltar a responder.
    let stuck: Vec<i64> = sqlx::query_scalar(
        "SELECT conversation_id FROM conversation_state \
         WHERE ai_state = 'ai_thinking' AND updated_at < now() - interval '5 minutes'",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    for conv_id in &stuck {
        // Marca runs running como stale.
        let _ = sqlx::query(
            "UPDATE agent_run SET status = 'stale', finished_at = now() \
             WHERE conversation_id = $1 AND status = 'running'",
        )
        .bind(conv_id)
        .execute(pool)
        .await;

        // Força unlock do lock de conversa (Seção 6.3).
        // ponytail: DEL direto sem checagem de token — o run stale implica
        // worker morto; o token não será revalidado por ninguém. Em operação
        // normal a liberação é por script Lua com token (worker).
        let lock_key = format!("lock:conv:{conv_id}");
        if let Ok(mut conn) = redis.get().await {
            let _: redis::RedisResult<i64> =
                redis::cmd("DEL").arg(&lock_key).query_async(&mut *conn).await;
        }

        // Devolve a conversa para ai_active.
        let _ = sqlx::query(
            "UPDATE conversation_state SET ai_state = 'ai_active', updated_at = now() \
             WHERE conversation_id = $1",
        )
        .bind(conv_id)
        .execute(pool)
        .await;

        info!(conversation_id = conv_id, "reconcile: stale ai_thinking unlocked");
    }
    Ok(())
}

async fn reconcile_outbound(
    pool: &PgPool,
    chatwoot: &chatwoot::ChatwootClient,
) -> Result<()> {
    // ponytail: para cada outbound pending há > 2 min, se já temos
    // chatwoot_msg_id confiramos como enviada; caso contrário reenvia uma
    // vez (attempts < 3) e marca como abandoned após 3 tentativas.
    // Lemos `payload::text` porque o sqlx do workspace não tem feature `json`;
    // parseamos com serde_json::from_str no reenvio.
    let rows: Vec<(i64, i64, Option<i64>, i16, String)> = sqlx::query_as(
        "SELECT id, conversation_id, chatwoot_msg_id, attempts, payload::text \
         FROM outbound_message \
         WHERE state = 'pending' AND created_at < now() - interval '2 minutes'",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    for (id, conv_id, msg_id, attempts, payload_text) in rows {
        let payload: serde_json::Value = serde_json::from_str(&payload_text).unwrap_or_default();
        if let Some(mid) = msg_id {
            // Já temos id — confirma no Chatwoot antes de marcar sent.
            match chatwoot.conversation_has_message(conv_id, mid).await {
                Ok(true) => {
                    let _ = sqlx::query(
                        "UPDATE outbound_message SET state = 'sent', sent_at = now() WHERE id = $1",
                    )
                    .bind(id)
                    .execute(pool)
                    .await;
                    continue;
                }
                Ok(false) => {
                    // Não chegou — cai no reenvio abaixo.
                }
                Err(e) => {
                    warn!(outbound_id = id, error = %e, "outbound check failed");
                    continue;
                }
            }
        }
        // Reenvio simples: incrementa attempts; após 3, abandona.
        if attempts >= 3 {
            let _ = sqlx::query(
                "UPDATE outbound_message SET state = 'abandoned', last_error = 'max_attempts' WHERE id = $1",
            )
            .bind(id)
            .execute(pool)
            .await;
            continue;
        }
        // Tenta reenviar o payload (C1) via POST /messages.
        let url = format!(
            "{}/api/v1/accounts/{}/conversations/{}/messages",
            chatwoot.base(), chatwoot.account_id(), conv_id
        );
        match chatwoot.repost_message(&url, &payload).await {
            Ok(new_id) => {
                let _ = sqlx::query(
                    "UPDATE outbound_message SET state = 'sent', chatwoot_msg_id = $2, \
                     sent_at = now(), attempts = attempts + 1 WHERE id = $1",
                )
                .bind(id)
                .bind(new_id)
                .execute(pool)
                .await;
            }
            Err(e) => {
                let _ = sqlx::query(
                    "UPDATE outbound_message SET attempts = attempts + 1, last_error = $2 WHERE id = $1",
                )
                .bind(id)
                .bind(e.to_string())
                .execute(pool)
                .await;
            }
        }
    }
    Ok(())
}

// ====================================================================
// Job: purga de dados (a cada 24h, Seção 10.4)
// ====================================================================

async fn purge_old_data(pool: PgPool, retention_days: u32) {
    let cutoff = OffsetDateTime::now_utc() - Duration::days(retention_days as i64);
    // message_log, agent_run, notification_log: TTL = DATA_RETENTION_DAYS.
    // audit_log retém 5 anos sem conteúdo — não tocamos aqui (Seção 10.4).
    for (label, sql) in [
        ("message_log", "DELETE FROM message_log WHERE created_at < $1"),
        ("agent_run", "DELETE FROM agent_run WHERE started_at < $1"),
        ("notification_log", "DELETE FROM notification_log WHERE created_at < $1"),
    ] {
        match sqlx::query(sql).bind(cutoff).execute(&pool).await {
            Ok(res) => info!(table = label, rows = res.rows_affected(), "purge done"),
            Err(e) => error!(table = label, error = %e, "purge failed"),
        }
    }
}

// ====================================================================
// main
// ====================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // tracing: JSON em produção, pretty em dev. LOG_LEVEL via env (Seção 14).
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // 1. Config (Seção 14). ponytail: o crate expõe `Config::load()`, não
    //    `from_env()` — `load()` já lê todas as env vars via figment.
    let config = Config::load().map_err(anyhow::Error::msg)?;
    info!(account = config.chatwoot.account_id, "bridge-scheduler starting");

    // 2. Postgres.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(config.infra.database_url.expose())
        .await?;

    // 3. Redis (deadpool).
    let redis_cfg = deadpool_redis::Config::from_url(config.infra.redis_url.expose());
    let redis_pool = redis_cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;

    // 4. ChatwootClient.
    let http = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(10))
        .build()?;
    let chatwoot = Arc::new(chatwoot::ChatwootClient::new(&config.chatwoot, http.clone()));

    // 5. Notifier (Telegram primeiro, Seção 11.3). chat_id único para v1.
    let tg_chat: Option<i64> = std::env::var("TELEGRAM_CHAT_ID")
        .ok()
        .and_then(|s| s.trim().parse().ok());
    let notifier: Arc<dyn Notifier> = Arc::new(TelegramNotifier::new(
        config.notification.telegram_bot_token.expose().to_string(),
        tg_chat,
        http,
    ));

    let bh = Arc::new(BusinessHoursConfig::default());
    let config = Arc::new(config);

    // 6. Agendamentos. Cada job é um loop `tokio::time::interval` que também
    // escuta um oneshot de shutdown (Ctrl+C). ponytail: `&mut rx` funciona no
    // select! porque `oneshot::Receiver` é `Unpin`.
    let (p1, c1) = {
        let pool = pool.clone();
        let cw = chatwoot.clone();
        let n = notifier.clone();
        let cfg = config.clone();
        let bh = bh.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(StdDuration::from_secs(30));
            let mut rx = rx;
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        check_sla_timers(
                            pool.clone(), cw.clone(), n.clone(), cfg.clone(), bh.clone(),
                        ).await;
                    }
                    _ = &mut rx => break,
                }
            }
        });
        (tx, handle)
    };

    let (p2, c2) = {
        let pool = pool.clone();
        let cw = chatwoot.clone();
        let rp = redis_pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(StdDuration::from_secs(300));
            let mut rx = rx;
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        reconcile(pool.clone(), cw.clone(), rp.clone()).await;
                    }
                    _ = &mut rx => break,
                }
            }
        });
        (tx, handle)
    };

    let (p3, c3) = {
        let pool = pool.clone();
        let retention = config.infra.data_retention_days;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            // ponytail: 24h. Primeiro tick imediato roda purge ao subir.
            let mut tick = tokio::time::interval(StdDuration::from_secs(86_400));
            let mut rx = rx;
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        purge_old_data(pool.clone(), retention).await;
                    }
                    _ = &mut rx => break,
                }
            }
        });
        (tx, handle)
    };

    // 7. Aguardar signal (Ctrl+C).
    info!("bridge-scheduler running — Ctrl+C to stop");
    signal::ctrl_c().await?;
    info!("shutdown signal received");
    let _ = p1.send(());
    let _ = p2.send(());
    let _ = p3.send(());
    // Dá um tique para os loops saírem graciosamente.
    let _ = tokio::time::timeout(StdDuration::from_secs(2), async {
        let _ = c1.await;
        let _ = c2.await;
        let _ = c3.await;
    })
    .await;
    Ok(())
}
