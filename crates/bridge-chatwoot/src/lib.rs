//! bridge-chatwoot — cliente HTTP da Application API do Chatwoot + verificação
//! HMAC de webhooks.
//!
//! Spec normativa: `ESPECchatwootaibridge.md`, **Seção 4 (Integração com o
//! Chatwoot)**. Este crate implementa:
//! - **4.4** verificação de assinatura HMAC (tempo constante, replay guard).
//! - **4.3 / 4.5** payloads de webhook do Agent Bot, tolerantes a campos extras.
//! - **4.6 (C1–C13)** chamadas de saída à Application API.
//! - **4.7** retry com backoff + jitter, circuit breaker e idempotência.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tokio::time::{sleep, Instant};
use url::Url;

use bridge_core::{RunId, SecretString};

// ====================================================================
// 4.4 — Verificação de assinatura HMAC
// ====================================================================

/// Janela máxima de aceitação do timestamp (300s). Spec 4.4 regra 3.
pub const WEBHOOK_MAX_SKEW_SECS: i64 = 300;

/// Verifica a assinatura HMAC-SHA256 do Chatwoot em tempo constante.
///
/// `header_sig` vem do header `X-Chatwoot-Signature` no formato `sha256=<hex>`.
/// A mensagem assinada é `timestamp + "." + raw_body` (corpo bruto, nunca
/// desserializado e re-serializado). Implementação literal do pseudocódigo da
/// Spec 4.4.
pub fn verify_webhook_signature(
    secret: &[u8],
    timestamp: &str,
    raw_body: &[u8],
    header_sig: &str,
) -> bool {
    // `new_from_slice` nunca erro para HMAC-SHA256 (chave de tamanho arbitrário).
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(raw_body);
    let expected = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    expected.as_bytes().ct_eq(header_sig.as_bytes()).into()
}

/// Tenta cada secret configurado (rotação sem downtime, Spec 4.4 regra 5).
/// Aceita se qualquer um bater.
pub fn verify_webhook_signature_any(
    secrets: &[&[u8]],
    timestamp: &str,
    raw_body: &[u8],
    header_sig: &str,
) -> bool {
    secrets
        .iter()
        .any(|s| verify_webhook_signature(s, timestamp, raw_body, header_sig))
}

/// `true` se `timestamp` (Unix, segundos) está dentro da janela de skew.
/// Spec 4.4 regra 3 — proteção contra replay.
pub fn is_timestamp_fresh(timestamp: &str, now_secs: i64, max_skew: i64) -> bool {
    match timestamp.trim().parse::<i64>() {
        Ok(ts) => (now_secs - ts).abs() <= max_skew,
        Err(_) => false,
    }
}

/// Conveniência: frescor contra o relógio atual do sistema.
pub fn is_timestamp_fresh_now(timestamp: &str, max_skew: i64) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    is_timestamp_fresh(timestamp, now, max_skew)
}

// ====================================================================
// 4.3 / 4.5 — Payloads de webhook do Agent Bot
// ====================================================================
//
// Todos os structs usam `#[serde(default)]` e `Option<T>` — a ponte DEVE ser
// tolerante a campos extras e ausentes (Spec 4.5). `deny_unknown_fields`
// intencionalmente ausente.

/// Discriminação do remetente — tabela crítica da Spec 4.5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderKind {
    /// `message_type=incoming` + `sender.type=contact` → processar.
    Contact,
    /// `message_type=outgoing` + `sender.type=User` → pausa IA + cancela SLA.
    User,
    /// `message_type=outgoing` + `sender.type=AgentBot` → descartar sempre (eco).
    AgentBot,
    /// `activity` / `template` / qualquer outro evento de sistema.
    System,
}

impl SenderKind {
    /// `true` somente para mensagem de cliente que alimenta o buffer.
    pub fn is_contact(self) -> bool {
        matches!(self, Self::Contact)
    }
    /// `true` para eco da própria IA (guard anti-loop G2).
    pub fn is_agent_bot(self) -> bool {
        matches!(self, Self::AgentBot)
    }
}

/// Payload de entrada do webhook do Agent Bot (Spec 4.3, 4.5).
/// Tolerante a campos extras e ausentes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentBotWebhookPayload {
    pub event: String,
    pub id: Option<i64>,
    pub content: Option<String>,
    pub message_type: Option<String>,
    pub private: Option<bool>,
    pub conversation: Option<ConversationSummary>,
    pub sender: Option<SenderInfo>,
    pub account: Option<AccountInfo>,
    pub inbox: Option<InboxInfo>,
    pub created_at: Option<String>,
}

impl AgentBotWebhookPayload {
    /// Classifica o remetente conforme a tabela da Spec 4.5.
    /// Olha `message_type` × `sender.type`. A verificação de `private` é
    /// separada (nota interna → descartar sempre).
    pub fn sender_kind(&self) -> SenderKind {
        let mt = self.message_type.as_deref().unwrap_or("");
        let st = self
            .sender
            .as_ref()
            .and_then(|s| s.r#type.as_deref())
            .unwrap_or("");
        match (mt, st) {
            ("incoming", "contact") => SenderKind::Contact,
            ("outgoing", "User") => SenderKind::User,
            ("outgoing", "AgentBot") => SenderKind::AgentBot,
            _ => SenderKind::System,
        }
    }

    /// `true` se é nota interna (Spec 4.5: `private == true` → descartar sempre).
    pub fn is_private_note(&self) -> bool {
        self.private.unwrap_or(false)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConversationSummary {
    pub id: i64,
    pub status: Option<String>,
    pub inbox_id: Option<i64>,
    pub channel: Option<String>,
    pub labels: Option<Vec<String>>,
    pub meta: Option<ConversationMeta>,
    pub custom_attributes: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConversationMeta {
    pub assignee: Option<AgentInfo>,
    pub team: Option<TeamInfo>,
    pub sender: Option<SenderInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SenderInfo {
    pub id: i64,
    pub name: Option<String>,
    pub r#type: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentInfo {
    pub id: i64,
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TeamInfo {
    pub id: i64,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AccountInfo {
    pub id: i64,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InboxInfo {
    pub id: i64,
    pub name: Option<String>,
}

// ====================================================================
// Erros (Seção 4)
// ====================================================================

#[derive(Debug, thiserror::Error)]
pub enum ChatwootError {
    #[error("api error: {status} {body}")]
    ApiError { status: u16, body: String },
    #[error("not found")]
    NotFound,
    #[error("auth error")]
    AuthError,
    #[error("rate limited")]
    RateLimited,
    #[error("timeout")]
    Timeout,
    #[error("circuit open")]
    CircuitOpen,
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Resposta genérica da Application API.
/// ponytail: alias para `serde_json::Value` — estreitar para struct quando o
/// caller precisar de campos específicos (id da mensagem criada etc.).
pub type ChatwootResponse = serde_json::Value;

/// Item de `input_select` (C4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectItem {
    pub title: String,
    pub value: String,
}

// ====================================================================
// 4.7 — Circuit breaker
// ====================================================================

/// Limiar de falhas consecutivas que abre o circuito. Spec 4.7: 5.
const FAILURE_THRESHOLD: u32 = 5;
/// Tempo que o circuito fica aberto antes de half-open. Spec 4.7: 60s.
const OPEN_DURATION_SECS: u64 = 60;

#[derive(Debug)]
enum CircuitState {
    Closed { failures: u32 },
    Open { until: Instant },
    HalfOpen,
}

impl Default for CircuitState {
    fn default() -> Self {
        Self::Closed { failures: 0 }
    }
}

// ====================================================================
// 4.6 — Cliente da Application API
// ====================================================================

/// Cliente HTTP da Application API do Chatwoot (Seção 4.6, C1–C13).
///
/// Autenticação: header `api_access_token` (token do Agent Bot, Spec 4.2).
/// Retry/circuit breaker/idempotência conforme Seção 4.7.
pub struct ChatwootClient {
    base_url: Url,
    bot_token: SecretString,
    account_id: i64,
    http_client: Client,
    /// ponytail: `std::sync::Mutex` simples basta para v1 (Spec 4.7 diz para
    /// fazer assim). Contenção sob alta concorrência é aceitável; trocar por
    /// `AtomicU8` + cargos se throughput da API do Chatwoot for o gargalo.
    circuit_state: Arc<Mutex<CircuitState>>,
}

impl ChatwootClient {
    /// Constrói o cliente. Monta a base URL e registra o header `api_access_token`
    /// como default em todas as chamadas (Spec 4.6).
    // ponytail: `new -> Self` (não-Result) por contrato; falhas aqui são erros
    // de config (URL/token inválidos) — fail fast com expect é aceitável na
    // construção. Trocar por `try_new -> Result` se a config puder vir suja.
    pub fn new(base_url: &str, bot_token: &str, account_id: i64) -> Self {
        let base = if base_url.ends_with('/') {
            base_url.to_string()
        } else {
            format!("{base_url}/")
        };
        let base_url = Url::parse(&base).expect("CHATWOOT_BASE_URL inválida");

        let mut headers = HeaderMap::new();
        headers.insert(
            "api_access_token",
            HeaderValue::from_str(bot_token).expect("CHATWOOT_BOT_TOKEN com chars inválidos"),
        );
        let http_client = Client::builder()
            .timeout(Duration::from_secs(10)) // Spec 4.7: 10s por chamada
            .default_headers(headers)
            .build()
            .expect("reqwest client build");

        Self {
            base_url,
            bot_token: SecretString::from(bot_token.to_string()),
            account_id,
            http_client,
            circuit_state: Arc::new(Mutex::new(CircuitState::default())),
        }
    }

    /// Accessors de inspeção (testes/health).
    pub fn account_id(&self) -> i64 {
        self.account_id
    }
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }
    pub fn bot_token(&self) -> &SecretString {
        &self.bot_token
    }

    // ---- Helpers de URL ----

    fn url(&self, path: &str) -> Url {
        self.base_url.join(path).expect("caminho de URL inválido")
    }

    fn conv_path(&self, conv_id: i64, suffix: &str) -> Url {
        self.url(&format!(
            "api/v1/accounts/{}/conversations/{conv_id}/{suffix}",
            self.account_id
        ))
    }

    fn messages_url(&self, conv_id: i64) -> Url {
        self.conv_path(conv_id, "messages")
    }

    // ---- C1: Enviar mensagem ao cliente ----
    pub async fn send_message(
        &self,
        conv_id: i64,
        content: &str,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let key = self.idempotency_key("send_message");
        let url = self.messages_url(conv_id);
        let body = serde_json::json!({
            "content": content,
            "message_type": "outgoing",
            "content_type": "text",
            "private": false,
        });
        let resp = self
            .call_with_retry("send_message", || {
                self.http_client
                    .post(url.clone())
                    .header("Idempotency-Key", &key)
                    .json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C2: Nota interna ----
    pub async fn send_private_note(
        &self,
        conv_id: i64,
        content: &str,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let key = self.idempotency_key("send_private_note");
        let url = self.messages_url(conv_id);
        let body = serde_json::json!({
            "content": content,
            "message_type": "outgoing",
            "private": true,
        });
        let resp = self
            .call_with_retry("send_private_note", || {
                self.http_client
                    .post(url.clone())
                    .header("Idempotency-Key", &key)
                    .json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C3: Anexo (multipart/form-data) ----
    pub async fn send_attachment(
        &self,
        conv_id: i64,
        content: &str,
        file_bytes: Vec<u8>,
        filename: &str,
        mime: &str,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let key = self.idempotency_key("send_attachment");
        let url = self.messages_url(conv_id);
        // ponytail: o form é reconstruído a cada tentativa, clonando file_bytes.
        // Aceitável para v1 (≤20MB/turno, ≤5 anexos, Spec 6.5 L8). Evitar clone
        // exigiria RequestBuilder clonável, que reqwest não expõe.
        let resp = self
            .call_with_retry("send_attachment", || {
                let part = reqwest::multipart::Part::bytes(file_bytes.clone())
                    .file_name(filename.to_string())
                    .mime_str(mime)
                    .expect("mime inválido");
                let form = reqwest::multipart::Form::new()
                    .text("content", content.to_string())
                    .text("message_type", "outgoing".to_string())
                    .part("attachments[]", part);
                self.http_client
                    .post(url.clone())
                    .header("Idempotency-Key", &key)
                    .multipart(form)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C4: Botões input_select ----
    pub async fn send_input_select(
        &self,
        conv_id: i64,
        content: &str,
        items: Vec<SelectItem>,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let url = self.messages_url(conv_id);
        let items_json: Vec<serde_json::Value> = items
            .into_iter()
            .map(|i| serde_json::json!({ "title": i.title, "value": i.value }))
            .collect();
        let body = serde_json::json!({
            "content": content,
            "content_type": "input_select",
            "content_attributes": { "items": items_json },
            "message_type": "outgoing",
            "private": false,
        });
        let resp = self
            .call_with_retry("send_input_select", || {
                self.http_client.post(url.clone()).json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C5: Toggle status ----
    pub async fn toggle_status(
        &self,
        conv_id: i64,
        status: &str,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let url = self.conv_path(conv_id, "toggle_status");
        let body = serde_json::json!({ "status": status });
        let resp = self
            .call_with_retry("toggle_status", || {
                self.http_client.post(url.clone()).json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C6: Assign agent ----
    pub async fn assign_agent(
        &self,
        conv_id: i64,
        assignee_id: i64,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let url = self.conv_path(conv_id, "assignments");
        let body = serde_json::json!({ "assignee_id": assignee_id });
        let resp = self
            .call_with_retry("assign_agent", || {
                self.http_client.post(url.clone()).json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C7: Assign team ----
    pub async fn assign_team(
        &self,
        conv_id: i64,
        team_id: i64,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let url = self.conv_path(conv_id, "assignments");
        let body = serde_json::json!({ "team_id": team_id });
        let resp = self
            .call_with_retry("assign_team", || {
                self.http_client.post(url.clone()).json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C8: Set labels (ATENÇÃO: substitui o conjunto inteiro) ----
    pub async fn set_labels(
        &self,
        conv_id: i64,
        labels: &[String],
    ) -> Result<ChatwootResponse, ChatwootError> {
        let url = self.conv_path(conv_id, "labels");
        let body = serde_json::json!({ "labels": labels });
        let resp = self
            .call_with_retry("set_labels", || {
                self.http_client.post(url.clone()).json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C9: Get current labels ----
    /// Tolerante ao shape do Chatwoot: aceita `["a","b"]`, `{"labels":["a","b"]}`
    /// ou `{"labels":[{"title":"a",...}]}`.
    pub async fn get_labels(&self, conv_id: i64) -> Result<Vec<String>, ChatwootError> {
        let url = self.conv_path(conv_id, "labels");
        let resp = self
            .call_with_retry("get_labels", || self.http_client.get(url.clone()))
            .await?;
        let val: serde_json::Value = self.parse_json(resp).await?;
        let arr = val
            .get("labels")
            .and_then(|v| v.as_array())
            .or_else(|| val.as_array());
        let out = arr
            .map(|a| {
                a.iter()
                    .filter_map(|i| {
                        if let Some(s) = i.as_str() {
                            Some(s.to_string())
                        } else {
                            i.get("title")
                                .or_else(|| i.get("name"))
                                .and_then(|t| t.as_str())
                                .map(|s| s.to_string())
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(out)
    }

    // ---- C10: Update custom attributes ----
    pub async fn update_custom_attributes(
        &self,
        conv_id: i64,
        attributes: serde_json::Value,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let url = self.conv_path(conv_id, "custom_attributes");
        let body = serde_json::json!({ "custom_attributes": attributes });
        let resp = self
            .call_with_retry("update_custom_attributes", || {
                self.http_client.post(url.clone()).json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ---- C11: Get conversation ----
    pub async fn get_conversation(
        &self,
        conv_id: i64,
    ) -> Result<serde_json::Value, ChatwootError> {
        let url = self.url(&format!(
            "api/v1/accounts/{}/conversations/{conv_id}",
            self.account_id
        ));
        let resp = self
            .call_with_retry("get_conversation", || self.http_client.get(url.clone()))
            .await?;
        self.parse_json(resp).await
    }

    // ---- C12: Get contact ----
    pub async fn get_contact(
        &self,
        contact_id: i64,
    ) -> Result<serde_json::Value, ChatwootError> {
        let url = self.url(&format!(
            "api/v1/accounts/{}/contacts/{contact_id}",
            self.account_id
        ));
        let resp = self
            .call_with_retry("get_contact", || self.http_client.get(url.clone()))
            .await?;
        self.parse_json(resp).await
    }

    // ---- C13: Set priority ----
    pub async fn set_priority(
        &self,
        conv_id: i64,
        priority: &str,
    ) -> Result<ChatwootResponse, ChatwootError> {
        let url = self.conv_path(conv_id, "toggle_priority");
        let body = serde_json::json!({ "priority": priority });
        let resp = self
            .call_with_retry("set_priority", || {
                self.http_client.post(url.clone()).json(&body)
            })
            .await?;
        self.parse_json(resp).await
    }

    // ================================================================
    // 4.7 — Idempotência
    // ================================================================

    /// Gera `idempotency_key = "{run_id}:{operation}"` (Spec 4.7).
    /// ponytail: a chave é gerada uma vez e capturada na closure de
    /// `call_with_retry` — estável across as 3 tentativas. A dedup durável
    /// cross-call é a linha em `outbound_message` (Spec 4.7); o header HTTP
    /// é guarda secundária. Threadar o `run_id` do worker quando ele for
    /// ligado (substituir `RunId::new()` por um `run_id` parâmetro).
    fn idempotency_key(&self, op: &str) -> String {
        format!("{}:{op}", RunId::new())
    }

    // ================================================================
    // 4.7 — Retry + Circuit breaker
    // ================================================================

    /// Wrapper de todas as chamadas C1–C13. Timeout de 10s (set no builder),
    /// 3 tentativas com backoff 1s/3s/9s ±20% jitter, retry só em 5xx/408/429,
    /// circuit breaker de 5 falhas → 60s open → half-open com 1 tentativa.
    // ponytail: função interna, sem trait — Spec 4.7 e o ticket pedem isso.
    async fn call_with_retry<F>(
        &self,
        op: &'static str,
        build: F,
    ) -> Result<reqwest::Response, ChatwootError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        const MAX_ATTEMPTS: u32 = 3;
        // Spec 4.7: 1s, 3s, 9s. Com 3 tentativas usamos os 2 primeiros intervalos;
        // o 9s fica reservado para se MAX_ATTEMPTS subir a 4.
        const BACKOFF_MS: [u64; 3] = [1000, 3000, 9000];

        let mut last_err: Option<ChatwootError> = None;

        for attempt in 0..MAX_ATTEMPTS {
            // Circuit breaker gate — recusa imediato se open.
            self.check_circuit(op)?;

            let resp = match build().send().await {
                Ok(r) => r,
                Err(e) => {
                    // Erro de transporte: timeout conta como Timeout, resto como Network.
                    // Ambos são retryable e contam para o circuito.
                    let err = if e.is_timeout() {
                        ChatwootError::Timeout
                    } else {
                        ChatwootError::Network(e)
                    };
                    tracing::warn!(op, attempt, error = %err, "chatwoot call transport error");
                    self.on_failure();
                    last_err = Some(err);
                    if attempt + 1 < MAX_ATTEMPTS {
                        Self::sleep_jitter(BACKOFF_MS[attempt as usize]).await;
                        continue;
                    }
                    break;
                }
            };

            let status = resp.status();
            let code = status.as_u16();

            if status.is_success() {
                self.on_success();
                return Ok(resp);
            }

            // Resposta de erro — lê corpo para diagnóstico.
            let body = resp.text().await.unwrap_or_default();
            let err = match code {
                401 | 403 => ChatwootError::AuthError,
                404 => ChatwootError::NotFound,
                408 => ChatwootError::Timeout,
                429 => ChatwootError::RateLimited,
                _ => ChatwootError::ApiError { status: code, body },
            };

            let retryable = matches!(code, 408 | 429) || code >= 500;

            if retryable {
                tracing::warn!(op, attempt, code, "chatwoot retryable error");
                self.on_failure();
                last_err = Some(err);
                if attempt + 1 < MAX_ATTEMPTS {
                    Self::sleep_jitter(BACKOFF_MS[attempt as usize]).await;
                    continue;
                }
                break;
            }

            // 4xx (exceto 408/429): loga e retorna — sem retry, sem contar
            // para o circuito (erro de cliente, não de serviço).
            tracing::warn!(op, code, "chatwoot api client error (no retry)");
            return Err(err);
        }

        Err(last_err.unwrap_or(ChatwootError::CircuitOpen))
    }

    /// Backoff com jitter ±20% (Spec 4.7).
    // ponytail: jitter derivado do relógio — não cripto, mas suficiente para
    // dessincronizar retries concorrentes sem adicionar o crate `rand`.
    async fn sleep_jitter(base_ms: u64) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let factor = 0.8 + (nanos as f64 / 1_000_000_000.0) * 0.4; // 0.8 ..= 1.2
        let ms = (base_ms as f64 * factor) as u64;
        sleep(Duration::from_millis(ms)).await;
    }

    fn check_circuit(&self, op: &'static str) -> Result<(), ChatwootError> {
        let mut st = self.circuit_state.lock().expect("circuit mutex poisoned");
        let now = Instant::now();
        match &mut *st {
            CircuitState::Open { until } if *until > now => {
                tracing::warn!(op, "chatwoot circuit open — call rejected");
                Err(ChatwootError::CircuitOpen)
            }
            CircuitState::Open { .. } => {
                // Janela expirou: libera uma tentativa (half-open).
                tracing::info!(op, "chatwoot circuit half-open probe");
                *st = CircuitState::HalfOpen;
                Ok(())
            }
            CircuitState::HalfOpen | CircuitState::Closed { .. } => Ok(()),
        }
    }

    fn on_success(&self) {
        let mut st = self.circuit_state.lock().expect("circuit mutex poisoned");
        if !matches!(*st, CircuitState::Closed { failures: 0 }) {
            tracing::info!("chatwoot circuit closed (recovered)");
        }
        *st = CircuitState::Closed { failures: 0 };
    }

    fn on_failure(&self) {
        let mut st = self.circuit_state.lock().expect("circuit mutex poisoned");
        let now = Instant::now();
        let next = match &*st {
            CircuitState::Closed { failures } => {
                let f = *failures + 1;
                if f >= FAILURE_THRESHOLD {
                    tracing::error!(failures = f, "chatwoot circuit opening");
                    CircuitState::Open {
                        until: now + Duration::from_secs(OPEN_DURATION_SECS),
                    }
                } else {
                    CircuitState::Closed { failures: f }
                }
            }
            CircuitState::HalfOpen => {
                tracing::error!("chatwoot circuit re-opening (half-open probe failed)");
                CircuitState::Open {
                    until: now + Duration::from_secs(OPEN_DURATION_SECS),
                }
            }
            CircuitState::Open { .. } => return, // já aberto, mantém o until
        };
        *st = next;
    }

    async fn parse_json(&self, resp: reqwest::Response) -> Result<serde_json::Value, ChatwootError> {
        let bytes = resp.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

// ====================================================================
// Self-check (ponytail: um teste mínimo que falha se a lógica quebrar)
// ====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hmac_verify_roundtrip() {
        let secret = b"whsec_top";
        let ts = "1722168000";
        let body = br#"{"event":"message_created"}"#;

        // assinatura correta
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(ts.as_bytes());
        mac.update(b".");
        mac.update(body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verify_webhook_signature(secret, ts, body, &sig));
        // corpo alterado → inválida
        assert!(!verify_webhook_signature(secret, ts, b"{}", &sig));
        // secret errado → inválida
        assert!(!verify_webhook_signature(b"other", ts, body, &sig));
        // header malformado → inválida
        assert!(!verify_webhook_signature(secret, ts, body, "sha256=deadbeef"));
    }

    #[test]
    fn hmac_verify_any_accepts_any_secret() {
        let secrets: Vec<&[u8]> = vec![b"old", b"new"];
        let ts = "1722168000";
        let body = b"body";
        let mut mac = Hmac::<Sha256>::new_from_slice(b"new").unwrap();
        mac.update(ts.as_bytes());
        mac.update(b".");
        mac.update(body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        assert!(verify_webhook_signature_any(&secrets, ts, body, &sig));
    }

    #[test]
    fn timestamp_freshness() {
        let now = 1_000_000;
        assert!(is_timestamp_fresh("1000000", now, 300));
        assert!(is_timestamp_fresh("999800", now, 300));
        assert!(!is_timestamp_fresh("999000", now, 300));
        assert!(!is_timestamp_fresh("not-a-number", now, 300));
    }

    #[test]
    fn sender_kind_classification() {
        // contato falando → processar
        let p = AgentBotWebhookPayload {
            event: "message_created".into(),
            message_type: Some("incoming".into()),
            private: Some(false),
            sender: Some(SenderInfo {
                id: 88,
                name: Some("João".into()),
                r#type: Some("contact".into()),
            }),
            ..Default::default()
        };
        assert_eq!(p.sender_kind(), SenderKind::Contact);
        assert!(p.sender_kind().is_contact());
        assert!(!p.is_private_note());

        // humano respondeu → pausa IA
        let p = AgentBotWebhookPayload {
            message_type: Some("outgoing".into()),
            sender: Some(SenderInfo {
                id: 12,
                r#type: Some("User".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(p.sender_kind(), SenderKind::User);

        // eco da própria IA → descartar
        let p = AgentBotWebhookPayload {
            message_type: Some("outgoing".into()),
            sender: Some(SenderInfo {
                id: 7,
                r#type: Some("AgentBot".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(p.sender_kind(), SenderKind::AgentBot);
        assert!(p.sender_kind().is_agent_bot());

        // atividade/template → sistema
        let p = AgentBotWebhookPayload {
            message_type: Some("activity".into()),
            ..Default::default()
        };
        assert_eq!(p.sender_kind(), SenderKind::System);

        // nota interna → descartar sempre
        let p = AgentBotWebhookPayload {
            message_type: Some("outgoing".into()),
            private: Some(true),
            sender: Some(SenderInfo {
                id: 12,
                r#type: Some("User".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(p.is_private_note());
    }

    #[test]
    fn payload_tolerates_extra_and_missing_fields() {
        let raw = json!({
            "event": "message_created",
            "id": 9021,
            "content": "ola",
            "message_type": "incoming",
            "extra_field_unknown": 123,
            "conversation": {
                "id": 523,
                "status": "pending",
                "future_attr": true
            },
            "sender": { "id": 88, "type": "contact", "name": "João" }
        });
        let p: AgentBotWebhookPayload = serde_json::from_value(raw).unwrap();
        assert_eq!(p.id, Some(9021));
        assert_eq!(p.conversation.as_ref().unwrap().id, 523);
        assert_eq!(p.sender_kind(), SenderKind::Contact);
    }

    #[test]
    fn client_constructs_base_url() {
        let c = ChatwootClient::new("https://chat.example.com", "tok", 1);
        assert_eq!(c.account_id(), 1);
        assert_eq!(c.base_url().as_str(), "https://chat.example.com/");
        // com trailing slash é normalizado igual
        let c2 = ChatwootClient::new("https://chat.example.com/", "tok", 2);
        assert_eq!(c2.base_url().as_str(), "https://chat.example.com/");
    }

    #[test]
    fn circuit_opens_after_threshold_failures() {
        let c = ChatwootClient::new("https://chat.example.com", "tok", 1);
        // 4 falhas: ainda closed
        for _ in 0..4 {
            c.on_failure();
        }
        match &*c.circuit_state.lock().unwrap() {
            CircuitState::Closed { failures } => assert_eq!(*failures, 4),
            other => panic!("esperado Closed, foi {other:?}"),
        }
        // 5ª: abre
        c.on_failure();
        match &*c.circuit_state.lock().unwrap() {
            CircuitState::Open { .. } => {}
            other => panic!("esperado Open, foi {other:?}"),
        }
        // circuito aberto → check rejeita
        assert!(matches!(c.check_circuit("x"), Err(ChatwootError::CircuitOpen)));
        // sucesso fecha de volta
        c.on_success();
        assert!(matches!(
            &*c.circuit_state.lock().unwrap(),
            CircuitState::Closed { failures: 0 }
        ));
    }

    #[test]
    fn idempotency_key_format() {
        let c = ChatwootClient::new("https://chat.example.com", "tok", 1);
        let k = c.idempotency_key("send_message");
        assert!(k.ends_with(":send_message"));
        assert!(k.split(':').count() >= 2);
    }
}
