//! bridge-api — servidor HTTP que recebe webhooks do Chatwoot.
//!
//! Spec normativa: `ESPECchatwootaibridge.md` Seções 3, 4, 6, 9.2, 10.1, 15.1.
//!
//! Responsabilidades (e só estas):
//! 1. Verificar HMAC do webhook (Seção 4.4) — corpo bruto ANTES de JSON.
//! 2. Deduplicar eventos (Seção 6.4).
//! 3. Discriminar remetente (Seção 4.5) — só `incoming` de `contact` dispara.
//! 4. Persistir metadados em `message_log` (Seção 13).
//! 5. Alimentar buffer/debounce no Redis (Seção 6.2) e enfileirar jobs.
//! 6. Webhook de conta: manter `conversation_state` e armar SLA (Seções 7, 11).
//!
//! Regra de ouro (Seção 2.3): **NÃO** chama a IA nem a Application API do
//! Chatwoot. Só valida, persiste e enfileira. SLO: 200 em < 50 ms p95 (9.2).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use bridge_chatwoot::{
    is_timestamp_fresh, verify_webhook_signature, AgentBotWebhookPayload, ChatwootClient,
    ConversationSummary, SenderKind,
};
use bridge_core::{
    buffer_key, dedup_key, AiState, Config, ConversationId, SecretString, StateEvent,
    DEBOUNCE_ZSET, QUEUE_AGENT_RUNS,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use redis::AsyncCommands;
use serde::Serialize;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tracing_subscriber::EnvFilter;

// ---- Constantes de controle (Seção 6.4 / 9.2 / 4.4) ----

/// Janela máxima de replay do timestamp do webhook (Seção 4.4 regra 3).
const WEBHOOK_TIMESTAMP_TOLERANCE_SECS: i64 = 300;
/// TTL do dedup de mensagem (Seção 6.4): 86400s = 24h.
const DEDUP_TTL_SECS: u64 = 86_400;
/// TTL das keys de buffer (Seção 6.2): 300s.
const BUFFER_TTL_SECS: u64 = 300;

// ====================================================================
// AppState compartilhado (Seção 3 / item 6)
// ====================================================================

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub redis_pool: deadpool_redis::Pool,
    pub chatwoot: Arc<ChatwootClient>,
    pub config: Arc<Config>,
    pub startup: Arc<std::time::Instant>,
}

// ====================================================================
// Métricas (Seção 15.1)
// ====================================================================

fn describe_metrics() {
    metrics::describe_counter!(
        "bridge_webhook_received_total",
        "Total de webhooks recebidos por evento e resultado"
    );
    metrics::describe_counter!(
        "bridge_webhook_signature_failures_total",
        "Falhas de verificação de assinatura HMAC"
    );
    metrics::describe_counter!(
        "bridge_buffer_flush_total",
        "Disparos de buffer por motivo"
    );
    metrics::describe_histogram!(
        "bridge_buffer_messages_per_turn",
        "Mensagens agrupadas por turno (eficácia do buffer)"
    );
}

fn install_metrics_recorder() {
    // ponytail: instala o recorder Prometheus para que `increment_counter!`
    // não seja no-op. O endpoint /metrics vem depois (Seção 10.1 exige
    // escutar só em 127.0.0.1 — fora do escopo dos 3 endpoints públicos).
    let _ = PrometheusBuilder::new().install_recorder();
}

// ====================================================================
// Healthz (item 2)
// ====================================================================

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    let uptime_secs = state.startup.elapsed().as_secs();
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": uptime_secs,
    }))
}

// ====================================================================
// Webhook Agent Bot (Seção 4.3, 4.4, 4.5, 6.2, 6.4)
// ====================================================================

async fn webhook_agent_bot(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, String)> {
    // (a) Extrair headers de assinatura.
    let sig = header_str(&headers, "x-chatwoot-signature");
    let ts = header_str(&headers, "x-chatwoot-timestamp");

    // (b)+(c) Verificar HMAC e timestamp. Corpo bruto — NÃO desserializar antes.
    verify_request(&state.config.chatwoot.webhook_secrets, sig, ts, &body)
        .map_err(|e| {
            metrics::increment_counter!("bridge_webhook_signature_failures_total");
            (StatusCode::UNAUTHORIZED, e)
        })?;

    // (d) Parsear APÓS verificação.
    let payload: AgentBotWebhookPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            // JSON inválido: não há o que processar, mas 200 para não enforcar o
            // Chatwoot em retries de um payload que não vai melhorar.
            tracing::warn!(error = %e, "agent-bot webhook: invalid JSON");
            return Ok(Json(json!({"ok": true, "ignored": "invalid_json"})));
        }
    };

    process_agent_bot_payload(&state, &payload).await
}

/// Processamento real do payload do Agent Bot, já autenticado.
async fn process_agent_bot_payload(
    state: &AppState,
    payload: &AgentBotWebhookPayload,
) -> Result<Json<Value>, (StatusCode, String)> {
    let event = payload.event.as_str();
    metrics::increment_counter!(
        "bridge_webhook_received_total",
        "event" => event.to_string(),
        "result" => "received"
    );

    // (f) Apenas `message_created` é de interesse no webhook do bot (Seção 4.3).
    if event != "message_created" {
        metrics::increment_counter!(
            "bridge_webhook_received_total",
            "event" => event.to_string(),
            "result" => "ignored"
        );
        return Ok(Json(json!({"ok": true, "ignored": event})));
    }

    // Campos obrigatórios (Seção 4.5): id, conversation.id, account.id.
    let msg_id = match payload.id {
        Some(id) => id,
        None => {
            tracing::warn!("message_created sem id — descartando");
            return Ok(Json(json!({"ok": true, "ignored": "no_id"})));
        }
    };
    let conv = match payload.conversation.as_ref() {
        Some(c) => c,
        None => {
            tracing::warn!(msg_id, "message_created sem conversation — descartando");
            return Ok(Json(json!({"ok": true, "ignored": "no_conversation"})));
        }
    };
    let conv_id: ConversationId = conv.id;
    let account_id = payload.account.as_ref().map(|a| a.id).unwrap_or(0);

    // Discriminação do remetente (Seção 4.5 — a mais importante).
    // Nota interna, eco da IA, evento de sistema → descartar sempre.
    if payload.is_private_note() {
        metrics::increment_counter!(
            "bridge_webhook_received_total",
            "event" => event.to_string(),
            "result" => "discarded"
        );
        return Ok(Json(json!({"ok": true, "discarded": "private"})));
    }
    match payload.sender_kind() {
        SenderKind::Contact => { /* processa abaixo */ }
        SenderKind::AgentBot | SenderKind::User | SenderKind::System => {
            // AgentBot/User/System chegam no webhook do bot; só Contact
            // alimenta o buffer. Outros são descartados silenciosamente.
            metrics::increment_counter!(
                "bridge_webhook_received_total",
                "event" => event.to_string(),
                "result" => "skipped"
            );
            return Ok(Json(json!({"ok": true, "skipped": true})));
        }
    }

    let mut conn = state
        .redis_pool
        .get()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // (e) Dedup (Seção 6.4): SET NX EX. Se já existir → duplicado, 200.
    let dedup_key = dedup_key(account_id, msg_id);
    let was_set: Option<String> = redis::cmd("SET")
        .arg(&dedup_key)
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(DEDUP_TTL_SECS)
        .query_async(&mut *conn)
        .await
        .map_err(redis_err)?;
    if was_set.is_none() {
        tracing::info!(account_id, msg_id, "webhook duplicado ignorado");
        metrics::increment_counter!(
            "bridge_webhook_received_total",
            "event" => event.to_string(),
            "result" => "duplicate"
        );
        return Ok(Json(json!({"ok": true, "duplicate": true})));
    }

    // (g) Persistir em message_log (Seção 13). Conteúdo cifrado fica NULL por
    // enquanto — pgcrypto exige chave de infra; wired quando a chave existir.
    let content = payload.content.clone().unwrap_or_default();
    let content_len = content.len() as i32;
    // ponytail: o payload do bot (shape mínimo 4.5) não expõe `attachments`;
    // has_attachment fica sempre false até o campo entrar no payload.
    let has_attachment = false;
    let created_at = payload
        .created_at
        .as_deref()
        .and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok())
        .unwrap_or_else(OffsetDateTime::now_utc);
    let _log_id: i64 = sqlx::query_scalar(
        "INSERT INTO message_log
            (chatwoot_msg_id, conversation_id, direction, sender_kind, is_private,
             content_enc, content_len, has_attachment, created_at, ingested_at)
         VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8, now())
         RETURNING id",
    )
    .bind(msg_id)
    .bind(conv_id)
    .bind("inbound")
    .bind("contact")
    .bind(false)
    .bind(content_len)
    .bind(has_attachment)
    .bind(created_at)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // (h) Buffer/debounce (Seção 6.2).
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let now_ms = now.as_millis() as i64;

    let buffered = BufferedMessage {
        id: msg_id,
        account_id,
        conversation_id: conv_id,
        content: content.clone(),
        created_at: payload.created_at.clone().unwrap_or_default(),
    };
    let serialized = serde_json::to_string(&buffered).unwrap_or_default();

    let buf_cfg = state.config.buffer;
    let key_buf = buffer_key(conv_id);
    let key_first = format!("buf:first:{conv_id}");
    let key_chars = format!("buf:chars:{conv_id}");

    let r = &mut *conn;
    // RPUSH + EXPIRE.
    let _: i64 = r.rpush(&key_buf, &serialized).await.map_err(redis_err)?;
    let _: bool = r
        .expire(&key_buf, BUFFER_TTL_SECS as i64)
        .await
        .map_err(redis_err)?;
    // SETNX marca o início da janela; EXPIRE.
    let _: i64 = redis::cmd("SETNX")
        .arg(&key_first)
        .arg(now_ms)
        .query_async(r)
        .await
        .map_err(redis_err)?;
    let _: bool = r
        .expire(&key_first, BUFFER_TTL_SECS as i64)
        .await
        .map_err(redis_err)?;

    let n: i64 = r.llen(&key_buf).await.map_err(redis_err)?;
    let chars: i64 = redis::cmd("INCRBY")
        .arg(&key_chars)
        .arg(content_len as i64)
        .query_async(r)
        .await
        .map_err(redis_err)?;
    let _: bool = r
        .expire(&key_chars, BUFFER_TTL_SECS as i64)
        .await
        .map_err(redis_err)?;
    let first_ms: i64 = r
        .get::<_, Option<String>>(&key_first)
        .await
        .map_err(redis_err)?
        .and_then(|s| s.parse().ok())
        .unwrap_or(now_ms);

    // Gatilhos de disparo imediato (Seção 6.2).
    let trigger = if n as u32 >= buf_cfg.max_messages {
        Some("max_messages")
    } else if chars as u32 >= buf_cfg.max_chars {
        Some("max_chars")
    } else if now_ms - first_ms >= buf_cfg.max_wait_ms as i64 {
        Some("max_wait")
    } else {
        None
    };

    if let Some(reason) = trigger {
        metrics::histogram!("bridge_buffer_messages_per_turn", n as f64);
        enqueue_agent_run(&mut conn, conv_id, account_id, reason).await?;
    } else {
        // Janela deslizante: cada nova mensagem REAGENDA o debounce.
        // ponytail: o payload do bot não expõe anexo (shape 4.5), então a
        // janela de mídia (BUFFER_MEDIA_DEBOUNCE_MS) fica reservada — ativar
        // quando o campo attachments entrar no payload.
        let janela = buf_cfg.debounce_ms as i64;
        let remaining_until_cap = buf_cfg.max_wait_ms as i64 - (now_ms - first_ms);
        let delay = janela.min(remaining_until_cap).max(0);
        let score = now_ms + delay;
        let _: i64 = r
            .zadd(DEBOUNCE_ZSET, conv_id.to_string(), score)
            .await
            .map_err(redis_err)?;
    }

    metrics::increment_counter!(
        "bridge_webhook_received_total",
        "event" => event.to_string(),
        "result" => "processed"
    );
    // (i) Sempre 200 — Chatwoot não deve esperar.
    Ok(Json(json!({"ok": true})))
}

// ====================================================================
// Webhook de Conta (Seção 4.3, 7, 11)
// ====================================================================

async fn webhook_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, String)> {
    let sig = header_str(&headers, "x-chatwoot-signature");
    let ts = header_str(&headers, "x-chatwoot-timestamp");

    verify_request(&state.config.chatwoot.webhook_secrets, sig, ts, &body).map_err(|e| {
        metrics::increment_counter!("bridge_webhook_signature_failures_total");
        (StatusCode::UNAUTHORIZED, e)
    })?;

    let payload: AgentBotWebhookPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "account webhook: invalid JSON");
            return Ok(Json(json!({"ok": true, "ignored": "invalid_json"})));
        }
    };

    process_account_payload(&state, &payload).await
}

async fn process_account_payload(
    state: &AppState,
    payload: &AgentBotWebhookPayload,
) -> Result<Json<Value>, (StatusCode, String)> {
    let event = payload.event.as_str();
    metrics::increment_counter!(
        "bridge_webhook_received_total",
        "event" => event.to_string(),
        "result" => "received"
    );

    let conv = match payload.conversation.as_ref() {
        Some(c) => c,
        None => return Ok(Json(json!({"ok": true, "ignored": "no_conversation"}))),
    };
    let conv_id: ConversationId = conv.id;
    let account_id = payload.account.as_ref().map(|a| a.id).unwrap_or(0);

    // Dedup de eventos de conversa (Seção 6.4):
    // `seen:conv:{conv_id}:{event}:{created_at}`.
    let mut conn = state
        .redis_pool
        .get()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let dedup_ts = payload.created_at.clone().unwrap_or_default();
    let conv_dedup_key = format!("seen:conv:{conv_id}:{event}:{dedup_ts}");
    let was_set: Option<String> = redis::cmd("SET")
        .arg(&conv_dedup_key)
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(DEDUP_TTL_SECS)
        .query_async(&mut *conn)
        .await
        .map_err(redis_err)?;
    if was_set.is_none() {
        tracing::info!(conv_id, event, "account webhook duplicado ignorado");
        return Ok(Json(json!({"ok": true, "duplicate": true})));
    }

    match event {
        "conversation_created" => {
            handle_conversation_created(state, conv, account_id).await?;
        }
        "conversation_updated" => {
            handle_conversation_updated(state, conv).await?;
        }
        "conversation_status_changed" => {
            handle_status_changed(state, conv_id, conv.status.as_deref()).await?;
        }
        "message_created" => {
            // No webhook de conta: detectar outgoing de humano (Seção 4.5).
            if matches!(payload.sender_kind(), SenderKind::User) {
                handle_human_message(state, conv_id).await?;
            }
        }
        _ => {
            // typing_on/off, webwidget_triggered, message_updated → ignorar.
        }
    }

    metrics::increment_counter!(
        "bridge_webhook_received_total",
        "event" => event.to_string(),
        "result" => "processed"
    );
    Ok(Json(json!({"ok": true})))
}

/// `conversation_created`: cria linha em `conversation_state` + arma SLA
/// de primeira resposta (Seção 11.1).
async fn handle_conversation_created(
    state: &AppState,
    conv: &ConversationSummary,
    account_id: i64,
) -> Result<(), (StatusCode, String)> {
    let inbox_id = conv.inbox_id.unwrap_or(0);
    let contact_id = conv
        .meta
        .as_ref()
        .and_then(|m| m.sender.as_ref())
        .map(|s| s.id)
        .unwrap_or(0);
    let channel = conv.channel.clone().unwrap_or_default();
    let chatwoot_status = conv.status.clone().unwrap_or_else(|| "pending".into());
    // Conversas de inbox com bot nascem em `pending` → IA ativa (Seção 7.3).
    let ai_state = if chatwoot_status == "pending" {
        AiState::AiActive.as_str()
    } else {
        AiState::AwaitingHuman.as_str()
    };

    sqlx::query(
        "INSERT INTO conversation_state
            (conversation_id, account_id, inbox_id, contact_id, channel,
             ai_state, chatwoot_status, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
         ON CONFLICT (conversation_id) DO UPDATE
            SET chatwoot_status = EXCLUDED.chatwoot_status,
                ai_state = EXCLUDED.ai_state,
                updated_at = now()",
    )
    .bind(conv.id)
    .bind(account_id)
    .bind(inbox_id)
    .bind(contact_id)
    .bind(&channel)
    .bind(ai_state)
    .bind(&chatwoot_status)
    .execute(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // SLA de primeira resposta (Seção 11.1). due_at inicial = now + 3 min;
    // o scheduler ajusta o relógio pelo horário comercial (Seção 11.2).
    // ponytail: intervalo fixo de 3 min (nível 0); configurável via scheduler.
    sqlx::query(
        "INSERT INTO sla_timer (conversation_id, kind, due_at, status, created_at)
         VALUES ($1, 'first_response', now() + interval '3 minutes', 'armed', now())
         ON CONFLICT (conversation_id, kind) DO NOTHING",
    )
    .bind(conv.id)
    .execute(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

/// `conversation_updated`: detecta assignee/team/labels trocados (Seção 4.3).
async fn handle_conversation_updated(
    state: &AppState,
    conv: &ConversationSummary,
) -> Result<(), (StatusCode, String)> {
    let assignee_id = conv
        .meta
        .as_ref()
        .and_then(|m| m.assignee.as_ref())
        .map(|a| a.id);
    let team_id = conv
        .meta
        .as_ref()
        .and_then(|m| m.team.as_ref())
        .map(|t| t.id);
    let labels = conv.labels.clone().unwrap_or_default();

    // Se um atendente foi atribuído, a IA cede lugar (Seção 7.2: HumanAssigned).
    let ai_state = if assignee_id.is_some() {
        Some(AiState::HumanHandling.as_str())
    } else {
        None
    };

    sqlx::query(
        "INSERT INTO conversation_state (conversation_id, updated_at)
         VALUES ($1, now())
         ON CONFLICT (conversation_id) DO UPDATE
            SET assignee_id = COALESCE($2, conversation_state.assignee_id),
                team_id     = COALESCE($3, conversation_state.team_id),
                labels      = $4,
                ai_state    = COALESCE($5, conversation_state.ai_state),
                updated_at  = now()",
    )
    .bind(conv.id)
    .bind(assignee_id)
    .bind(team_id)
    .bind(&labels)
    .bind(ai_state)
    .execute(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

/// `conversation_status_changed`: transição da máquina de estados (Seção 7).
async fn handle_status_changed(
    state: &AppState,
    conv_id: ConversationId,
    new_status: Option<&str>,
) -> Result<(), (StatusCode, String)> {
    let Some(status) = new_status else {
        return Ok(());
    };

    // Mapeia o status do Chatwoot para um StateEvent (Seção 7.2/7.3).
    // ponytail: carrega o estado atual e aplica a transição formal; em caso
    // de transição inválida, loga e persiste o mapeamento direto (fallback).
    let event = match status {
        "pending" => StateEvent::HumanSetPending,
        "open" => StateEvent::AiRequestedHandoff,
        "resolved" => StateEvent::HumanResolved,
        "snoozed" => StateEvent::LabelAdded("snoozed".into()),
        _ => return Ok(()),
    };

    let row: Option<(String,)> =
        sqlx::query_as("SELECT ai_state FROM conversation_state WHERE conversation_id = $1")
            .bind(conv_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let next = match row {
        Some((cur,)) => match parse_ai_state(&cur) {
            Some(s) => match s.transition(&event) {
                Ok(next) => next,
                Err(err) => {
                    tracing::warn!(
                        conv_id, %cur, %event, %err,
                        "transição inválida — fallback direto"
                    );
                    status_to_ai_state(status)
                }
            }
            None => status_to_ai_state(status),
        },
        None => status_to_ai_state(status),
    };

    sqlx::query(
        "INSERT INTO conversation_state (conversation_id, ai_state, chatwoot_status, updated_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (conversation_id) DO UPDATE
            SET ai_state = EXCLUDED.ai_state,
                chatwoot_status = EXCLUDED.chatwoot_status,
                updated_at = now()",
    )
    .bind(conv_id)
    .bind(next.as_str())
    .bind(status)
    .execute(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

/// `message_created` outgoing de humano: pausa a IA + cancela SLA (Seção 4.5).
async fn handle_human_message(
    state: &AppState,
    conv_id: ConversationId,
) -> Result<(), (StatusCode, String)> {
    sqlx::query(
        "INSERT INTO conversation_state (conversation_id, ai_state, last_human_msg_at, updated_at)
         VALUES ($1, 'human_handling', now(), now())
         ON CONFLICT (conversation_id) DO UPDATE
            SET ai_state = 'human_handling',
                last_human_msg_at = now(),
                updated_at = now()",
    )
    .bind(conv_id)
    .execute(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Cancela timers de SLA armados (humano respondeu).
    // ponytail: cancela todos os armados; o scheduler rearma se reabrir.
    sqlx::query(
        "UPDATE sla_timer SET status = 'cancelled', cancelled_reason = 'human_replied'
         WHERE conversation_id = $1 AND status = 'armed'",
    )
    .bind(conv_id)
    .execute(&state.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

// ====================================================================
// Buffer/disparo (item 5)
// ====================================================================

/// Enfileira um job de run do agente e remove a conversa do debounce ZSET.
///
/// ponytail: segue a Seção 6.2 (`disparar` faz `LPUSH queue:agent_runs
/// {conv_id, motivo, trace_id}` + `ZREM debounce:zset conv_id`). O snippet do
/// ticket (DEL + LRANGE + LPUSH) tem bug de ordem (DEL antes de LRANGE apaga
/// as mensagens) e conflita com a Seção 6.2 — o buffer é consumido pelo
/// worker sob lock (Seção 6.3), não aqui. Mantemos o buffer intacto.
async fn enqueue_agent_run(
    conn: &mut deadpool_redis::Connection,
    conv_id: ConversationId,
    account_id: i64,
    reason: &str,
) -> Result<(), (StatusCode, String)> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let now_ms = now.as_millis() as i64;

    let job = AgentRunJob {
        conversation_id: conv_id,
        account_id,
        reason: reason.to_string(),
        enqueued_at_ms: now_ms,
    };
    let payload = serde_json::to_string(&job).unwrap_or_default();

    let r = &mut *conn;
    let _: i64 = r.lpush(QUEUE_AGENT_RUNS, &payload).await.map_err(redis_err)?;
    let _: i64 = r
        .zrem(DEBOUNCE_ZSET, conv_id.to_string())
        .await
        .map_err(redis_err)?;

    metrics::increment_counter!("bridge_buffer_flush_total", "reason" => reason.to_string());
    tracing::info!(conv_id, account_id, reason, "agent run enqueued");
    Ok(())
}

#[derive(Serialize)]
struct AgentRunJob {
    conversation_id: ConversationId,
    account_id: i64,
    reason: String,
    enqueued_at_ms: i64,
}

#[derive(Serialize)]
struct BufferedMessage {
    id: i64,
    account_id: i64,
    conversation_id: ConversationId,
    content: String,
    created_at: String,
}

// ====================================================================
// Helpers de HMAC / timestamp / headers
// ====================================================================

/// Retorna o valor de um header como `&str`, ou string vazia se ausente.
fn header_str(headers: &HeaderMap, name: &str) -> &str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// Verifica assinatura HMAC + timestamp. Erro → 401 (Seção 4.4).
fn verify_request(
    secrets: &[SecretString],
    sig: &str,
    ts: &str,
    body: &[u8],
) -> Result<(), String> {
    if sig.is_empty() || ts.is_empty() {
        return Err("missing signature or timestamp".into());
    }
    // (c) Proteção contra replay: |agora - timestamp| > 300s → 401.
    if !is_timestamp_fresh(ts, now_secs(), WEBHOOK_TIMESTAMP_TOLERANCE_SECS) {
        return Err("timestamp out of tolerance".into());
    }
    // (b) Tentar cada secret em WEBHOOK_SECRETS (rotação, Seção 4.4 regra 5).
    let ok = secrets
        .iter()
        .any(|s| verify_webhook_signature(s.expose().as_bytes(), ts, body, sig));
    if !ok {
        return Err("invalid signature".into());
    }
    Ok(())
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Faz parse de `AiState` a partir do valor persistido em BD (snake_case).
/// ponytail: bridge-core não expõe `FromStr` para `AiState`; helper local
/// espelha `AiState::as_str`. Mover para bridge-core quando reusado.
fn parse_ai_state(s: &str) -> Option<AiState> {
    match s.trim() {
        "ai_active" => Some(AiState::AiActive),
        "ai_thinking" => Some(AiState::AiThinking),
        "awaiting_human" => Some(AiState::AwaitingHuman),
        "human_handling" => Some(AiState::HumanHandling),
        "ai_paused_manual" => Some(AiState::AiPausedManual),
        "ai_paused_limit" => Some(AiState::AiPausedLimit),
        "closed" => Some(AiState::Closed),
        _ => None,
    }
}

/// Mapeia o status do Chatwoot para `AiState` (fallback quando a máquina
/// formal rejeita a transição).
fn status_to_ai_state(status: &str) -> AiState {
    match status {
        "pending" => AiState::AiActive,
        "open" => AiState::AwaitingHuman,
        "resolved" => AiState::Closed,
        "snoozed" => AiState::AiPausedManual,
        _ => AiState::AiActive,
    }
}

/// Converte `redis::RedisError` em 500.
fn redis_err(e: redis::RedisError) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

// ====================================================================
// main (item 1)
// ====================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Config (bridge_core::Config::load — equivalente a from_env).
    let config = Arc::new(Config::load()?);

    // 2. Tracing/logging. ponytail: filtro por env; redação de PII custom
    // (Seção 10.4) exige layer dedicada — entra com a Fase 0 completa.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.infra.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Métricas (Seção 15.1).
    install_metrics_recorder();
    describe_metrics();

    // 3. PostgreSQL.
    let pool = sqlx::PgPool::connect(config.infra.database_url.expose()).await?;

    // 4. Redis (deadpool-redis).
    let redis_pool = deadpool_redis::Config::from_url(config.infra.redis_url.expose())
        .builder()?
        .build()?;

    // 5. ChatwootClient (token do Agent Bot, Seção 4.2).
    let chatwoot = Arc::new(ChatwootClient::new(
        &config.chatwoot.base_url,
        config.chatwoot.bot_token.expose(),
        config.chatwoot.account_id,
    ));

    let state = AppState {
        pool,
        redis_pool,
        chatwoot,
        config: config.clone(),
        startup: Arc::new(std::time::Instant::now()),
    };

    // 6. Servidor axum na porta 8080 (item 1).
    // 7. Rotas (Seção 10.1).
    let app = Router::new()
        .route("/webhooks/chatwoot/agent-bot", post(webhook_agent_bot))
        .route("/webhooks/chatwoot/account", post(webhook_account))
        .route("/healthz", get(healthz))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("bridge-api listening on 0.0.0.0:8080");
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
