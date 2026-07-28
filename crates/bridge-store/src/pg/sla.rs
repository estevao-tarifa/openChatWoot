//! `sla_timer` + `notification_log` — timers de SLA e escalonamento (Seção 11/13).
use crate::pg::PgPool;
use crate::Result;
use bridge_core::ConversationId;
use chrono::{DateTime, Utc};
use sqlx::FromRow;

pub const KIND_FIRST_RESPONSE: &str = "first_response";
pub const KIND_HUMAN_RESPONSE: &str = "human_response";
pub const KIND_ASSIGNMENT: &str = "assignment";
pub const KIND_RESOLUTION: &str = "resolution";

pub const STATUS_ARMED: &str = "armed";
pub const STATUS_FIRED: &str = "fired";
pub const STATUS_CANCELLED: &str = "cancelled";

/// Espelha a tabela `sla_timer`.
#[derive(Debug, Clone, FromRow)]
pub struct SlaTimerRow {
    pub id: i64,
    pub conversation_id: ConversationId,
    pub kind: String,
    pub due_at: DateTime<Utc>,
    pub escalation_level: i16,
    pub status: String,
    pub cancelled_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl SlaTimerRow {
    pub fn new(
        conversation_id: ConversationId,
        kind: impl Into<String>,
        due_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: 0,
            conversation_id,
            kind: kind.into(),
            due_at,
            escalation_level: 0,
            status: STATUS_ARMED.into(),
            cancelled_reason: None,
            created_at: Utc::now(),
        }
    }
}

/// Upsert de timer. `UNIQUE (conversation_id, kind)` garante um timer por tipo.
pub async fn upsert(pool: &PgPool, row: &SlaTimerRow) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO sla_timer (conversation_id, kind, due_at, escalation_level, status, cancelled_reason)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (conversation_id, kind) DO UPDATE SET
          due_at = EXCLUDED.due_at,
          escalation_level = EXCLUDED.escalation_level,
          status = EXCLUDED.status,
          cancelled_reason = EXCLUDED.cancelled_reason
        RETURNING id
        "#,
    )
    .bind(row.conversation_id)
    .bind(&row.kind)
    .bind(row.due_at)
    .bind(row.escalation_level)
    .bind(&row.status)
    .bind(&row.cancelled_reason)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Cancela um timer (humano respondeu, IA respondeu, etc.).
pub async fn cancel(
    pool: &PgPool,
    conversation_id: ConversationId,
    kind: &str,
    reason: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE sla_timer SET status='cancelled', cancelled_reason=$3 WHERE conversation_id=$1 AND kind=$2",
    )
    .bind(conversation_id)
    .bind(kind)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

/// Timers `armed` vencidos até `now` — alvo do disparo de escalonamento.
pub async fn list_due(pool: &PgPool, now: &DateTime<Utc>, limit: i64) -> Result<Vec<SlaTimerRow>> {
    let rows = sqlx::query_as::<_, SlaTimerRow>(
        "SELECT * FROM sla_timer WHERE status='armed' AND due_at <= $1 LIMIT $2",
    )
    .bind(now)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Avança o nível de escalonamento e rearma `due_at` para o próximo degrau.
pub async fn escalate(
    pool: &PgPool,
    id: i64,
    next_due_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        "UPDATE sla_timer SET escalation_level = escalation_level + 1, due_at = $2 WHERE id = $1",
    )
    .bind(id)
    .bind(next_due_at)
    .execute(pool)
    .await?;
    Ok(())
}

// ---- notification_log (idempotência por nível) ----

/// Espelha a tabela `notification_log`.
#[derive(Debug, Clone, FromRow)]
pub struct NotificationLogRow {
    pub id: i64,
    pub conversation_id: ConversationId,
    pub sla_kind: String,
    pub level: i16,
    pub recipient: String,
    pub channel: String,
    pub state: String,
    pub created_at: DateTime<Utc>,
}

/// Registra notificação. `UNIQUE (conversation_id, sla_kind, level, channel)`
/// garante idempotência: re-disparo do mesmo nível não duplica (Seção 11.2).
/// Retorna `true` se foi inserida (primeira vez), `false` se já existia.
pub async fn log_notification(
    pool: &PgPool,
    conversation_id: ConversationId,
    sla_kind: &str,
    level: i16,
    recipient: &str,
    channel: &str,
    state: &str,
) -> Result<bool> {
    let res = sqlx::query(
        r#"INSERT INTO notification_log (conversation_id, sla_kind, level, recipient, channel, state)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (conversation_id, sla_kind, level, channel) DO NOTHING"#,
    )
    .bind(conversation_id)
    .bind(sla_kind)
    .bind(level)
    .bind(recipient)
    .bind(channel)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}
