//! `outbound_message` — idempotência e reconciliação de envios (Seção 4.7/11.4/13).
use crate::pg::PgPool;
use crate::Result;
use bridge_core::{ConversationId, RunId};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

/// Estados de uma mensagem de saída.
pub const STATE_PENDING: &str = "pending";
pub const STATE_SENT: &str = "sent";
pub const STATE_FAILED: &str = "failed";
pub const STATE_ABANDONED: &str = "abandoned";

/// Espelha a tabela `outbound_message`.
#[derive(Debug, Clone, FromRow)]
pub struct OutboundMessageRow {
    pub id: i64,
    pub idempotency_key: String,
    pub run_id: Option<Uuid>,
    pub conversation_id: ConversationId,
    pub payload: Value,
    pub state: String,
    pub chatwoot_msg_id: Option<i64>,
    pub attempts: i16,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
}

impl OutboundMessageRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        idempotency_key: impl Into<String>,
        run_id: Option<RunId>,
        conversation_id: ConversationId,
        payload: Value,
    ) -> Self {
        Self {
            id: 0,
            idempotency_key: idempotency_key.into(),
            run_id: run_id.map(|r| r.as_uuid()),
            conversation_id,
            payload,
            state: STATE_PENDING.into(),
            chatwoot_msg_id: None,
            attempts: 0,
            last_error: None,
            created_at: Utc::now(),
            sent_at: None,
        }
    }
}

/// Insere uma mensagem `pending`. Falha em `UNIQUE (idempotency_key)` indica
/// reprocessamento — o caller deve tratar como já-enfileirado.
pub async fn insert(pool: &PgPool, row: &OutboundMessageRow) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO outbound_message
          (idempotency_key, run_id, conversation_id, payload, state,
           chatwoot_msg_id, attempts, last_error, created_at, sent_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id
        "#,
    )
    .bind(&row.idempotency_key)
    .bind(row.run_id)
    .bind(row.conversation_id)
    .bind(&row.payload)
    .bind(&row.state)
    .bind(row.chatwoot_msg_id)
    .bind(row.attempts)
    .bind(&row.last_error)
    .bind(row.created_at)
    .bind(row.sent_at)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Busca por `idempotency_key` — usado pelo reconciliador antes de reenviar.
pub async fn get_by_key(pool: &PgPool, key: &str) -> Result<Option<OutboundMessageRow>> {
    let row = sqlx::query_as::<_, OutboundMessageRow>(
        "SELECT * FROM outbound_message WHERE idempotency_key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Marca como enviada: `state=sent`, `chatwoot_msg_id`, `sent_at=now()`.
pub async fn mark_sent(
    pool: &PgPool,
    id: i64,
    chatwoot_msg_id: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE outbound_message SET state='sent', chatwoot_msg_id=$2, sent_at=now() WHERE id=$1",
    )
    .bind(id)
    .bind(chatwoot_msg_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Registra falha: incrementa tentativas, guarda erro. Se `abandon`, marca
/// como `abandoned` (não será mais reenviada pelo reconciliador).
pub async fn mark_failed(
    pool: &PgPool,
    id: i64,
    error: &str,
    abandon: bool,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE outbound_message
           SET attempts = attempts + 1,
               last_error = $2,
               state = CASE WHEN $3 THEN 'abandoned'::text ELSE state END
           WHERE id = $1"#,
    )
    .bind(id)
    .bind(error)
    .bind(abandon)
    .execute(pool)
    .await?;
    Ok(())
}

/// Lista mensagens `pending` há mais de `older_than` — alvo da reconciliação
/// (Seção 11.4.3): verifica no Chatwoot se existem, reenvia ou marca enviada.
pub async fn list_stale_pending(
    pool: &PgPool,
    older_than: &DateTime<Utc>,
    limit: i64,
) -> Result<Vec<OutboundMessageRow>> {
    let rows = sqlx::query_as::<_, OutboundMessageRow>(
        "SELECT * FROM outbound_message WHERE state='pending' AND created_at < $1 LIMIT $2",
    )
    .bind(older_than)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
