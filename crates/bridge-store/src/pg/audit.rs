//! `audit_log` — auditoria append-only, sem conteúdo (Seção 10.4/13).
//!
//! // ponytail: somente INSERT/SELECT. Nenhum UPDATE/DELETE é exposto — a
//! tabela é imutável por contrato (retenção 5 anos). Garantia adicional fica
//! para um trigger `BEFORE UPDATE/DELETE` na migration, não em código.
use crate::pg::PgPool;
use crate::Result;
use bridge_core::ConversationId;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

/// Espelha a tabela `audit_log`.
#[derive(Debug, Clone, FromRow)]
pub struct AuditLogRow {
    pub id: i64,
    pub at: DateTime<Utc>,
    pub actor: String,
    pub action: String,
    pub conversation_id: Option<ConversationId>,
    pub run_id: Option<Uuid>,
    pub payload_hash: Option<String>,
    pub meta: Option<Value>,
}

impl AuditLogRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor: impl Into<String>,
        action: impl Into<String>,
        conversation_id: Option<ConversationId>,
        run_id: Option<Uuid>,
        payload_hash: Option<String>,
        meta: Option<Value>,
    ) -> Self {
        Self {
            id: 0,
            at: Utc::now(),
            actor: actor.into(),
            action: action.into(),
            conversation_id,
            run_id,
            payload_hash,
            meta,
        }
    }
}

/// Append-only: registra evento de auditoria. Retorna o id inserido.
pub async fn append(pool: &PgPool, row: &AuditLogRow) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO audit_log (at, actor, action, conversation_id, run_id, payload_hash, meta)
        VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id
        "#,
    )
    .bind(row.at)
    .bind(&row.actor)
    .bind(&row.action)
    .bind(row.conversation_id)
    .bind(row.run_id)
    .bind(&row.payload_hash)
    .bind(row.meta.as_ref())
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Histórico de auditoria por conversa (mais novo primeiro).
pub async fn list_for_conv(
    pool: &PgPool,
    conv_id: ConversationId,
    limit: i64,
) -> Result<Vec<AuditLogRow>> {
    let rows = sqlx::query_as::<_, AuditLogRow>(
        "SELECT * FROM audit_log WHERE conversation_id = $1 ORDER BY at DESC LIMIT $2",
    )
    .bind(conv_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
