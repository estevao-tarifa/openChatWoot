//! `gate_decision` — auditabilidade dos gates de entrada/saída (Seção 8/13).
use crate::pg::PgPool;
use crate::Result;
use bridge_core::ConversationId;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::FromRow;

/// Espelha a tabela `gate_decision`.
#[derive(Debug, Clone, FromRow)]
pub struct GateDecisionRow {
    pub id: i64,
    pub conversation_id: ConversationId,
    pub gate: String,
    pub rule: String,
    pub decision: String,
    pub detail: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl GateDecisionRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conversation_id: ConversationId,
        gate: impl Into<String>,
        rule: impl Into<String>,
        decision: impl Into<String>,
        detail: Option<Value>,
    ) -> Self {
        Self {
            id: 0,
            conversation_id,
            gate: gate.into(),
            rule: rule.into(),
            decision: decision.into(),
            detail,
            created_at: Utc::now(),
        }
    }
}

/// Append-only: registra uma decisão de gate.
// ponytail: sem UPDATE/DELETE — gate_decision é histórico imutável por design (Seção 8.1).
pub async fn insert(pool: &PgPool, row: &GateDecisionRow) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO gate_decision (conversation_id, gate, rule, decision, detail)
        VALUES ($1,$2,$3,$4,$5) RETURNING id
        "#,
    )
    .bind(row.conversation_id)
    .bind(&row.gate)
    .bind(&row.rule)
    .bind(&row.decision)
    .bind(row.detail.as_ref())
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Histórico recente de decisões para uma conversa (mais novas primeiro).
pub async fn list_for_conv(
    pool: &PgPool,
    conv_id: ConversationId,
    limit: i64,
) -> Result<Vec<GateDecisionRow>> {
    let rows = sqlx::query_as::<_, GateDecisionRow>(
        "SELECT * FROM gate_decision WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT $2",
    )
    .bind(conv_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
