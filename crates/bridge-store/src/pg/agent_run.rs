//! `agent_run` — cada execução do agente (Seção 13).
use crate::pg::PgPool;
use crate::Result;
use bridge_core::{ConversationId, RunId};
use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

/// Espelha a tabela `agent_run`.
/// `cost_usd` lido como `f64` via cast `::float8` (NUMERIC sem feature extra).
#[derive(Debug, Clone, FromRow)]
pub struct AgentRunRow {
    pub run_id: Uuid,
    pub conversation_id: ConversationId,
    pub provider: String,
    pub agent_id: Option<String>,
    pub trigger_reason: String,
    pub input_msg_ids: Vec<i64>,
    pub status: String,
    pub error_kind: Option<String>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub cost_usd: Option<f64>,
    pub latency_ms: Option<i32>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl AgentRunRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        conversation_id: ConversationId,
        provider: impl Into<String>,
        agent_id: Option<String>,
        trigger_reason: impl Into<String>,
        input_msg_ids: Vec<i64>,
    ) -> Self {
        Self {
            run_id: run_id.as_uuid(),
            conversation_id,
            provider: provider.into(),
            agent_id,
            trigger_reason: trigger_reason.into(),
            input_msg_ids,
            status: "running".into(),
            error_kind: None,
            input_tokens: None,
            output_tokens: None,
            cost_usd: None,
            latency_ms: None,
            started_at: Utc::now(),
            finished_at: None,
        }
    }
}

/// Insere um run no estado `running` (início do turno).
pub async fn insert(pool: &PgPool, row: &AgentRunRow) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO agent_run
          (run_id, conversation_id, provider, agent_id, trigger_reason,
           input_msg_ids, status, error_kind, input_tokens, output_tokens,
           cost_usd, latency_ms, started_at, finished_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        "#,
    )
    .bind(row.run_id)
    .bind(row.conversation_id)
    .bind(&row.provider)
    .bind(&row.agent_id)
    .bind(&row.trigger_reason)
    .bind(&row.input_msg_ids)
    .bind(&row.status)
    .bind(&row.error_kind)
    .bind(row.input_tokens)
    .bind(row.output_tokens)
    .bind(row.cost_usd)
    .bind(row.latency_ms)
    .bind(row.started_at)
    .bind(row.finished_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Marca o fim do run: status final, tokens, custo, latência, `finished_at`.
#[allow(clippy::too_many_arguments)]
pub async fn finish(
    pool: &PgPool,
    run_id: RunId,
    status: &str,
    error_kind: Option<&str>,
    input_tokens: Option<i32>,
    output_tokens: Option<i32>,
    cost_usd: Option<f64>,
    latency_ms: Option<i32>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE agent_run SET
          status       = $2,
          error_kind   = $3,
          input_tokens = COALESCE($4, input_tokens),
          output_tokens= COALESCE($5, output_tokens),
          cost_usd     = COALESCE($6, cost_usd),
          latency_ms   = COALESCE($7, latency_ms),
          finished_at  = now()
        WHERE run_id = $1
        "#,
    )
    .bind(run_id.as_uuid())
    .bind(status)
    .bind(error_kind)
    .bind(input_tokens)
    .bind(output_tokens)
    .bind(cost_usd)
    .bind(latency_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// Busca um run pelo id.
pub async fn get(pool: &PgPool, run_id: RunId) -> Result<Option<AgentRunRow>> {
    let row = sqlx::query_as::<_, AgentRunRow>(
        // ponytail: cast `::float8` evita depender da feature `bigdecimal`/`rust_decimal` do sqlx.
        r#"SELECT run_id, conversation_id, provider, agent_id, trigger_reason,
                  input_msg_ids, status, error_kind, input_tokens, output_tokens,
                  cost_usd::float8 AS cost_usd, latency_ms, started_at, finished_at
           FROM agent_run WHERE run_id = $1"#,
    )
    .bind(run_id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Soma do custo (USD) gasto no dia atual — alimenta o limitador L6.
pub async fn spent_today(pool: &PgPool) -> Result<f64> {
    let total: Option<f64> = sqlx::query_scalar(
        "SELECT COALESCE(sum(cost_usd), 0)::float8 FROM agent_run WHERE started_at::date = now()::date",
    )
    .fetch_one(pool)
    .await?;
    Ok(total.unwrap_or(0.0))
}
