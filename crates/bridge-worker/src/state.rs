//! Estado compartilhado do worker: `AppState`, `ConversationState` e os
//! helpers de acesso a Postgres/Redis que o pipeline usa.
//!
//! ponytail: o crate `bridge-store` existe no workspace mas está incompleto
//! (declara módulos `gate_decision`/`outbound`/`audit` que não estão no
//! disco). Para o worker não depender de código que não compila, mantemos
//! aqui só o CRUD mínimo de `conversation_state`/`agent_run`/`gate_decision`
//! que o pipeline realmente toca. Consolidar no `bridge-store` quando ele
//! estiver completo — a migração é mover as funções, não reescrever lógica.

use std::sync::Arc;

use bridge_agent::AgentProvider;
use bridge_chatwoot::ChatwootClient;
use bridge_core::{AiState, Config, ConversationId, RunId};
use chrono::{DateTime, Utc};
use deadpool_redis::Pool as RedisPool;
use sqlx::PgPool;
use thiserror::Error;

/// Erro do worker. Tratado com log, nunca pânico (regra PONYTAIL / spec 9.3).
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("redis pool: {0}")]
    RedisPool(#[from] deadpool_redis::PoolError),
    #[error("postgres: {0}")]
    Pg(#[from] sqlx::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("chatwoot: {0}")]
    Chatwoot(#[from] bridge_chatwoot::ChatwootError),
    #[error("agent: {0}")]
    Agent(#[from] bridge_core::AgentError),
    #[error("state transition: {0}")]
    State(String),
    #[error("gate blocked: {rule} — {reason}")]
    Blocked { rule: String, reason: String },
    #[error("lock contention")]
    LockContention,
    #[error("empty turn")]
    EmptyTurn,
    #[error("io: {0}")]
    Io(String),
}

/// Container de dependências compartilhadas entre as tasks do worker.
/// Construído uma vez em `main.rs` e clonado (Arc) para cada `tokio::spawn`.
pub struct AppState {
    pub redis: RedisPool,
    pub pg: PgPool,
    pub chatwoot: Arc<ChatwootClient>,
    pub agent: Arc<Box<dyn AgentProvider>>,
    pub fallback: Option<Arc<Box<dyn AgentProvider>>>,
    pub config: Arc<Config>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            redis: self.redis.clone(),
            pg: self.pg.clone(),
            chatwoot: self.chatwoot.clone(),
            agent: self.agent.clone(),
            fallback: self.fallback.clone(),
            config: self.config.clone(),
        }
    }
}

/// Espelha a tabela `conversation_state` (Seção 13). Apenas os campos que o
/// worker lê/escreve. `ai_state` já tipado como `AiState` na carga.
#[derive(Debug, Clone, Default)]
pub struct ConversationState {
    pub conversation_id: ConversationId,
    pub account_id: i64,
    pub inbox_id: i64,
    pub contact_id: i64,
    pub channel: String,
    pub ai_state: AiState,
    pub chatwoot_status: String,
    pub assignee_id: Option<i64>,
    pub team_id: Option<i64>,
    pub labels: Vec<String>,
    pub provider_session_id: Option<String>,
    pub prior_ai_turns_in_row: u16,
    pub last_ai_msg_hash: Option<String>,
    pub paused_until: Option<DateTime<Utc>>,
    pub pause_reason: Option<String>,
}

impl ConversationState {
    /// `true` se a IA está pausada por qualquer motivo (manual/limitador).
    pub fn is_ai_paused(&self) -> bool {
        self.ai_state.is_paused()
    }
}

/// Carrega o estado de controle de uma conversa. `None` se a linha não
/// existir (conversa não ingerida pelo `bridge-api` ainda).
pub async fn load_conversation_state(
    pool: &PgPool,
    conv_id: ConversationId,
) -> Result<Option<ConversationState>, WorkerError> {
    let row = sqlx::query(
        r#"SELECT conversation_id, account_id, inbox_id, contact_id, channel,
                  ai_state, chatwoot_status, assignee_id, team_id, labels,
                  provider_session_id, prior_ai_turns_in_row, last_ai_msg_hash,
                  paused_until, pause_reason
           FROM conversation_state WHERE conversation_id = $1"#,
    )
    .bind(conv_id)
    .fetch_optional(pool)
    .await?;

    let Some(r) = row else { return Ok(None) };

    let ai_state_str: String = r.try_get("ai_state").unwrap_or_else(|_| "ai_active".into());
    let ai_state = parse_ai_state(&ai_state_str);

    // ponytail: `prior_ai_turns_in_row` é SMALLINT no DB; truncamos para u16
    // (limite de spec = 4). Se algum dia passar de 32767, há bug maior.
    let prior: i16 = r.try_get::<i16, _>("prior_ai_turns_in_row").unwrap_or(0);

    Ok(Some(ConversationState {
        conversation_id: r.try_get("conversation_id").unwrap_or(conv_id),
        account_id: r.try_get("account_id").unwrap_or(0),
        inbox_id: r.try_get("inbox_id").unwrap_or(0),
        contact_id: r.try_get("contact_id").unwrap_or(0),
        channel: r.try_get("channel").unwrap_or_default(),
        ai_state,
        chatwoot_status: r.try_get("chatwoot_status").unwrap_or_default(),
        assignee_id: r.try_get("assignee_id").unwrap_or(None),
        team_id: r.try_get("team_id").unwrap_or(None),
        labels: r.try_get("labels").unwrap_or_default(),
        provider_session_id: r.try_get("provider_session_id").unwrap_or(None),
        prior_ai_turns_in_row: prior.max(0) as u16,
        last_ai_msg_hash: r.try_get("last_ai_msg_hash").unwrap_or(None),
        paused_until: r.try_get("paused_until").unwrap_or(None),
        pause_reason: r.try_get("pause_reason").unwrap_or(None),
    }))
}

/// Persiste o `ai_state` e campos de controle relevantes ao fim do turno.
/// ponytail: atualização focal — só o que o worker mudou. Labels/timestamps
/// de msg viram chamadas separadas (touch_msg_at / set_labels no Chatwoot).
pub async fn save_ai_state(
    pool: &PgPool,
    conv_id: ConversationId,
    ai_state: AiState,
    prior_ai_turns_in_row: u16,
    provider_session_id: Option<&str>,
    last_ai_msg_hash: Option<&str>,
) -> Result<(), WorkerError> {
    sqlx::query(
        r#"UPDATE conversation_state SET
             ai_state = $2,
             prior_ai_turns_in_row = $3,
             provider_session_id = COALESCE($4, provider_session_id),
             last_ai_msg_hash = COALESCE($5, last_ai_msg_hash),
             last_ai_msg_at = CASE WHEN $5 IS NULL THEN last_ai_msg_at ELSE now() END,
             updated_at = now()
           WHERE conversation_id = $1"#,
    )
    .bind(conv_id)
    .bind(ai_state.as_str())
    .bind(prior_ai_turns_in_row as i16)
    .bind(provider_session_id)
    .bind(last_ai_msg_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Insere uma decisão de gate para auditoria (Seção 8.1: "por que a IA não
/// respondeu?"). Append-only.
pub async fn record_gate_decision(
    pool: &PgPool,
    conv_id: ConversationId,
    gate: &str,
    rule: &str,
    decision: &str,
    detail: Option<&serde_json::Value>,
) -> Result<(), WorkerError> {
    sqlx::query(
        r#"INSERT INTO gate_decision
           (conversation_id, gate, rule, decision, detail)
           VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind(conv_id)
    .bind(gate)
    .bind(rule)
    .bind(decision)
    .bind(detail)
    .execute(pool)
    .await?;
    Ok(())
}

/// Cria a linha de `agent_run` no estado `running`.
pub async fn insert_agent_run(
    pool: &PgPool,
    run_id: RunId,
    conv_id: ConversationId,
    provider: &str,
    trigger_reason: &str,
    input_msg_ids: &[i64],
) -> Result<(), WorkerError> {
    sqlx::query(
        r#"INSERT INTO agent_run
           (run_id, conversation_id, provider, trigger_reason, input_msg_ids,
            status, started_at)
           VALUES ($1, $2, $3, $4, $5, 'running', now())"#,
    )
    .bind(run_id.as_uuid())
    .bind(conv_id)
    .bind(provider)
    .bind(trigger_reason)
    .bind(input_msg_ids)
    .execute(pool)
    .await?;
    Ok(())
}

/// Finaliza o `agent_run` com status/tokens/custo/latência.
pub async fn finish_agent_run(
    pool: &PgPool,
    run_id: RunId,
    status: &str,
    error_kind: Option<&str>,
    input_tokens: Option<i32>,
    output_tokens: Option<i32>,
    cost_usd: Option<f64>,
    latency_ms: Option<i32>,
) -> Result<(), WorkerError> {
    sqlx::query(
        r#"UPDATE agent_run SET
             status = $2,
             error_kind = $3,
             input_tokens = COALESCE($4, input_tokens),
             output_tokens = COALESCE($5, output_tokens),
             cost_usd = COALESCE($6, cost_usd),
             latency_ms = COALESCE($7, latency_ms),
             finished_at = now()
           WHERE run_id = $1"#,
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

/// Soma do custo (USD) gasto hoje — alimenta o limitador L6 (orçamento diário).
pub async fn spent_today(pool: &PgPool) -> Result<f64, WorkerError> {
    let total: Option<f64> = sqlx::query_scalar(
        "SELECT COALESCE(sum(cost_usd), 0)::float8 FROM agent_run \
         WHERE started_at::date = now()::date",
    )
    .fetch_one(pool)
    .await?;
    Ok(total.unwrap_or(0.0))
}

use sqlx::Row;

/// Converte a string `ai_state` do banco (snake_case) em `AiState`.
/// ponytail: `AiState` não impl `FromStr` no bridge-core; fazemos o match
/// aqui em vez de depender de serde (o valor vem cru, sem aspas JSON).
/// Centralizar quando o `bridge-store` ganhar o helper.
fn parse_ai_state(s: &str) -> AiState {
    match s.trim().to_ascii_lowercase().as_str() {
        "ai_thinking" => AiState::AiThinking,
        "awaiting_human" => AiState::AwaitingHuman,
        "human_handling" => AiState::HumanHandling,
        "ai_paused_manual" => AiState::AiPausedManual,
        "ai_paused_limit" => AiState::AiPausedLimit,
        "closed" => AiState::Closed,
        _ => AiState::AiActive, // default seguro: deixa a IA responder
    }
}
