//! Fila de agent runs (Seção 6.2). `queue:agent_runs`.
//!
//! `LPUSH` enfileira (cabeça), `BRPOP` consome (cauda) → FIFO. O payload é
//! JSON `{conv_id, reason, trace_id}`.
use crate::redis::RedisPool;
use crate::Result;
use bridge_core::{ConversationId, QUEUE_AGENT_RUNS};
use redis::AsyncCommands;

/// Timeout padrão do `BRPOP` (segundos). Evita bloquear indefinidamente.
// ponytail: f64 — é o tipo do timeout de brpop no redis-rs 0.27.
pub const DEQUEUE_TIMEOUT_SECS: f64 = 5.0;

/// Enfileira job de agent run. `LPUSH queue:agent_runs {json}`.
pub async fn enqueue(
    pool: &RedisPool,
    conv_id: ConversationId,
    reason: &str,
    trace_id: &str,
) -> Result<()> {
    let payload = serde_json::json!({
        "conv_id": conv_id,
        "reason": reason,
        "trace_id": trace_id,
    });
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    r.lpush::<_, _, ()>(QUEUE_AGENT_RUNS, payload.to_string()).await?;
    Ok(())
}

/// Consome próximo job (`BRPOP`). Bloqueia até `DEQUEUE_TIMEOUT_SECS`.
/// Retorna `Some(payload_json)` quando há item, `None` no timeout.
pub async fn dequeue(pool: &RedisPool) -> Result<Option<String>> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let res: Option<(String, String)> = r.brpop(QUEUE_AGENT_RUNS, DEQUEUE_TIMEOUT_SECS).await?;
    Ok(res.map(|(_, v)| v))
}
