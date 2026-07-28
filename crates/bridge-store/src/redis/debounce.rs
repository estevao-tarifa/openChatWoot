//! Debounce com janela deslizante via ZSET (Seção 6.2).
//!
//! `debounce:zset` mapeia `conv_id -> score(fire_at_ms)`. O sweeper do worker
//! faz `ZRANGEBYSCORE -inf now` a cada 250ms. Durável e inspecionável —
//! prefira a keyspace notifications (decisão fechada, Seção 16.3).
use crate::redis::RedisPool;
use crate::Result;
use bridge_core::{ConversationId, DEBOUNCE_ZSET};
use redis::AsyncCommands;

/// Agenda ou reagenda um timer de debounce no ZSET. `ZADD` substitui o score
/// anterior (mesmo membro) — assim cada nova mensagem desliza a janela.
pub async fn schedule(
    pool: &RedisPool,
    conv: ConversationId,
    at_ms: i64,
) -> Result<()> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let _: i64 = r.zadd(DEBOUNCE_ZSET, conv.to_string(), at_ms).await?;
    Ok(())
}

/// Busca convs com timer vencido (score <= `now_ms`). Usado pelo sweeper.
/// `LIMIT 0 limit` para processar em lotes.
pub async fn poll_due(
    pool: &RedisPool,
    now_ms: i64,
    limit: usize,
) -> Result<Vec<ConversationId>> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let items: Vec<String> = r
        .zrangebyscore_limit(DEBOUNCE_ZSET, "-inf", now_ms, 0, limit as isize)
        .await?;
    // ponytail: parse simples — membros são sempre conv_id serializado por nós.
    let convs = items
        .into_iter()
        .filter_map(|s| s.parse::<ConversationId>().ok())
        .collect();
    Ok(convs)
}

/// Remove timer do ZSET (quando o disparo acontece ou é cancelado).
pub async fn cancel(pool: &RedisPool, conv: ConversationId) -> Result<()> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let _: i64 = r.zrem(DEBOUNCE_ZSET, conv.to_string()).await?;
    Ok(())
}
