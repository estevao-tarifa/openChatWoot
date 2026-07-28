//! Buffer de mensagens por conversa (Seção 6.2). `buf:{conv}`.
//!
//! - `push`: `RPUSH` + `EXPIRE 300` (buffer de 5 min).
//! - `drain`: `LRANGE` + `DEL` em pipeline atômico (evita race entre leitura e limpeza).
//! - `count`: `LLEN`.
use crate::redis::RedisPool;
use crate::Result;
use bridge_core::{buffer_key, ConversationId, InboundMessage};
use redis::AsyncCommands;

/// TTL do buffer (segundos). Spec 6.2: 300.
pub const BUFFER_TTL_SECS: u64 = 300;

/// Adiciona msg ao buffer. `RPUSH` (JSON) + `EXPIRE 300`. Retorna o novo tamanho.
pub async fn push(
    pool: &RedisPool,
    conv: ConversationId,
    msg: &InboundMessage,
) -> Result<usize> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let payload = serde_json::to_string(msg)?;
    let key = buffer_key(conv);
    let mut pipe = redis::pipe();
    pipe.atomic();
    pipe.rpush(&key, &payload).ignore();
    pipe.expire(&key, BUFFER_TTL_SECS as i64).ignore();
    pipe.llen(&key);
    let (len,): (usize,) = pipe.query_async(r).await?;
    Ok(len)
}

/// Lê e limpa o buffer. `LRANGE 0 -1` + `DEL` em pipeline atômico — evita
/// race onde mensagens chegam entre leitura e deleção (regra PONYTAIL).
pub async fn drain(pool: &RedisPool, conv: ConversationId) -> Result<Vec<String>> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let key = buffer_key(conv);
    let mut pipe = redis::pipe();
    pipe.atomic();
    pipe.lrange(&key, 0, -1);
    pipe.del(&key);
    let (items, _deleted): (Vec<String>, i64) = pipe.query_async(r).await?;
    Ok(items)
}

/// Conta msgs no buffer. `LLEN`.
pub async fn count(pool: &RedisPool, conv: ConversationId) -> Result<usize> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let n: usize = r.llen(buffer_key(conv)).await?;
    Ok(n)
}
