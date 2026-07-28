//! Lock por conversa — um turno de cada vez (Seção 6.3).
//!
//! `SET NX PX 90000` (90s). Liberação por script Lua que compara o token:
//! evita liberar o lock de outro processo. Watchdog estende o TTL via `PEXPIRE`
//! com a mesma checagem de token.
use crate::redis::RedisPool;
use crate::Result;
use bridge_core::{lock_key, ConversationId};
use uuid::Uuid;

/// TTL padrão do lock (ms). Spec 6.3: 90s.
pub const LOCK_TTL_MS: u64 = 90_000;

// Script carregado em tempo de compilação (regra PONYTAIL: include_str!).
const RELEASE_LOCK_LUA: &str = include_str!("scripts/release_lock.lua");

/// Script de extensão: só estende se o token bater (mesma garantia do release).
const EXTEND_LOCK_LUA: &str = r#"
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"#;

/// Adquire lock por conversa. `SET NX PX 90000`. Retorna `Some(token)` se
/// adquirido, `None` se já estava preso (caller reenfileira com atraso, 6.3.1).
pub async fn acquire(pool: &RedisPool, conv: ConversationId) -> Result<Option<String>> {
    let mut conn = pool.get().await?;
    let token = Uuid::now_v7().to_string();
    let key = lock_key(conv);
    let got: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg(&token)
        .arg("NX")
        .arg("PX")
        .arg(LOCK_TTL_MS)
        .query_async(&mut *conn)
        .await?;
    Ok(got.map(|_| token))
}

/// Libera usando script Lua. Só libera se o token bater. `true` se liberou.
pub async fn release(pool: &RedisPool, conv: ConversationId, token: &str) -> Result<bool> {
    let mut conn = pool.get().await?;
    let n: i64 = redis::Script::new(RELEASE_LOCK_LUA)
        .key(lock_key(conv))
        .arg(token)
        .invoke_async(&mut *conn)
        .await?;
    Ok(n == 1)
}

/// Estende TTL do lock (watchdog). Só estende se o token bater. `true` se estendeu.
pub async fn extend(
    pool: &RedisPool,
    conv: ConversationId,
    token: &str,
    ms: u64,
) -> Result<bool> {
    let mut conn = pool.get().await?;
    let n: i64 = redis::Script::new(EXTEND_LOCK_LUA)
        .key(lock_key(conv))
        .arg(token)
        .arg(ms)
        .invoke_async(&mut *conn)
        .await?;
    Ok(n == 1)
}
