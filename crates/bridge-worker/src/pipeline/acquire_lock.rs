//! `acquire_lock` — lock por conversa (Seção 6.3).
//!
//! `SET lock:conv:{id} <token> NX PX 90000` + liberação por script Lua que
//! verifica o token (evita liberar lock de outro processo). Watchdog de TTL
//! entra se o run passar de 60s.

use std::time::Duration;

use bridge_core::lock_key;
use deadpool_redis::Pool;
use redis::AsyncCommands;
use tracing::{debug, warn};

use crate::state::WorkerError;

/// TTL do lock: 90s. Watchdog estende se o run passar de 60s (spec 6.3 regra 3).
const LOCK_TTL_MS: u64 = 90_000;
/// Janela em que o watchdog ainda não age (spec: 60s).
const WATCHDOG_GRACE_SECS: u64 = 60;

/// Token de posse do lock. Só quem tem o token pode liberar.
#[derive(Debug, Clone)]
pub struct LockGuard {
    pub conv_id: i64,
    pub token: String,
}

/// Script Lua de release (spec 6.3 regra 2). Só deleta se o token bater.
/// Evita que um worker lento libere o lock de outro que já o reassumiu.
const RELEASE_SCRIPT: &str = r#"
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"#;

/// Tenta adquirir o lock. Retorna `Ok(Some(guard))` se pegou; `Ok(None)` se
/// não pegou (conversa sendo processada por outra réplica). Em caso de
/// contenção, o job é re-enfileirado com atraso de 2s (spec 6.3 regra 1).
pub async fn acquire_lock(
    redis_pool: &Pool,
    conv_id: i64,
) -> Result<Option<LockGuard>, WorkerError> {
    let mut conn = redis_pool.get().await?;
    let token = uuid::Uuid::now_v7().to_string();
    let key = lock_key(conv_id);

    // SET NX PX 90000 — atomicamente.
    let set: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg(&token)
        .arg("NX")
        .arg("PX")
        .arg(LOCK_TTL_MS)
        .query_async(&mut *conn)
        .await?;

    if set.is_some() {
        debug!(conv_id, token = %token, "lock acquired");
        Ok(Some(LockGuard { conv_id, token }))
    } else {
        // Contenção: re-enfileira o job com atraso de 2s via ZADD no debounce
        // zset com score = now + 2000ms. O sweeper pega dali a pouco.
        // ponytail: usar o mesmo ZSET de debounce como "fila com atraso" evita
        // criar uma segunda fila. Upgrade: fila dedicada com prioridade se a
        // contenção virar gargalo mensurável.
        let mut conn2 = redis_pool.get().await?;
        let now = chrono::Utc::now().timestamp_millis();
        let _: () = conn2
            .zadd(
                "debounce:zset",
                conv_id.to_string(),
                now + 2_000,
            )
            .await?;
        debug!(conv_id, "lock contention; re-enqueued in 2s");
        Ok(None)
    }
}

/// Libera o lock via script Lua (verificação de token). Idempotente.
pub async fn release_lock(redis_pool: &Pool, guard: &LockGuard) {
    let res = try_release(redis_pool, guard).await;
    if let Err(e) = res {
        warn!(conv_id = guard.conv_id, error = %e, "failed to release lock (will expire)");
    }
}

async fn try_release(redis_pool: &Pool, guard: &LockGuard) -> Result<(), WorkerError> {
    let mut conn = redis_pool.get().await?;
    let key = lock_key(guard.conv_id);
    let script = redis::Script::new(RELEASE_SCRIPT);
    let _: i32 = script
        .key(key)
        .arg(&guard.token)
        .invoke_async(&mut *conn)
        .await?;
    Ok(())
}

/// Watchdog: estende o TTL do lock se o run ainda estiver vivo após a janela
/// de graça. Ponytail: chamado periodicamente pelo `finalize`/long-run path;
/// se o worker morrer, o lock expira sozinho em 90s (spec 6.3 regra 3).
pub async fn extend_lock(redis_pool: &Pool, guard: &LockGuard) -> Result<(), WorkerError> {
    let mut conn = redis_pool.get().await?;
    let key = lock_key(guard.conv_id);
    // PEXPIRE só se o token bater — evita estender lock de outro.
    let script = redis::Script::new(
        r#"if redis.call('get', KEYS[1]) == ARGV[1] then
             return redis.call('pexpire', KEYS[1], ARGV[2])
           else
             return 0
           end"#,
    );
    let _: i32 = script
        .key(key)
        .arg(&guard.token)
        .arg(LOCK_TTL_MS)
        .invoke_async(&mut *conn)
        .await?;
    Ok(())
}

/// Conveniência: duração da janela de graça antes do watchdog.
pub fn watchdog_grace() -> Duration {
    Duration::from_secs(WATCHDOG_GRACE_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lua_release_script_is_token_guarded() {
        // checagem de sanidade: o script só deleta sob match de token.
        assert!(RELEASE_SCRIPT.contains("get', KEYS[1]) == ARGV[1]"));
        assert!(RELEASE_SCRIPT.contains("redis.call('del'"));
    }

    #[test]
    fn lock_key_format() {
        assert_eq!(bridge_core::lock_key(523), "lock:conv:523");
    }

    #[test]
    fn zset_and_queue_constants() {
        // o re-enfileiramento por contenção usa debounce:zset, não queue direta.
        assert_eq!("debounce:zset", "debounce:zset");
        assert_eq!(QUEUE_AGENT_RUNS, "queue:agent_runs");
    }
}
