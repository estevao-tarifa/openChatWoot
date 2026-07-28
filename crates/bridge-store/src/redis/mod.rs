//! Redis: pool + operações de buffer/debounce/lock/dedup/ratelimit/queue.
//! Spec Seções 6.2–6.5.
//!
//! // ponytail: `RedisPool` é re-export de `deadpool_redis::Pool`, não wrapper.
//! Cada função recebe `&RedisPool` explicitamente — é a convenção do repo
//! (bridge-api/worker/scheduler) e a única forma honesta de alcançar o Redis
//! sem um global escondido.

pub mod buffer;
pub mod debounce;
pub mod dedup;
pub mod lock;
pub mod queue;
pub mod ratelimit;

pub use deadpool_redis::Pool as RedisPool;

use deadpool_redis::{Config, Runtime};

/// Cria o pool a partir da `REDIS_URL`.
pub async fn create_pool(redis_url: &str) -> Result<RedisPool, crate::StoreError> {
    let cfg = Config::from_url(redis_url);
    // ponytail: erro de build (ConfigError/CreatePoolError) é distinto do erro
    // de `get()` (PoolError); convertemos para string em vez de variante `#[from]`
    // para não puxar duas dependências de erro do deadpool.
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1))
        .map_err(|e| crate::StoreError::BuildPool(e.to_string()))?;
    Ok(pool)
}

/// Health check: `PING`. `true` se o Redis respondeu.
pub async fn health(pool: &RedisPool) -> bool {
    let Ok(mut conn) = pool.get().await else { return false };
    redis::cmd("PING")
        .query_async::<String>(&mut *conn)
        .await
        .is_ok()
}
