//! bridge-store — camada de persistência da ponte (PostgreSQL + Redis).
//!
//! Spec normativa: `ESPECchatwootaibridge.md` Seções 6.2–6.5 e 13.
//! Este crate é a **única** fronteira com Postgres/Redis; o domínio puro
//! (`bridge-core`) não toca I/O.
//!
//! // ponytail: `StoreError` vive no lib.rs em vez de arquivo próprio — um
//! enum curto não justifica um módulo extra. Mover para `error.rs` quando
//! surgirem conversões `From` demais para caber aqui.

pub mod pg;
pub mod redis;

// Re-export para acesso simplificado (lib.rs da spec).
pub use pg::PgPool;
pub use redis::RedisPool;

use thiserror::Error;

/// Erro unificado de persistência.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("postgres: {0}")]
    Pg(#[from] sqlx::Error),
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("pool: {0}")]
    Pool(#[from] deadpool_redis::PoolError),
    #[error("pool build: {0}")]
    BuildPool(String),
    #[error("not found: {entity} {id}")]
    NotFound { entity: String, id: String },
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;
