//! Postgres: pool, migrações, health check.
//! Tabelas conforme migration `001_initial.sql` (Seção 13 da spec).

pub mod agent_run;
pub mod audit;
pub mod contact_link;
pub mod conversation;
pub mod gate_decision;
pub mod message_log;
pub mod outbound;
pub mod sla;

use sqlx::postgres::PgPoolOptions;

// ponytail: `PgPool` é re-export de sqlx, não wrapper próprio (regra PONYTAIL).
pub type PgPool = sqlx::PgPool;

/// Cria o pool a partir da `DATABASE_URL`. Configura acquire_timeout e limites
/// razoáveis para a VPS-alvo (2 GB). Ajustar via config quando necessário.
pub async fn create_pool(database_url: &str) -> Result<PgPool, crate::StoreError> {
    Ok(PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(database_url)
        .await?)
}

/// Roda as migrações. O dir `migrations/` vive na raiz do workspace; o macro
/// `sqlx::migrate!` resolve relativo a `CARGO_MANIFEST_DIR` (compile time).
pub async fn run_migrations(pool: &PgPool) -> Result<(), crate::StoreError> {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await
        // ponytail: MigrateError não vira StoreError direto; passa por sqlx::Error.
        .map_err(|e| crate::StoreError::Pg(e.into()))?;
    Ok(())
}

/// Health check simples: `SELECT 1`. `true` se o Postgres respondeu.
pub async fn health(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await
        .is_ok()
}
