//! `contact_link` — vínculo contato <-> cliente do ERP (Seção 10.6/13).
use crate::pg::PgPool;
use crate::Result;
use bridge_core::ContactId;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::FromRow;

/// Espelha a tabela `contact_link`.
#[derive(Debug, Clone, FromRow)]
pub struct ContactLinkRow {
    pub contact_id: ContactId,
    pub erp_client_id: Option<String>,
    pub cnpj: Option<String>,
    pub verified: bool,
    pub verified_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub attributes: Value,
}

impl ContactLinkRow {
    pub fn new(contact_id: ContactId) -> Self {
        Self {
            contact_id,
            erp_client_id: None,
            cnpj: None,
            verified: false,
            verified_at: None,
            expires_at: None,
            attributes: Value::Object(serde_json::Map::new()),
        }
    }
}

/// Upsert do vínculo. `contact_id` é PK.
pub async fn upsert(pool: &PgPool, row: &ContactLinkRow) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contact_link
          (contact_id, erp_client_id, cnpj, verified, verified_at, expires_at, attributes)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (contact_id) DO UPDATE SET
          erp_client_id = EXCLUDED.erp_client_id,
          cnpj          = EXCLUDED.cnpj,
          verified      = EXCLUDED.verified,
          verified_at   = EXCLUDED.verified_at,
          expires_at    = EXCLUDED.expires_at,
          attributes    = EXCLUDED.attributes
        "#,
    )
    .bind(row.contact_id)
    .bind(&row.erp_client_id)
    .bind(&row.cnpj)
    .bind(row.verified)
    .bind(row.verified_at)
    .bind(row.expires_at)
    .bind(&row.attributes)
    .execute(pool)
    .await?;
    Ok(())
}

/// Busca vínculo por contato. `None` se não existir (contato não verificado).
pub async fn get(pool: &PgPool, contact_id: ContactId) -> Result<Option<ContactLinkRow>> {
    let row = sqlx::query_as::<_, ContactLinkRow>(
        "SELECT * FROM contact_link WHERE contact_id = $1",
    )
    .bind(contact_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Marca como verificado (telefone/CNPJ confirmados contra o ERP).
pub async fn mark_verified(
    pool: &PgPool,
    contact_id: ContactId,
    erp_client_id: &str,
    cnpj: Option<&str>,
    verified_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE contact_link SET
             verified=true, erp_client_id=$2, cnpj=COALESCE($3, cnpj),
             verified_at=$4, expires_at=$5 WHERE contact_id=$1"#,
    )
    .bind(contact_id)
    .bind(erp_client_id)
    .bind(cnpj)
    .bind(verified_at)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// `true` se o vínculo está verificado e dentro da validade (TTL 30d, Seção 10.6.3).
pub async fn is_verified_and_valid(
    pool: &PgPool,
    contact_id: ContactId,
    now: &DateTime<Utc>,
) -> Result<bool> {
    let ok: Option<bool> = sqlx::query_scalar(
        "SELECT verified AND (expires_at IS NULL OR expires_at > $2) FROM contact_link WHERE contact_id = $1",
    )
    .bind(contact_id)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(ok.unwrap_or(false))
}
