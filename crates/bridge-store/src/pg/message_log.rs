//! `message_log` — metadados + conteúdo cifrado (pgcrypto, Seção 10.4/13).
//!
//! // ponytail: a row espelha a tabela (carrega `content_enc` já cifrado).
//! A cifragem em si é exposta por helpers `encrypt_content`/`decrypt_content`
//! que chamam `pgp_sym_encrypt`/`pgp_sym_decrypt` no Postgres. A chave vive
//! em config (PII_KEY), nunca hardcodeada.
use crate::pg::PgPool;
use crate::Result;
use bridge_core::ConversationId;
use chrono::{DateTime, Utc};
use sqlx::FromRow;

/// Espelha a tabela `message_log`. `content_enc` é o blob cifrado (BYTEA).
#[derive(Debug, Clone, FromRow)]
pub struct MessageLogRow {
    pub id: i64,
    pub chatwoot_msg_id: i64,
    pub conversation_id: ConversationId,
    pub direction: String,
    pub sender_kind: String,
    pub is_private: bool,
    pub content_enc: Option<Vec<u8>>,
    pub content_len: i32,
    pub has_attachment: bool,
    pub created_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
}

impl MessageLogRow {
    /// Construtor a partir de dados de webhook. `content_enc` deve vir já
    /// cifrado por `encrypt_content` quando houver conteúdo.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chatwoot_msg_id: i64,
        conversation_id: ConversationId,
        direction: impl Into<String>,
        sender_kind: impl Into<String>,
        is_private: bool,
        content_enc: Option<Vec<u8>>,
        content_len: i32,
        has_attachment: bool,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: 0,
            chatwoot_msg_id,
            conversation_id,
            direction: direction.into(),
            sender_kind: sender_kind.into(),
            is_private,
            content_enc,
            content_len,
            has_attachment,
            created_at,
            ingested_at: Utc::now(),
        }
    }
}

/// Insere a linha e retorna o `id` (BIGSERIAL). `UNIQUE (chatwoot_msg_id)`
/// protege contra reprocessamento de webhook — erro de violação vira `Pg` error
/// que o caller trata como duplicado.
pub async fn insert(pool: &PgPool, row: &MessageLogRow) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO message_log
          (chatwoot_msg_id, conversation_id, direction, sender_kind, is_private,
           content_enc, content_len, has_attachment, created_at, ingested_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        RETURNING id
        "#,
    )
    .bind(row.chatwoot_msg_id)
    .bind(row.conversation_id)
    .bind(&row.direction)
    .bind(&row.sender_kind)
    .bind(row.is_private)
    .bind(row.content_enc.as_deref())
    .bind(row.content_len)
    .bind(row.has_attachment)
    .bind(row.created_at)
    .bind(row.ingested_at)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Cifra conteúdo via `pgp_sym_encrypt` (pgcrypto, AES). Retorna o BYTEA.
pub async fn encrypt_content(
    pool: &PgPool,
    plaintext: &str,
    pii_key: &str,
) -> Result<Vec<u8>> {
    let enc: Vec<u8> = sqlx::query_scalar("SELECT pgp_sym_encrypt($1, $2)")
        .bind(plaintext)
        .bind(pii_key)
        .fetch_one(pool)
        .await?;
    Ok(enc)
}

/// Decifra conteúdo via `pgp_sym_decrypt`. `None` se `enc` for `None`.
pub async fn decrypt_content(
    pool: &PgPool,
    enc: Option<&[u8]>,
    pii_key: &str,
) -> Result<Option<String>> {
    let Some(enc) = enc else { return Ok(None) };
    let txt: String = sqlx::query_scalar("SELECT pgp_sym_decrypt($1, $2)")
        .bind(enc)
        .bind(pii_key)
        .fetch_one(pool)
        .await?;
    Ok(Some(txt))
}

/// Purge de retenção: apaga linhas ingeridas antes de `before` E mais antigas
/// que `days`. Retorna quantas linhas foram removidas.
// ponytail: `days` é redundante com `before` na prática, mas a spec pede ambos;
// usa `before - days` como janela para ser defensivo quando caller passa now().
pub async fn purge_before(
    pool: &PgPool,
    before: &DateTime<Utc>,
    days: i32,
) -> Result<u64> {
    let res = sqlx::query(
        "DELETE FROM message_log WHERE ingested_at < $1 AND ingested_at < ($1 - make_interval(days => $2))",
    )
    .bind(before)
    .bind(days)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Conta mensagens por conversa (debug/observabilidade).
#[allow(dead_code)]
pub async fn count_for_conv(pool: &PgPool, conv_id: ConversationId) -> Result<i64> {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM message_log WHERE conversation_id = $1")
        .bind(conv_id)
        .fetch_one(pool)
        .await?;
    Ok(n)
}
