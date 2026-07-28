//! Deduplicação de webhook (Seção 6.4). `SET NX EX 86400` em `seen:msg:{account}:{msg_id}`.
use crate::redis::RedisPool;
use crate::Result;
use bridge_core::{dedup_key, AccountId};

/// TTL da chave de dedup (24h). Spec 6.4.
pub const DEDUP_TTL_SECS: u64 = 86_400;

/// Dedup de webhook: `SET NX EX 86400`. Retorna `true` se é novo (deve
/// processar), `false` se já foi visto (ignorar e responder 200).
pub async fn check_and_set(
    pool: &RedisPool,
    msg_id: i64,
    account: AccountId,
) -> Result<bool> {
    let mut conn = pool.get().await?;
    let was_set: Option<String> = redis::cmd("SET")
        .arg(dedup_key(account, msg_id))
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(DEDUP_TTL_SECS)
        .query_async(&mut *conn)
        .await?;
    Ok(was_set.is_some())
}
