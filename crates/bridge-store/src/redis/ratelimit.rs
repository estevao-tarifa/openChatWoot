//! Limitadores em cascata L1–L6 (Seção 6.5).
//!
//! Cada limitador é um token bucket atômico em Lua (leitura + decremento num
//! único round-trip, sem race). A verificação ocorre em ordem L1→L6; a função
//! `check_all` retorna a lista de códigos dos limitadores que estouraram
//! (vazia = tudo liberado).
//!
//! // ponytail: um único script Lua serve para os buckets L1–L4 (mesma forma,
//! só muda a chave e a janela). L5 (semáforo) e L6 (orçamento) têm formato
//! próprio. Mantém tudo aqui — sem abstração de "Limiter trait" para 6 casos.
use crate::redis::RedisPool;
use crate::Result;
use bridge_core::ratelimit::RateLimits;
use bridge_core::{AccountId, ContactId, ConversationId};
use redis::Script;

/// Códigos dos limitadores (Seção 6.5).
pub const L1_CONV: &str = "L1";
pub const L2_CONTACT: &str = "L2";
pub const L3_OUT: &str = "L3";
pub const L4_ACCOUNT: &str = "L4";
pub const L5_CONCURRENCY: &str = "L5";
pub const L6_BUDGET: &str = "L6";

/// Token bucket fixo por janela: conta requisições na janela atual e expira a
/// chave ao fim da janela. `ARGV[1]=max`, `ARGV[2]=window_secs`.
/// Retorna 1 se permitido (incrementou), 0 se estourou.
const BUCKET_LUA: &str = r#"
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
    return 0
end
return 1
"#;

/// Semáforo de concorrência (L5): `INCR` com TTL por slot. Sem lista de slots
/// individuais — aproximamos com um contador com TTL curto por run (15s por
/// slot). `ARGV[1]=max`, `ARGV[2]=slot_ttl_secs`.
const SEMAPHORE_LUA: &str = r#"
local current = redis.call("GET", KEYS[1])
local n = 0
if current then n = tonumber(current) end
if n >= tonumber(ARGV[1]) then
    return 0
end
redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], ARGV[2])
return 1
"#;

/// Orçamento diário (L6): soma de custo. Aqui verificamos o limite antes de
/// gastar — o gasto real é registrado em `agent_run.cost_usd` (Postgres) e o
/// contador diário no Redis via `INCRBYFLOAT`. `ARGV[1]=budget_usd`.
const BUDGET_LUA: &str = r#"
local spent = redis.call("GET", KEYS[1])
local s = 0
if spent then s = tonumber(spent) end
if s >= tonumber(ARGV[1]) then
    return 0
end
return 1
"#;

/// Verifica L1–L6 em ordem. Retorna os códigos dos limitadores que estouraram
/// (vazia = tudo liberado). Não bloqueia nem enfileira — essa decisão é do
/// caller (Gate de Entrada, Seção 8.1 G9). L3 (saída) é verificado à parte no
/// Gate de Saída; aqui é incluído como leitura de estado.
// ponytail: retorna Vec<String> com os códigos estourados em vez de um enum —
// o caller precisa saber "quais" para decidir a ação (pausar, enfileirar, handoff).
pub async fn check_all(
    pool: &RedisPool,
    conv_id: ConversationId,
    contact_id: ContactId,
    account_id: AccountId,
    limits: &RateLimits,
) -> Result<Vec<String>> {
    let mut conn = pool.get().await?;
    let r = &mut *conn;
    let mut breached = Vec::new();

    // L1 — runs por conversa por minuto.
    let ok: i64 = Script::new(BUCKET_LUA)
        .key(RateLimits::conv_key(conv_id))
        .arg(limits.conv_runs_per_min)
        .arg(60)
        .invoke_async(r)
        .await?;
    if ok == 0 {
        breached.push(L1_CONV.to_string());
    }

    // L2 — runs por contato por hora.
    let ok: i64 = Script::new(BUCKET_LUA)
        .key(RateLimits::contact_key(contact_id))
        .arg(limits.contact_runs_per_hour)
        .arg(3600)
        .invoke_async(r)
        .await?;
    if ok == 0 {
        breached.push(L2_CONTACT.to_string());
    }

    // L3 — mensagens de saída por conversa por hora.
    let ok: i64 = Script::new(BUCKET_LUA)
        .key(RateLimits::out_key(conv_id))
        .arg(limits.out_msgs_per_conv_per_hour)
        .arg(3600)
        .invoke_async(r)
        .await?;
    if ok == 0 {
        breached.push(L3_OUT.to_string());
    }

    // L4 — runs por conta por minuto.
    let ok: i64 = Script::new(BUCKET_LUA)
        .key(RateLimits::account_key(account_id))
        .arg(limits.account_runs_per_min)
        .arg(60)
        .invoke_async(r)
        .await?;
    if ok == 0 {
        breached.push(L4_ACCOUNT.to_string());
    }

    // L5 — concorrência global (semáforo).
    let ok: i64 = Script::new(SEMAPHORE_LUA)
        .key(RateLimits::SEMAPHORE_KEY)
        .arg(limits.max_concurrent_runs)
        .arg(15)
        .invoke_async(r)
        .await?;
    if ok == 0 {
        breached.push(L5_CONCURRENCY.to_string());
    }

    // L6 — orçamento diário (freio de emergência financeiro, Seção 6.5).
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let ok: i64 = Script::new(BUDGET_LUA)
        .key(RateLimits::budget_key(&today))
        .arg(limits.daily_budget_usd)
        .invoke_async(r)
        .await?;
    if ok == 0 {
        breached.push(L6_BUDGET.to_string());
    }

    Ok(breached)
}

/// Contabiliza um gasto no orçamento diário (L6). Chamado após um run custear.
/// `INCRBYFLOAT budget:{YYYY-MM-DD} cost`.
pub async fn add_spend(pool: &RedisPool, cost_usd: f64) -> Result<()> {
    let mut conn = pool.get().await?;
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let _: () = redis::cmd("INCRBYFLOAT")
        .arg(RateLimits::budget_key(&today))
        .arg(cost_usd)
        .query_async(&mut *conn)
        .await?;
    Ok(())
}

/// Libera um slot do semáforo L5 ao fim do run (`DECR` com floor em 0).
pub async fn release_concurrency_slot(pool: &RedisPool) -> Result<()> {
    let mut conn = pool.get().await?;
    let n: i64 = redis::cmd("DECR")
        .arg(RateLimits::SEMAPHORE_KEY)
        .query_async(&mut *conn)
        .await?;
    if n < 0 {
        // ponytail: corrige drift negativo (slots liberados demais por TTL expirado cedo).
        let _: () = redis::cmd("SET")
            .arg(RateLimits::SEMAPHORE_KEY)
            .arg(0)
            .query_async(&mut *conn)
            .await?;
    }
    Ok(())
}

/// Conta mensagens de saída já consumidas por conversa na janela atual (L3).
/// Usado pelo Gate de Saída (S11) antes de aprovar o envio.
pub async fn out_msgs_used(pool: &RedisPool, conv_id: ConversationId) -> Result<i64> {
    let mut conn = pool.get().await?;
    let n: Option<i64> = redis::cmd("GET")
        .arg(RateLimits::out_key(conv_id))
        .query_async(&mut *conn)
        .await?;
    Ok(n.unwrap_or(0))
}
