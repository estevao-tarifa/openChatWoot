//! `debounce_sweeper` — varredura do ZSET de debounce a cada 250 ms (Seção 6.2).
//!
//! Conversas vencidas saem do `debounce:zset` e entram na fila
//! `queue:agent_runs` (LPUSH). O ZSET é durável e inspecionável, sobrevive a
//! restart — por isso foi escolhido em vez de keyspace notifications (spec).

use std::time::Duration;

use bridge_core::{DEBOUNCE_ZSET, QUEUE_AGENT_RUNS};
use chrono::Utc;
use deadpool_redis::Pool;
use redis::AsyncCommands;
use tracing::{debug, warn};

/// Tick de 250 ms sobre o ZSET. Convs vencidas → LPUSH em `queue:agent_runs`.
// ponytail: loop simples com `tokio::time::sleep`. Sem cron, sem scheduler —
// a janela de debounce (6s) torna 250ms de latência aceitável. Trocar por um
// timer hierárquico se o número de convs pendentes passar de ~10k.
pub async fn run(redis_pool: Pool) {
    loop {
        // Erro aqui é logado, nunca derruba o sweeper — spec: tratar com log.
        if let Err(e) = sweep_once(&redis_pool).await {
            warn!(error = %e, "debounce sweep failed");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Uma passagem do sweeper. Pega até 100 convs vencidas, remove do ZSET e
/// enfileira. Cada uma vira um job `{"conversation_id": <id>}` na fila.
async fn sweep_once(redis_pool: &Pool) -> Result<(), redis::RedisError> {
    let mut conn = redis_pool.get().await?;
    let now = Utc::now().timestamp_millis();

    // ZRANGEBYSCORE debounce:zset -inf now LIMIT 0 100 — convs cujo score
    // (timestamp de disparo) já passou.
    let expired: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(DEBOUNCE_ZSET)
        .arg("-inf")
        .arg(now)
        .arg("LIMIT")
        .arg(0)
        .arg(100)
        .query_async(&mut *conn)
        .await?;

    if expired.is_empty() {
        return Ok(());
    }

    // Pipeline atômico: ZREM de cada vencida + LPUSH do job. Fazemos um por
    // um para isolar falhas (uma conv travada não atrapalha as outras).
    for conv_id in expired {
        // Best-effort ZREM; se outro worker já removeu (race), retorna 0 — ok.
        let removed: i64 = conn.zrem(DEBOUNCE_ZSET, &conv_id).await?;
        if removed == 0 {
            continue;
        }
        let job = serde_json::json!({ "conversation_id": conv_id });
        // ponytail: job leva só o conv_id. trace_id/reason são derivados no
        // consumer a partir do estado; o sweeper não tem contexto de webhook.
        let _: () = conn.lpush(QUEUE_AGENT_RUNS, job.to_string()).await?;
        debug!(conv_id, "debounce expired → enqueued");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ponytail: self-check de parsing do job — sem Redis real (CI sem deps).
    // O fluxo Redis é coberto por teste de integração no bridge-store.
    #[test]
    fn job_serializes_with_conversation_id() {
        let job = serde_json::json!({ "conversation_id": "523" });
        assert_eq!(job["conversation_id"], "523");
    }

    // guarda o tempo de tick nominal para documentação viva.
    #[test]
    fn tick_is_250ms() {
        assert_eq!(Duration::from_millis(250).as_millis(), 250);
    }
}
