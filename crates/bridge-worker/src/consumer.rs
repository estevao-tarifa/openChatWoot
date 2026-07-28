//! `consumer` — consome `queue:agent_runs` via BRPOP e despacha o pipeline.
//!
//! Ponytail: o consumer faz loop simples com `tokio::spawn` para concorrência.
//! Não há pool sofisticado — cada job vira uma task; o limitador de
//! concorrência global (L5, `sem:agent`) fica no pipeline. A spec exige
//! mínimo 2 réplicas do worker (6.3); o lock por conversa (acquire_lock)
//! garante que só uma processa cada conversa.

use std::time::Duration;

use bridge_core::QUEUE_AGENT_RUNS;
use serde::Deserialize;
use tokio::signal;
use tracing::{error, info, warn};

use crate::state::AppState;
use crate::state::WorkerError;

/// Job enfileirado pelo sweeper / pelo `bridge-api` em `disparar()`.
/// Traz apenas o conversation_id; o resto do contexto é carregado do estado.
#[derive(Debug, Clone, Deserialize)]
pub struct AgentRunJob {
    pub conversation_id: i64,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub trace_id: Option<String>,
}

/// Consome `queue:agent_runs` (BRPOP) e executa o pipeline completo.
///
/// `id` é só para logs (distinguir workers em multi-réplica).
pub async fn run(state: AppState, id: u8) {
    info!(worker_id = id, "consumer started");
    loop {
        match next_job(&state).await {
            Ok(Some(job)) => spawn_pipeline(state.clone(), job),
            Ok(None) => continue, // timeout do BRPOP, sem trabalho
            Err(e) => {
                error!(worker_id = id, error = %e, "BRPOP failed; backing off");
                // Backoff curto: Redis caído não deve virar busy-loop.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// BRPOP com timeout de 5s. Retorna `None` se estourou sem trabalho.
async fn next_job(state: &AppState) -> Result<Option<AgentRunJob>, WorkerError> {
    let mut conn = state.redis.get().await?;
    // BRPOP bloqueia até 5s. Lista vazia → retorna nil (None aqui).
    let res: Option<(String, String)> = redis::cmd("BRPOP")
        .arg(QUEUE_AGENT_RUNS)
        .arg(5) // timeout em segundos
        .query_async(&mut *conn)
        .await?;
    let Some((_key, raw)) = res else {
        return Ok(None);
    };
    let job: AgentRunJob = serde_json::from_str(&raw)
        .map_err(|e| WorkerError::Io(format!("invalid job payload: {e}")))?;
    Ok(Some(job))
}

/// Faz o spawn do pipeline em task própria. Erros dentro do pipeline são
/// logados, não propagados — um job falho não derruba o consumer.
fn spawn_pipeline(state: AppState, job: AgentRunJob) {
    tokio::spawn(async move {
        let conv_id = job.conversation_id;
        let span = tracing::info_span!("agent_run",
            conv_id, trace_id = ?job.trace_id, reason = ?job.reason);
        let _enter = span.enter();
        let started = std::time::Instant::now();
        match crate::pipeline::run(&state, job).await {
            Ok(()) => info!(elapsed = ?started.elapsed(), "run completed"),
            Err(WorkerError::Blocked { rule, reason }) => {
                // Bloqueio de gate é fluxo normal, só debug.
                warn!(rule, reason, "run blocked by gate");
            }
            Err(WorkerError::LockContention) => {
                // Lock não pego: o job já foi re-enfileirado pelo acquire_lock.
                warn!("run skipped (lock contention, re-enqueued)");
            }
            Err(e) => error!(error = %e, "run failed"),
        }
    });
}

/// Aguarda Ctrl+C para shutdown gracioso. Ponytail: sem drain elaborado —
/// jobs em andamento terminam (lock expira em 90s se o worker morrer mid-run,
/// spec 6.3 regra 3). Adicionar drain explícito quando houver fila persistente.
pub async fn shutdown_signal() {
    signal::ctrl_c()
        .await
        .expect("failed to install ctrl+c handler");
    info!("ctrl+c received; shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_job() {
        let j: AgentRunJob = serde_json::from_str(r#"{"conversation_id":523}"#).unwrap();
        assert_eq!(j.conversation_id, 523);
        assert!(j.reason.is_none());
    }

    #[test]
    fn parses_job_with_reason() {
        let j: AgentRunJob =
            serde_json::from_str(r#"{"conversation_id":523,"reason":"max_messages"}"#).unwrap();
        assert_eq!(j.reason.as_deref(), Some("max_messages"));
    }

    #[test]
    fn rejects_job_without_conversation_id() {
        assert!(serde_json::from_str::<AgentRunJob>(r#"{"reason":"x"}"#).is_err());
    }
}
