//! `pipeline` — orquestra as etapas do fluxo canônico (Seção 3.2) na ordem
//! fixa da v1.
//!
//! A pipeline é **uma função, não um trait** (regra PONYTAIL): a ordem das
//! etapas é deliberada e fixa na v1. Trocar a ordem exige refactor, e isso é
//! desejável — ninguém reordena por acidente.
//!
//! Ordem:
//! 1. acquire_lock — lock por conversa (6.3)
//! 2. collect_turn — drena o buffer, ordena, coalesce (6.6)
//! 3. build_context — ConversationContext + histórico + AgentRequest
//! 4. inbound_gate — G1–G11 (8.1)
//! 5. ack — ack de latência (9.1)
//! 6. run_agent — provider + fallback (5.7)
//! 7. outbound_gate — S1–S12 (8.2)
//! 8. apply_actions — Action → chamadas C1–C13
//! 9. finalize — atualiza estado, libera lock, métricas

pub mod acquire_lock;
pub mod ack;
pub mod apply_actions;
pub mod build_context;
pub mod collect_turn;
pub mod finalize;
pub mod inbound_gate;
pub mod outbound_gate;
pub mod run_agent;

use std::time::Instant;

use tracing::{info, warn};

use bridge_core::RunId;

use crate::consumer::AgentRunJob;
use crate::state::{
    finish_agent_run, insert_agent_run, record_gate_decision, AppState, WorkerError,
};
use crate::pipeline::acquire_lock::{acquire_lock, release_lock, LockGuard};

/// Executa o pipeline completo para um job.
pub async fn run(state: &AppState, job: AgentRunJob) -> Result<(), WorkerError> {
    let conv_id = job.conversation_id;
    let started = Instant::now();
    let run_id = RunId::new();

    // 1. Lock por conversa. Se não pegar, o acquire_lock re-enfileira com
    //    atraso de 2s e retorna LockContention (não é erro fatal).
    let lock = acquire_lock(&state.redis, conv_id).await?;
    let lock = match lock {
        Some(g) => g,
        None => return Err(WorkerError::LockContention),
    };

    // 2. Coleta o turno: drena o buffer, ordena por created_at, coalesce.
    let turn = collect_turn::drain_buffer(&state.redis, conv_id).await?;
    if turn.messages.is_empty() {
        // ponytail: buffer vazio — job chegou sem mensagens (race com sweeper
        // ou já processado). Libera o lock e sai sem erro.
        release_lock(&state.redis, &lock).await;
        return Ok(());
    }

    // 3. Carrega estado + monta contexto/requisição.
    let conv_state = match crate::state::load_conversation_state(&state.pg, conv_id).await? {
        Some(s) => s,
        None => {
            // Sem linha em conversation_state: conversa não ingerida. Loga,
            // libera o lock, descarta o job. O bridge-api deve ter criado a linha.
            warn!(conv_id, "no conversation_state row; dropping job");
            release_lock(&state.redis, &lock).await;
            return Ok(());
        }
    };

    // 4. Gate de entrada. Bloqueio é fluxo normal (retorna WorkerError::Blocked).
    let decision = inbound_gate::evaluate(&conv_state, &state.config, &turn.messages).await?;
    if !decision.is_allowed() {
        if let inbound_gate::GateDecision::Block { rule, reason } = decision {
            record_gate_decision(
                &state.pg,
                conv_id,
                "inbound",
                &rule,
                "block",
                Some(&serde_json::json!({ "reason": reason })),
            )
            .await
            .ok(); // best-effort: auditoria não derruba o pipeline
            release_lock(&state.redis, &lock).await;
            return Err(WorkerError::Blocked { rule, reason });
        }
    }

    // 5. Monta a requisição ao agente e cria o agent_run.
    let req = build_context::build_agent_request(
        &state.config,
        run_id,
        conv_id,
        &conv_state,
        &turn,
        job.reason.as_deref(),
    );
    insert_agent_run(
        &state.pg,
        run_id,
        conv_id,
        state.agent.id(),
        job.reason.as_deref().unwrap_or("debounce_expired"),
        &turn.message_ids(),
    )
    .await
    .ok(); // best-effort: run sem linha de BD ainda roda

    // 6. (opcional) ack de latência — só sinal, não bloqueia.
    if let Err(e) = ack::maybe_send_ack(state, &conv_state, &turn).await {
        warn!(conv_id, error = %e, "ack failed (non-fatal)");
    }

    // 7. Roda o agente (com fallback). Falha dupla → degradação (9.3).
    let agent_result = run_agent::run_with_fallback(state, req).await;

    let response = match agent_result {
        Ok(r) => r,
        Err(agent_err) => {
            // 9.3 — degradação: cliente nunca fica sem resposta.
            warn!(conv_id, error = %agent_err, "agent failed; degrading");
            apply_actions::degrade_on_failure(state, conv_id, &agent_err.to_string())
                .await?;
            finish_agent_run(
                &state.pg,
                run_id,
                "failed",
                Some(agent_err_kind(&agent_err)),
                None,
                None,
                None,
                Some(started.elapsed().as_millis() as i32),
            )
            .await
            .ok();
            release_lock(&state.redis, &lock).await;
            return Ok(());
        }
    };

    // 8. Gate de saída. Pode modificar a resposta (truncate, redact, drop
    //    actions). Não bloqueia a run — modifica e segue.
    let last_out = outbound_gate::last_outbound_text(&state.redis, conv_id).await.unwrap_or(None);
    let reviewed = outbound_gate::evaluate(&response, &conv_state, &state.config, last_out.as_deref());
    outbound_gate::record(&state.pg, conv_id, &reviewed).await.ok();

    // 9. Aplica ações no Chatwoot (C1–C13).
    apply_actions::apply(state, conv_id, &reviewed).await?;

    // 10. Finaliza: atualiza estado, métricas, libera lock.
    finalize::finalize(
        state,
        conv_id,
        run_id,
        &conv_state,
        &reviewed,
        started.elapsed(),
        &lock,
    )
    .await?;

    info!(conv_id, run_id = %run_id.as_uuid(), elapsed = ?started.elapsed(), "pipeline ok");
    Ok(())
}

/// Classifica o erro do agente para a coluna `agent_run.error_kind`.
fn agent_err_kind(e: &bridge_core::AgentError) -> &'static str {
    use bridge_core::AgentError::*;
    match e {
        Timeout => "timeout",
        RateLimited => "rate_limited",
        AuthError => "auth",
        BudgetExceeded => "budget_exceeded",
        ProviderError(_) => "provider_error",
        InvalidResponse(_) => "invalid_response",
    }
}

// re-export p/ submódulos
pub use inbound_gate::GateDecision;
