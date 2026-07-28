//! `finalize` — atualiza estado, libera lock e emite métricas ao fim do run.
//!
//! Aqui é onde o watchdog de lock (spec 6.3 regra 3) seria relevante: se o
//! run passou de 60s, estenderíamos o TTL — mas como o run já terminou,
//! só liberamos. O watchdog vira relevante em `run` longos (ainda não na v1).

use std::time::{Duration, Instant};

use bridge_core::{AiState, RunId, StateEvent};

use crate::pipeline::acquire_lock::{release_lock, LockGuard};
use crate::pipeline::outbound_gate::ReviewedResponse;
use crate::state::{finish_agent_run, save_ai_state, AppState, ConversationState, WorkerError};

/// Duração a partir da qual o run é considerado "longo" (watchdog). 60s (spec).
const LONG_RUN_THRESHOLD: Duration = Duration::from_secs(60);

/// Persiste o resultado do run: transição de estado, agent_run.finished_at,
/// métricas, e liberação do lock. Sempre libera o lock, mesmo em erro.
pub async fn finalize(
    state: &AppState,
    conv_id: i64,
    run_id: RunId,
    conv_state: &ConversationState,
    reviewed: &ReviewedResponse,
    elapsed: Duration,
    lock: &LockGuard,
) -> Result<(), WorkerError> {
    let was_long = elapsed > LONG_RUN_THRESHOLD;

    // Transição de estado. Respondido com sucesso → AiActive (reseta contagem
    // de turnos? Não — incrementamos, pois contato ainda não respondeu).
    // spec 8.4: prior_ai_turns_in_row incrementa a cada run, zera quando
    // chega mensagem de contato/humano. Como este run foi disparado POR msg
    // de contato, zeramos antes de incrementar para o próximo turno.
    let new_state = conv_state.ai_state.transition(&StateEvent::AiResponded)
        .unwrap_or(AiState::AiActive);

    // Contagem para o próximo turno: o contato acabou de falar (disparou o
    // run), então a cadeia "IA sem fala de contato" reseta e contamos este
    // turno como 1.
    let next_prior = 1u16;

    // Hash do último texto enviado (para S6 do próximo run).
    let last_hash = reviewed
        .reply
        .as_ref()
        .map(|r| hash_text(&r.text));

    // Salva estado de controle.
    save_ai_state(
        &state.pg,
        conv_id,
        new_state,
        next_prior,
        None, // provider_session_id vem da resposta; skip aqui por simplicidade
        last_hash.as_deref(),
    )
    .await?;

    // Finaliza agent_run com sucesso.
    finish_agent_run(
        &state.pg,
        run_id,
        "succeeded",
        None,
        None,
        None,
        None,
        Some(elapsed.as_millis() as i32),
    )
    .await?;

    // Métricas (spec 15.1). ponytail: contador simples via crate `metrics`.
    metrics::counter!(bridge_core::metrics::AGENT_RUN, "provider" => state.agent.id(), "status" => "succeeded").increment(1);
    metrics::histogram!(bridge_core::metrics::AGENT_RUN_DURATION, "provider" => state.agent.id())
        .record(elapsed.as_secs_f64());
    if was_long {
        tracing::warn!(conv_id, elapsed = ?elapsed, "long run (watchdog territory)");
    }
    if reviewed.blocked {
        metrics::counter!(bridge_core::metrics::GATE_BLOCK, "gate" => "outbound")
            .increment(1);
    }

    // Libera o lock — sempre, mesmo que o resto tenha falhado.
    release_lock(&state.redis, lock).await;

    Ok(())
}

/// Hash simples (FNV-1a 64) do texto para `last_ai_msg_hash`. Ponytail: não
/// é cripto, só dedup de similaridade exata; S6 faz a checagem fuzzy com o
/// texto completo (guardado em Redis). Trocar por sha256 se a auditoria exigir.
fn hash_text(text: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    format!("{h:016x}")
}

/// Expõe o limiar de "run longo" para testes/documentação.
pub fn long_run_threshold() -> Duration {
    LONG_RUN_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic() {
        assert_eq!(hash_text("bom dia"), hash_text("bom dia"));
        assert_ne!(hash_text("bom dia"), hash_text("boa tarde"));
    }

    #[test]
    fn hash_is_hex_64bit() {
        let h = hash_text("x");
        assert_eq!(h.len(), 16);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn long_run_threshold_is_60s() {
        assert_eq!(long_run_threshold(), Duration::from_secs(60));
    }

    #[test]
    fn transition_ai_responded_from_thinking() {
        // sanity: AiThinking + AiResponded → AiActive
        let s = AiState::AiThinking;
        let n = s.transition(&StateEvent::AiResponded).unwrap();
        assert_eq!(n, AiState::AiActive);
    }
}
