//! `run_agent` — chama o provider de IA com fallback (Seção 5.7) e checa o
//! orçamento diário (L6) antes de qualquer chamada.
//!
//! Regra (spec 5.7): falha do primário → tenta fallback UMA vez → se falhar
//! também, o pipeline roteia para a degradação (9.3). Nunca silêncio.

use bridge_agent::run_with_fallback as bridge_run_with_fallback;
use bridge_core::{AgentError, AgentRequest, AgentResponse};

use crate::state::{spent_today, AppState};

/// Roda o agente. Checa L6 (orçamento) primeiro; se estourado, devolve
/// `BudgetExceeded` para o pipeline acionar a degradação.
pub async fn run_with_fallback(
    state: &AppState,
    req: AgentRequest,
) -> Result<AgentResponse, AgentError> {
    // L6 — orçamento diário (freio de emergência financeiro, spec 6.5).
    if let Ok(spent) = spent_today(&state.pg).await {
        let budget = state.config.rate_limits.daily_budget_usd;
        if spent >= budget {
            tracing::error!(spent, budget, "daily budget exceeded — AI disabled");
            return Err(AgentError::BudgetExceeded);
        }
        // Alerta em 80% (spec 15.2). Não bloqueia, só loga.
        if spent >= budget * 0.8 {
            tracing::warn!(spent, budget, "daily budget at 80%");
        }
    }

    let primary = state.agent.as_ref().as_ref();
    match &state.fallback {
        Some(fb) => {
            let fb = fb.as_ref().as_ref();
            bridge_run_with_fallback(primary, fb, req).await
        }
        None => primary.run(req).await,
    }
}

/// `true` se o erro do agente é transitivo (candidato a fallback/degradação).
/// Usado pelo pipeline para classificar o `agent_run.error_kind`.
pub fn is_transient(err: &AgentError) -> bool {
    matches!(
        err,
        AgentError::Timeout | AgentError::ProviderError(_) | AgentError::RateLimited
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::AgentError;

    #[test]
    fn classifies_transient() {
        assert!(is_transient(&AgentError::Timeout));
        assert!(is_transient(&AgentError::ProviderError("x".into())));
        assert!(is_transient(&AgentError::RateLimited));
        assert!(!is_transient(&AgentError::AuthError));
        assert!(!is_transient(&AgentError::BudgetExceeded));
        assert!(!is_transient(&AgentError::InvalidResponse("x".into())));
    }
}
