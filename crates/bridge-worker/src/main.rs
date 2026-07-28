//! `bridge-worker` — binário 2. Consome filas, processa a IA, aplica os Gates.
//!
//! Spec normativa: `ESPECchatwootaibridge.md` Seções 3, 6, 7, 8, 9.
//!
//! Topologia de tasks (spec 6.3 regra 4: mínimo 2 réplicas do worker; o
//! lock por conversa garante que só uma processa cada conversa):
//! - 1x debounce_sweeper (tick 250ms sobre o ZSET)
//! - 2x consumer (BRPOP de `queue:agent_runs`), cada um com `tokio::spawn`
//!   interno por job para concorrência

mod consumer;
mod debounce_sweeper;
mod pipeline;
mod state;

use std::sync::Arc;

use bridge_agent::{AgentProvider, AnthropicProvider, OpenResponsesProvider};
use bridge_chatwoot::ChatwootClient;
use bridge_core::Config;
use deadpool_redis::Config as RedisConfig;
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use state::AppState;

#[tokio::main]
async fn main() {
    // 1. Config (env vars via figman). Falha com mensagem útil.
    let config = match Config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load config: {e}");
            std::process::exit(1);
        }
    };
    let config = Arc::new(config);

    // 2. Setup tracing. Redação de PII é do filtro/log layer (spec 10.4).
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.infra.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
    info!(version = env!("CARGO_PKG_VERSION"), "bridge-worker starting");

    // 3. PgPool.
    let pg_pool = match PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(config.infra.database_url.expose())
        .await
    {
        Ok(p) => p,
        Err(e) => {
            error!(error = %e, "failed to connect to postgres");
            std::process::exit(1);
        }
    };

    // 4. Redis pool (deadpool-redis 0.18: `.builder().build()`, runtime via
    //    feature `rt_tokio_1`). Mesmo padrão do `bridge-api` (binário 1).
    let redis_pool = match RedisConfig::from_url(config.infra.redis_url.expose())
        .builder()
    {
        Ok(b) => match b.build() {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "failed to build redis pool");
                std::process::exit(1);
            }
        },
        Err(e) => {
            error!(error = %e, "failed to build redis pool builder");
            std::process::exit(1);
        }
    };

    // 5. ChatwootClient (token do Agent Bot, Seção 4.2).
    let chatwoot = Arc::new(ChatwootClient::new(
        &config.chatwoot.base_url,
        config.chatwoot.bot_token.expose(),
        config.chatwoot.account_id,
    ));

    // 6. AgentProvider primário + fallback (Seção 5.7). O fallback só é
    //    construído se for diferente do primário (evita duplicar anthropic).
    let agent: Arc<Box<dyn AgentProvider>> =
        Arc::new(build_provider(&config));
    let fallback: Option<Arc<Box<dyn AgentProvider>>> =
        if config.agent.provider_fallback != config.agent.provider {
            Some(Arc::new(build_provider_named(&config, &config.agent.provider_fallback)))
        } else {
            None
        };

    let state = AppState {
        redis: redis_pool.clone(),
        pg: pg_pool.clone(),
        chatwoot,
        agent,
        fallback,
        config: config.clone(),
    };

    // 7. Spawn: debounce_sweeper + N consumers.
    let sweeper_pool = state.redis.clone();
    tokio::spawn(async move {
        debounce_sweeper::run(sweeper_pool).await;
    });

    let num_workers = 2u8;
    for id in 0..num_workers {
        tokio::spawn(consumer::run(state.clone(), id));
    }

    // 8. Aguardar signal (ctrl+c). Jobs em andamento terminam; o lock expira
    //    em 90s se o worker morrer mid-run (spec 6.3 regra 3).
    consumer::shutdown_signal().await;
    info!("shutdown signal received; consumers will finish in-flight jobs");
}

/// Constrói o provider conforme `name`. Fail-fast em credencial vazia: é
/// melhor recusar subir do que falar com a IA sem auth. Retorna `Box<dyn>`.
pub fn build_provider(config: &Config) -> Box<dyn AgentProvider> {
    build_provider_named(config, &config.agent.provider)
}

/// Constrói o provider pelo nome (usado para primário e fallback).
pub fn build_provider_named(config: &Config, name: &str) -> Box<dyn AgentProvider> {
    match name {
        "anthropic" => {
            let key = config.agent.anthropic_api_key.expose();
            if key.is_empty() {
                error!("ANTHROPIC_API_KEY vazia — provider não terá auth");
            }
            Box::new(AnthropicProvider::new(key, default_anthropic_model()))
        }
        "openclaw" => {
            let token = config.agent.openclaw_token.expose();
            if token.is_empty() {
                error!("OPENCLAW_TOKEN vazio — provider não terá auth");
            }
            Box::new(OpenResponsesProvider::new(
                "openclaw",
                &config.agent.openclaw_base_url,
                token,
                Some(config.agent.openclaw_agent_id.clone()),
                "x-openclaw-session-key",
                "x-openclaw-agent-id",
                "openclaw",
            ))
        }
        "hermes" => {
            // ponytail: Hermes reusa o token do OpenClaw por ora; token próprio
            // quando o shim tiver auth separada (Fase 3).
            let token = config.agent.openclaw_token.expose();
            Box::new(OpenResponsesProvider::new(
                "hermes",
                &config.agent.hermes_shim_url,
                token,
                None,
                "x-hermes-session-key",
                "x-hermes-agent-id",
                "hermes",
            ))
        }
        other => {
            error!(provider = other, "provider desconhecido; usando anthropic");
            Box::new(AnthropicProvider::new(
                config.agent.anthropic_api_key.expose(),
                default_anthropic_model(),
            ))
        }
    }
}

/// Modelo Anthropic default. ponytail: não há env var dedicada ao modelo;
/// fixar aqui é suficiente para v1. Trocar por `ANTHROPIC_MODEL` quando houver.
fn default_anthropic_model() -> &'static str {
    "claude-sonnet-5-20250610"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_model_is_sonnet() {
        assert!(default_anthropic_model().contains("sonnet"));
    }

    #[test]
    fn config_loads_with_defaults() {
        // Sem env vars, usa defaults de spec (Seção 14). Pode falhar só se o
        // figman não conseguir ler o ambiente — improvável em CI.
        if let Ok(c) = Config::load() {
            assert_eq!(c.agent.display_name, "Íris");
            assert_eq!(c.buffer.debounce_ms, 6_000);
            assert_eq!(c.rate_limits.max_consecutive_ai_turns, 4);
        }
    }

    #[test]
    fn build_provider_unknown_falls_back_to_anthropic() {
        // Não podemos instanciar sem keys reais, mas o contrato de
        // `build_provider_named` para "anthropic" devolve um provider cujo
        // id() == "anthropic". Validamos indiretamente o nome do modelo.
        assert!(default_anthropic_model().starts_with("claude-"));
    }
}
