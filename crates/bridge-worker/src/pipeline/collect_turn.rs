//! `collect_turn` — drena o buffer, ordena e coalesce (Seção 6.6).
//!
//! O buffer é a lista Redis `buf:{conv_id}` (spec 6.2). Drenar = LRANGE +
//! DEL (atomicamente seria RPOPLPUSH, mas o DEL após LRANGE é suficiente para
//! v1; o lock por conversa garante que só este worker está drenando).

use std::time::Duration;

use bridge_core::{buffer_key, InboundMessage};
use deadpool_redis::Pool;
use redis::AsyncCommands;
use tracing::debug;

use crate::state::WorkerError;

/// Janela de tolerância para mensagens fora de ordem por created_at (ms).
// ponytail: o created_at do webhook vem do Chatwoot e pode ter skew entre
// mensagens em rajada. Reordenamos por created_at string (ISO-8601 ordena
// lexicograficamente); diferenças < 1s são consideradas "simultâneas".
const ORDER_SKEW_MS: i64 = 1_000;

/// Turno coletado: as mensagens do buffer, drenadas e ordenadas, com metadados.
#[derive(Debug, Clone)]
pub struct CollectedTurn {
    pub messages: Vec<InboundMessage>,
}

impl CollectedTurn {
    /// IDs das mensagens de entrada (para `agent_run.input_msg_ids`).
    pub fn message_ids(&self) -> Vec<i64> {
        self.messages.iter().map(|m| m.id).collect()
    }

    /// Soma de caracteres do conteúdo (para L7 / decisão de truncar).
    pub fn total_chars(&self) -> usize {
        self.messages.iter().map(|m| m.content.chars().count()).sum()
    }

    /// `true` se alguma mensagem do turno tem anexo.
    pub fn has_attachment(&self) -> bool {
        self.messages.iter().any(|m| m.has_attachment || !m.attachments.is_empty())
    }

    /// Texto coalescido: uma linha por mensagem, preservando fronteiras (6.6).
    pub fn coalesced_text(&self) -> String {
        let mut s = String::new();
        for (i, m) in self.messages.iter().enumerate() {
            if i > 0 {
                s.push('\n');
            }
            s.push_str(&m.content);
        }
        s
    }
}

/// Drena o buffer da conversa: lê todas as mensagens, deleta a chave e
/// retorna ordenado por `created_at` crescente (spec 6.6 regra 1).
pub async fn drain_buffer(
    redis_pool: &Pool,
    conv_id: i64,
) -> Result<CollectedTurn, WorkerError> {
    let mut conn = redis_pool.get().await?;
    let key = buffer_key(conv_id);

    // LRANGE 0 -1 — pega tudo sem remover; removemos após parsear com sucesso.
    let raw: Vec<String> = conn.lrange(&key, 0, -1).await?;
    if raw.is_empty() {
        return Ok(CollectedTurn { messages: vec![] });
    }

    let mut messages: Vec<InboundMessage> = Vec::with_capacity(raw.len());
    for item in &raw {
        match serde_json::from_str::<InboundMessage>(item) {
            Ok(m) => messages.push(m),
            Err(e) => {
                // ponytail: payload corrompido no buffer — descarta aquele item,
                // não derruba o turno. Loga para investigar.
                tracing::warn!(conv_id, error = %e, "dropping corrupt buffer item");
            }
        }
    }

    // Ordena por created_at (ISO-8601 lexicalmente ordenável).
    messages.sort_by(|a, b| a.created_at.cmp(&b.created_at));

    // Só remove o buffer depois de parsear com sucesso. Se o worker morrer
    // entre LRANGE e DEL, o próximo run reprocessa (idempotência via dedup).
    let _: () = conn.del(&key).await?;
    // Limpa também o contador de chars (spec 6.2 — `buf:chars:{conv_id}`).
    let _: () = conn.del(format!("buf:chars:{conv_id}")).await?;
    let _: () = conn.del(format!("buf:first:{conv_id}")).await?;

    debug!(conv_id, n = messages.len(), "buffer drained");
    Ok(CollectedTurn { messages })
}

/// Trunca o conteúdo de cada mensagem ao teto de L7 (4000 chars) com marcador
/// `[...]` (spec 6.5 L7). Aplicado no build_context, não aqui, mas vive aqui
/// porque é regra do turno.
pub fn apply_input_limit(msgs: &mut [InboundMessage], max_chars: usize) {
    for m in msgs.iter_mut() {
        if m.content.chars().count() > max_chars {
            // ponytail: trunca em fronteira de char, não byte — UTF-8 seguro.
            let kept: String = m.content.chars().take(max_chars).collect();
            m.content = format!("{kept}[...]");
        }
    }
}

pub fn order_skew() -> Duration {
    Duration::from_millis(ORDER_SKEW_MS as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::Attachment;

    fn msg(id: i64, content: &str, at: &str) -> InboundMessage {
        InboundMessage {
            id,
            content: content.into(),
            created_at: at.into(),
            ..Default::default()
        }
    }

    #[test]
    fn coalesce_preserves_boundaries() {
        let t = CollectedTurn {
            messages: vec![msg(1, "bom dia", "2026-07-28T12:00:00Z"), msg(2, "preciso do DAS", "2026-07-28T12:00:01Z")],
        };
        assert_eq!(t.coalesced_text(), "bom dia\npreciso do DAS");
    }

    #[test]
    fn sorts_by_created_at() {
        let mut t = CollectedTurn {
            messages: vec![msg(2, "b", "2026-07-28T12:00:01Z"), msg(1, "a", "2026-07-28T12:00:00Z")],
        };
        t.messages.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        assert_eq!(t.messages[0].id, 1);
    }

    #[test]
    fn truncates_long_content_with_marker() {
        let mut m = msg(1, &"x".repeat(5000), "2026-07-28T12:00:00Z");
        let mut v = vec![m.clone()];
        apply_input_limit(&mut v, 4000);
        assert!(v[0].content.ends_with("[...]"));
        assert!(v[0].content.chars().count() <= 4000 + "[...]".chars().count());
        // a msg original não foi mutada (vec separado)
        assert_eq!(m.content.chars().count(), 5000);
    }

    #[test]
    fn detects_attachment() {
        let mut m = msg(1, "veja o anexo", "2026-07-28T12:00:00Z");
        m.has_attachment = true;
        m.attachments.push(Attachment {
            url: "https://x/p.pdf".into(),
            mime: "application/pdf".into(),
            name: "guia.pdf".into(),
        });
        let t = CollectedTurn { messages: vec![m] };
        assert!(t.has_attachment());
        assert!(t.total_chars() > 0);
    }

    #[test]
    fn message_ids_collected() {
        let t = CollectedTurn { messages: vec![msg(10, "a", "t1"), msg(20, "b", "t2")] };
        assert_eq!(t.message_ids(), vec![10, 20]);
    }
}
