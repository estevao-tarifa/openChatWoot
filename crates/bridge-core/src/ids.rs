use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Identificador de conversa no Chatwoot.
pub type ConversationId = i64;
/// Identificador de conta no Chatwoot.
pub type AccountId = i64;
/// Identificador de contato no Chatwoot.
pub type ContactId = i64;
/// Identificador de inbox no Chatwoot.
pub type InboxId = i64;

/// Identificador de uma execução do agente.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub Uuid);

impl RunId {
    pub fn new() -> Self {
        // ponytail: v4 por padrão; trocar por Uuid::now_v7() quando estável
        Self(Uuid::new_v4())
    }
    pub fn as_uuid(&self) -> Uuid { self.0 }
}

impl Default for RunId {
    fn default() -> Self { Self::new() }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
