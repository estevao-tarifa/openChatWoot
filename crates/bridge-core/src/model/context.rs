use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use crate::ids::ContactId;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ContactSummary {
    pub id: ContactId,
    pub name: String,
    pub phone_masked: String,
    pub email_masked: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ClientSummary {
    pub razao_social: String,
    pub cnpj: String,
    pub regime: String,
    pub ultimo_atendimento: Option<String>,
    pub pendencias: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryItem {
    pub role: String,
    pub content: String,
    pub at: String,
}

impl Default for HistoryItem {
    fn default() -> Self { Self { role: String::new(), content: String::new(), at: String::new() } }
}

/// Horário comercial (Seção 5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusinessHoursState {
    /// Dentro do horário comercial.
    Within,
    /// Fora do horário comercial.
    Outside,
    /// Feriado / recesso (relógio de SLA não corre).
    Holiday,
}

impl BusinessHoursState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Within => "within",
            Self::Outside => "outside",
            Self::Holiday => "holiday",
        }
    }
}

impl fmt::Display for BusinessHoursState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BusinessHoursState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "within" => Ok(Self::Within),
            "outside" => Ok(Self::Outside),
            "holiday" => Ok(Self::Holiday),
            _ => Err(format!("unknown BusinessHoursState: {s}")),
        }
    }
}

impl Default for BusinessHoursState {
    fn default() -> Self { Self::Within }
}

/// Contexto completo da conversa enviado à IA (Seção 5.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConversationContext {
    pub conversation_id: i64,
    pub inbox_channel: String,
    pub contact: Option<ContactSummary>,
    pub client: Option<ClientSummary>,
    pub labels: Vec<String>,
    pub assignee: Option<AgentSummary>,
    pub business_hours: BusinessHoursState,
    pub history_digest: Vec<HistoryItem>,
    pub prior_ai_turns_in_row: u8,
}

impl Default for ConversationContext {
    fn default() -> Self {
        Self {
            conversation_id: 0,
            inbox_channel: String::new(),
            contact: None,
            client: None,
            labels: vec![],
            assignee: None,
            business_hours: BusinessHoursState::default(),
            history_digest: vec![],
            prior_ai_turns_in_row: 0,
        }
    }
}

/// Resumo breve de um atendente (usado em ConversationContext.assignee).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentSummary {
    pub id: i64,
    pub name: String,
    pub email: String,
}
