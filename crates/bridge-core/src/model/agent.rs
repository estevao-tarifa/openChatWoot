use crate::ids::RunId;
use crate::model::context::ConversationContext;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// Conjunto fechado de ações que a IA pode solicitar (Seção 5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    SendMessage,
    SendPrivateNote,
    AddLabels,
    RemoveLabels,
    SetCustomAttributes,
    AssignTeam,
    AssignAgent,
    SetPriority,
    SetStatus,
    Snooze,
    CallTool,
    CallAgent,
    RequestHandoff,
}

impl ActionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SendMessage => "send_message",
            Self::SendPrivateNote => "send_private_note",
            Self::AddLabels => "add_labels",
            Self::RemoveLabels => "remove_labels",
            Self::SetCustomAttributes => "set_custom_attributes",
            Self::AssignTeam => "assign_team",
            Self::AssignAgent => "assign_agent",
            Self::SetPriority => "set_priority",
            Self::SetStatus => "set_status",
            Self::Snooze => "snooze",
            Self::CallTool => "call_tool",
            Self::CallAgent => "call_agent",
            Self::RequestHandoff => "request_handoff",
        }
    }
    /// Ações permitidas para agente especialista (depth > 0).
    pub fn allowed_for_specialist(&self) -> bool {
        matches!(self, Self::AddLabels | Self::RemoveLabels | Self::SetCustomAttributes | Self::CallTool)
    }
}

impl fmt::Display for ActionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ActionKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "send_message" => Ok(Self::SendMessage),
            "send_private_note" => Ok(Self::SendPrivateNote),
            "add_labels" => Ok(Self::AddLabels),
            "remove_labels" => Ok(Self::RemoveLabels),
            "set_custom_attributes" => Ok(Self::SetCustomAttributes),
            "assign_team" => Ok(Self::AssignTeam),
            "assign_agent" => Ok(Self::AssignAgent),
            "set_priority" => Ok(Self::SetPriority),
            "set_status" => Ok(Self::SetStatus),
            "snooze" => Ok(Self::Snooze),
            "call_tool" => Ok(Self::CallTool),
            "call_agent" => Ok(Self::CallAgent),
            "request_handoff" => Ok(Self::RequestHandoff),
            _ => Err(format!("unknown ActionKind: {s}")),
        }
    }
}

/// Ação da IA (plana: cada variante usa o campo correspondente).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Action {
    pub kind: ActionKind,
    pub labels: Vec<String>,
    pub attributes: serde_json::Value,
    pub team_id: Option<i64>,
    pub agent_id: Option<String>,
    pub priority: Option<String>,
    pub status: Option<String>,
    pub snoozed_until: Option<i64>,
    pub tool: Option<String>,
    pub arguments: serde_json::Value,
    pub target: Option<String>,
    pub task: Option<String>,
    pub payload: serde_json::Value,
    pub reason: Option<String>,
    pub depth: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply { pub text: String, pub content_type: Option<String> }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffInfo { pub required: bool, pub reason: Option<String>, pub target: Option<String> }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage { pub input_tokens: u32, pub output_tokens: u32, pub cost_usd: f64 }

/// Envelope da resposta da IA (Seção 5.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentResponse {
    pub run_id: Option<String>,
    pub reply: Option<Reply>,
    pub actions: Vec<Action>,
    pub handoff: HandoffInfo,
    pub confidence: f64,
    pub usage: Option<Usage>,
    pub provider_session_id: Option<String>,
    pub result: Option<serde_json::Value>,
    pub summary_for_supervisor: Option<String>,
}

/// Requisição para o provider de IA (Seção 5.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRequest {
    pub run_id: RunId,
    pub session_key: String,
    pub agent_id: Option<String>,
    pub turn: Vec<InboundMessage>,
    pub context: ConversationContext,
    pub allowed_actions: Vec<ActionKind>,
    pub deadline_ms: u64,
    pub max_output_chars: usize,
    pub locale: String,
}

impl AgentRequest {
    pub fn deadline(&self) -> Duration { Duration::from_millis(self.deadline_ms) }
}

/// Mensagem de entrada do buffer (Seção 6).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InboundMessage {
    pub id: i64,
    pub content: String,
    pub sender_kind: String,
    pub created_at: String,
    pub has_attachment: bool,
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment { pub url: String, pub mime: String, pub name: String }

impl Default for InboundMessage {
    fn default() -> Self {
        Self {
            id: 0,
            content: String::new(),
            sender_kind: String::new(),
            created_at: String::new(),
            has_attachment: false,
            attachments: vec![],
        }
    }
}

impl Default for Action {
    fn default() -> Self {
        Self {
            kind: ActionKind::SendMessage,
            labels: vec![],
            attributes: serde_json::Value::Null,
            team_id: None,
            agent_id: None,
            priority: None,
            status: None,
            snoozed_until: None,
            tool: None,
            arguments: serde_json::Value::Null,
            target: None,
            task: None,
            payload: serde_json::Value::Null,
            reason: None,
            depth: 0,
        }
    }
}

impl Default for AgentResponse {
    fn default() -> Self {
        Self {
            run_id: None,
            reply: None,
            actions: vec![],
            handoff: HandoffInfo::default(),
            confidence: 1.0,
            usage: None,
            provider_session_id: None,
            result: None,
            summary_for_supervisor: None,
        }
    }
}

impl Default for Reply {
    fn default() -> Self { Self { text: String::new(), content_type: None } }
}

impl Default for HandoffInfo {
    fn default() -> Self { Self { required: false, reason: None, target: None } }
}

impl Default for Usage {
    fn default() -> Self { Self { input_tokens: 0, output_tokens: 0, cost_usd: 0.0 } }
}

impl Default for Attachment {
    fn default() -> Self { Self { url: String::new(), mime: String::new(), name: String::new() } }
}

impl Default for AgentRequest {
    fn default() -> Self {
        Self {
            run_id: RunId::default(),
            session_key: String::new(),
            agent_id: None,
            turn: vec![],
            context: ConversationContext::default(),
            allowed_actions: vec![],
            deadline_ms: 30_000,
            max_output_chars: 1200,
            locale: "pt-BR".to_string(),
        }
    }
}
