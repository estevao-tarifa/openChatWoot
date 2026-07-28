//! CRUD de `conversation_state` (estado de controle por conversa, Seção 13).
use crate::pg::PgPool;
use crate::Result;
use bridge_core::{AccountId, ContactId, ConversationId, InboxId};
use chrono::{DateTime, Utc};
use sqlx::FromRow;

/// Espelha a tabela `conversation_state`.
#[derive(Debug, Clone, FromRow)]
pub struct ConversationRow {
    pub conversation_id: ConversationId,
    pub account_id: AccountId,
    pub inbox_id: InboxId,
    pub contact_id: ContactId,
    pub channel: String,
    pub ai_state: String,
    pub chatwoot_status: String,
    pub assignee_id: Option<i64>,
    pub team_id: Option<i64>,
    pub labels: Vec<String>,
    pub provider_session_id: Option<String>,
    pub prior_ai_turns_in_row: i16,
    pub last_contact_msg_at: Option<DateTime<Utc>>,
    pub last_human_msg_at: Option<DateTime<Utc>>,
    pub last_ai_msg_at: Option<DateTime<Utc>>,
    pub last_ai_msg_hash: Option<String>,
    pub paused_until: Option<DateTime<Utc>>,
    pub pause_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ConversationRow {
    /// Helper para montar a partir de dados de webhook (defaults sensatos).
    // ponytail: construtor manual em vez de builder — poucos campos obrigatórios.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conversation_id: ConversationId,
        account_id: AccountId,
        inbox_id: InboxId,
        contact_id: ContactId,
        channel: String,
        chatwoot_status: String,
    ) -> Self {
        Self {
            conversation_id,
            account_id,
            inbox_id,
            contact_id,
            channel,
            ai_state: "ai_active".into(),
            chatwoot_status,
            assignee_id: None,
            team_id: None,
            labels: vec![],
            provider_session_id: None,
            prior_ai_turns_in_row: 0,
            last_contact_msg_at: None,
            last_human_msg_at: None,
            last_ai_msg_at: None,
            last_ai_msg_hash: None,
            paused_until: None,
            pause_reason: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

/// Upsert: insere ou atualiza estado de controle. `updated_at` sempre = now().
// ponytail: SQL raw com `query_as`/`query` — nada de query builder (regra PONYTAIL).
pub async fn upsert(pool: &PgPool, row: &ConversationRow) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO conversation_state
          (conversation_id, account_id, inbox_id, contact_id, channel, ai_state,
           chatwoot_status, assignee_id, team_id, labels, provider_session_id,
           prior_ai_turns_in_row, last_contact_msg_at, last_human_msg_at,
           last_ai_msg_at, last_ai_msg_hash, paused_until, pause_reason,
           created_at, updated_at)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
        ON CONFLICT (conversation_id) DO UPDATE SET
          account_id         = EXCLUDED.account_id,
          inbox_id           = EXCLUDED.inbox_id,
          contact_id         = EXCLUDED.contact_id,
          channel            = EXCLUDED.channel,
          ai_state           = EXCLUDED.ai_state,
          chatwoot_status    = EXCLUDED.chatwoot_status,
          assignee_id        = EXCLUDED.assignee_id,
          team_id            = EXCLUDED.team_id,
          labels             = EXCLUDED.labels,
          provider_session_id= EXCLUDED.provider_session_id,
          prior_ai_turns_in_row = EXCLUDED.prior_ai_turns_in_row,
          last_contact_msg_at  = EXCLUDED.last_contact_msg_at,
          last_human_msg_at    = EXCLUDED.last_human_msg_at,
          last_ai_msg_at       = EXCLUDED.last_ai_msg_at,
          last_ai_msg_hash     = EXCLUDED.last_ai_msg_hash,
          paused_until         = EXCLUDED.paused_until,
          pause_reason         = EXCLUDED.pause_reason,
          updated_at           = now()
        "#,
    )
    .bind(row.conversation_id)
    .bind(row.account_id)
    .bind(row.inbox_id)
    .bind(row.contact_id)
    .bind(&row.channel)
    .bind(&row.ai_state)
    .bind(&row.chatwoot_status)
    .bind(row.assignee_id)
    .bind(row.team_id)
    .bind(&row.labels)
    .bind(&row.provider_session_id)
    .bind(row.prior_ai_turns_in_row)
    .bind(row.last_contact_msg_at)
    .bind(row.last_human_msg_at)
    .bind(row.last_ai_msg_at)
    .bind(&row.last_ai_msg_hash)
    .bind(row.paused_until)
    .bind(&row.pause_reason)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Busca estado por `conversation_id`. `None` se não existir.
pub async fn get(pool: &PgPool, conv_id: ConversationId) -> Result<Option<ConversationRow>> {
    let row = sqlx::query_as::<_, ConversationRow>(
        "SELECT * FROM conversation_state WHERE conversation_id = $1",
    )
    .bind(conv_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Atualização parcial de campos de controle usados pela máquina de estados.
/// `labels`/`meta` (_meta_ não existe nesta tabela — ignorado).
#[allow(clippy::too_many_arguments)]
pub async fn update_state(
    pool: &PgPool,
    conv_id: ConversationId,
    ai_state: &str,
    chatwoot_status: Option<&str>,
    assignee_id: Option<i64>,
    team_id: Option<i64>,
    labels: Option<&[String]>,
    provider_session_id: Option<&str>,
    prior_ai_turns_in_row: Option<i16>,
    paused_until: Option<DateTime<Utc>>,
    pause_reason: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE conversation_state SET
          ai_state              = COALESCE($2, ai_state),
          chatwoot_status       = COALESCE($3, chatwoot_status),
          assignee_id           = COALESCE($4, assignee_id),
          team_id               = COALESCE($5, team_id),
          labels                = COALESCE($6, labels),
          provider_session_id   = COALESCE($7, provider_session_id),
          prior_ai_turns_in_row = COALESCE($8, prior_ai_turns_in_row),
          paused_until          = COALESCE($9, paused_until),
          pause_reason          = COALESCE($10, pause_reason),
          updated_at            = now()
        WHERE conversation_id = $1
        "#,
    )
    .bind(conv_id)
    .bind(ai_state)
    .bind(chatwoot_status)
    .bind(assignee_id)
    .bind(team_id)
    .bind(labels)
    .bind(provider_session_id)
    .bind(prior_ai_turns_in_row)
    .bind(paused_until)
    .bind(pause_reason)
    .execute(pool)
    .await?;
    Ok(())
}

/// Stamp de timestamp da última mensagem de cada tipo (usado por SLA/loop guard).
/// `ai_msg_hash` só é usado quando `kind == Ai` (Guard anti-loop, S6).
pub async fn touch_msg_at(
    pool: &PgPool,
    conv_id: ConversationId,
    kind: MsgAt,
    at: DateTime<Utc>,
    ai_msg_hash: Option<&str>,
) -> Result<()> {
    // ponytail: três queries separadas em vez de SQL dinâmico — o tipo do
    // `Query` builder muda com o nº de binds, então um único caminho com
    // bind condicional não tipa. Três constantes é mais legível que macro.
    match kind {
        MsgAt::Contact => {
            sqlx::query(
                "UPDATE conversation_state SET last_contact_msg_at=$2, updated_at=now() WHERE conversation_id=$1",
            )
            .bind(conv_id)
            .bind(at)
            .execute(pool)
            .await?;
        }
        MsgAt::Human => {
            sqlx::query(
                "UPDATE conversation_state SET last_human_msg_at=$2, updated_at=now() WHERE conversation_id=$1",
            )
            .bind(conv_id)
            .bind(at)
            .execute(pool)
            .await?;
        }
        MsgAt::Ai => {
            sqlx::query(
                "UPDATE conversation_state SET last_ai_msg_at=$2, last_ai_msg_hash=$3, updated_at=now() WHERE conversation_id=$1",
            )
            .bind(conv_id)
            .bind(at)
            .bind(ai_msg_hash)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

pub enum MsgAt {
    Contact,
    Human,
    Ai,
}
