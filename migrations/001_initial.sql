-- 001_initial.sql
-- Tabelas do chatwoot-ai-bridge (Seção 13 da ESPEC)
-- -- ponytail: schema completo em uma migração; separar quando houver mais de ~3 migrations vivas

-- Extensão para cifragem de PII (content_enc em message_log)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Estado de controle por conversa (uma linha por conversa)
CREATE TABLE conversation_state (
    conversation_id       BIGINT PRIMARY KEY,
    account_id            BIGINT NOT NULL,
    inbox_id              BIGINT NOT NULL,
    contact_id            BIGINT NOT NULL,
    channel               TEXT   NOT NULL,
    ai_state              TEXT   NOT NULL DEFAULT 'ai_active',
    chatwoot_status       TEXT   NOT NULL,
    assignee_id           BIGINT,
    team_id               BIGINT,
    labels                TEXT[] NOT NULL DEFAULT '{}',
    provider_session_id   TEXT,
    prior_ai_turns_in_row SMALLINT NOT NULL DEFAULT 0,
    last_contact_msg_at   TIMESTAMPTZ,
    last_human_msg_at     TIMESTAMPTZ,
    last_ai_msg_at        TIMESTAMPTZ,
    last_ai_msg_hash      TEXT,
    paused_until          TIMESTAMPTZ,
    pause_reason          TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_conversation_state_state_updated ON conversation_state (ai_state, updated_at);
CREATE INDEX idx_conversation_state_assignee_awaiting ON conversation_state (assignee_id) WHERE ai_state = 'awaiting_human';

-- Log de mensagens (metadados; conteúdo cifrado, sujeito a retenção)
CREATE TABLE message_log (
    id               BIGSERIAL PRIMARY KEY,
    chatwoot_msg_id  BIGINT NOT NULL,
    conversation_id  BIGINT NOT NULL,
    direction        TEXT   NOT NULL,          -- inbound | outbound
    sender_kind      TEXT   NOT NULL,          -- contact | user | agent_bot
    is_private       BOOLEAN NOT NULL DEFAULT false,
    content_enc      BYTEA,                    -- pgcrypto (AES-256)
    content_len      INT NOT NULL DEFAULT 0,
    has_attachment   BOOLEAN NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chatwoot_msg_id)
);
CREATE INDEX idx_message_log_conv_created ON message_log (conversation_id, created_at DESC);

-- Cada execução do agente
CREATE TABLE agent_run (
    run_id           UUID PRIMARY KEY,
    conversation_id  BIGINT NOT NULL,
    provider         TEXT   NOT NULL,
    agent_id         TEXT,
    trigger_reason   TEXT   NOT NULL,          -- debounce_expired | max_messages | ...
    input_msg_ids    BIGINT[] NOT NULL,
    status           TEXT   NOT NULL,          -- running|succeeded|failed|blocked|timeout
    error_kind       TEXT,
    input_tokens     INT,
    output_tokens    INT,
    cost_usd         NUMERIC(10,6),
    latency_ms       INT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ
);
CREATE INDEX idx_agent_run_conv_started ON agent_run (conversation_id, started_at DESC);
CREATE INDEX idx_agent_run_status_started ON agent_run (status, started_at DESC);

-- Decisões dos gates (auditabilidade do "por que não respondeu")
CREATE TABLE gate_decision (
    id               BIGSERIAL PRIMARY KEY,
    conversation_id  BIGINT NOT NULL,
    gate             TEXT NOT NULL,            -- inbound | outbound
    rule             TEXT NOT NULL,            -- G1..G11 | S1..S12
    decision         TEXT NOT NULL,            -- allow | block | modify
    detail           JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Mensagens de saída (idempotência e reconciliação)
CREATE TABLE outbound_message (
    id               BIGSERIAL PRIMARY KEY,
    idempotency_key  TEXT UNIQUE NOT NULL,
    run_id           UUID,
    conversation_id  BIGINT NOT NULL,
    payload          JSONB NOT NULL,
    state            TEXT NOT NULL DEFAULT 'pending', -- pending|sent|failed|abandoned
    chatwoot_msg_id  BIGINT,
    attempts         SMALLINT NOT NULL DEFAULT 0,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at          TIMESTAMPTZ
);

-- SLA
CREATE TABLE sla_timer (
    id               BIGSERIAL PRIMARY KEY,
    conversation_id  BIGINT NOT NULL,
    kind             TEXT NOT NULL,            -- first_response|human_response|assignment|resolution
    due_at           TIMESTAMPTZ NOT NULL,
    escalation_level SMALLINT NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'armed', -- armed|fired|cancelled
    cancelled_reason TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (conversation_id, kind)
);
CREATE INDEX idx_sla_timer_status_due ON sla_timer (status, due_at) WHERE status = 'armed';

CREATE TABLE notification_log (
    id               BIGSERIAL PRIMARY KEY,
    conversation_id  BIGINT NOT NULL,
    sla_kind         TEXT NOT NULL,
    level            SMALLINT NOT NULL,
    recipient        TEXT NOT NULL,
    channel          TEXT NOT NULL,
    state            TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (conversation_id, sla_kind, level, channel)   -- idempotência
);

-- Vínculo contato <-> cliente do ERP
CREATE TABLE contact_link (
    contact_id       BIGINT PRIMARY KEY,
    erp_client_id    TEXT,
    cnpj             TEXT,
    verified         BOOLEAN NOT NULL DEFAULT false,
    verified_at      TIMESTAMPTZ,
    expires_at       TIMESTAMPTZ,
    attributes       JSONB NOT NULL DEFAULT '{}'
);

-- Auditoria append-only (5 anos, sem conteúdo)
CREATE TABLE audit_log (
    id               BIGSERIAL PRIMARY KEY,
    at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor            TEXT NOT NULL,            -- ai | human:{id} | system
    action           TEXT NOT NULL,
    conversation_id  BIGINT,
    run_id           UUID,
    payload_hash     TEXT,
    meta             JSONB
);
