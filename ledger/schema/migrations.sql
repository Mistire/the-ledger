-- =============================================================================
-- The Ledger — Schema Migrations
-- =============================================================================
-- Run this file against your PostgreSQL database to create all required tables.
-- All statements use CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS
-- so they are safe to re-run.

-- =============================================================================
-- Phase 3 — Projection Read Models
-- =============================================================================

-- ApplicationSummary projection read model
CREATE TABLE IF NOT EXISTS application_summary (
    application_id          TEXT PRIMARY KEY,
    state                   TEXT NOT NULL DEFAULT 'SUBMITTED',
    applicant_id            TEXT,
    requested_amount_usd    NUMERIC(18,2),
    approved_amount_usd     NUMERIC(18,2),
    risk_tier               TEXT,
    fraud_score             FLOAT,
    compliance_status       TEXT,
    decision                TEXT,
    agent_sessions_completed INT NOT NULL DEFAULT 0,
    last_event_type         TEXT,
    last_event_at           TIMESTAMPTZ,
    human_reviewer_id       TEXT,
    final_decision_at       TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- AgentPerformanceLedger projection read model
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id                TEXT NOT NULL,
    model_version           TEXT NOT NULL,
    analyses_completed      INT NOT NULL DEFAULT 0,
    decisions_generated     INT NOT NULL DEFAULT 0,
    avg_confidence_score    FLOAT,
    avg_duration_ms         FLOAT,
    approve_rate            FLOAT NOT NULL DEFAULT 0,
    decline_rate            FLOAT NOT NULL DEFAULT 0,
    refer_rate              FLOAT NOT NULL DEFAULT 0,
    human_override_rate     FLOAT NOT NULL DEFAULT 0,
    first_seen_at           TIMESTAMPTZ,
    last_seen_at            TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);

-- ComplianceAuditView projection read model with snapshot support
CREATE TABLE IF NOT EXISTS compliance_audit_events (
    id                      BIGSERIAL PRIMARY KEY,
    application_id          TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    rule_id                 TEXT,
    rule_name               TEXT,
    rule_version            TEXT,
    passed                  BOOLEAN,
    is_hard_block           BOOLEAN,
    overall_verdict         TEXT,
    full_payload            JSONB NOT NULL DEFAULT '{}',
    event_recorded_at       TIMESTAMPTZ NOT NULL,
    global_position         BIGINT NOT NULL DEFAULT 0,
    -- Snapshot support
    is_snapshot_row         BOOLEAN NOT NULL DEFAULT FALSE,
    snapshot_state          JSONB,
    snapshot_recorded_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_cae_application_id
    ON compliance_audit_events (application_id, event_recorded_at);

CREATE INDEX IF NOT EXISTS idx_cae_snapshot
    ON compliance_audit_events (application_id, is_snapshot_row)
    WHERE is_snapshot_row = TRUE;

-- =============================================================================
-- Projection Checkpoints (used by ProjectionDaemon)
-- =============================================================================
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name         TEXT PRIMARY KEY,
    last_position           BIGINT NOT NULL DEFAULT 0,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
