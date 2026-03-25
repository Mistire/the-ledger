"""
ledger/mcp_server.py — FastMCP Server for The Ledger
=====================================================
Exposes 8 command tools and 6 read resources over the MCP protocol.

Tools (write path):
  submit_application, start_agent_session, record_credit_analysis,
  record_fraud_screening, record_compliance_check, generate_decision,
  record_human_review, run_integrity_check_tool

Resources (read path):
  ledger://applications/{id}
  ledger://applications/{id}/compliance
  ledger://applications/{id}/audit-trail
  ledger://agents/{id}/performance
  ledger://agents/{id}/sessions/{session_id}
  ledger://ledger/health
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from decimal import Decimal

from fastmcp import FastMCP

from ledger.commands.handlers import (
    SubmitApplicationCommand,
    handle_submit_application,
    StartAgentSessionCommand,
    handle_start_agent_session,
    CreditAnalysisCompletedCommand,
    handle_credit_analysis_completed,
    FraudScreeningCompletedCommand,
    handle_fraud_screening_completed,
    ComplianceCheckCommand,
    handle_compliance_check,
    GenerateDecisionCommand,
    handle_generate_decision,
    HumanReviewCompletedCommand,
    handle_human_review_completed,
)
from ledger.event_store import EventStore, OptimisticConcurrencyError, DomainError
from ledger.integrity.audit_chain import run_integrity_check
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon

# ── MCP instance ──────────────────────────────────────────────────────────────

mcp = FastMCP("The Ledger")

# ── Shared store (set via set_store or auto-initialised from DATABASE_URL) ────

_store: EventStore | None = None


def set_store(store: EventStore) -> None:
    """Override the module-level store (used in tests and custom entrypoints)."""
    global _store
    _store = store


def _get_store() -> EventStore:
    global _store
    if _store is None:
        db_url = os.environ.get("DATABASE_URL", "")
        _store = EventStore(db_url)
    return _store


# ── Shared daemon (set via set_daemon) ────────────────────────────────────────

_daemon: ProjectionDaemon | None = None


def set_daemon(daemon: ProjectionDaemon) -> None:
    """Register the running ProjectionDaemon so get_health can query lags."""
    global _daemon
    _daemon = daemon


# ── Shared compliance projection (set via set_compliance_projection) ──────────

_compliance_projection: ComplianceAuditViewProjection | None = None


def set_compliance_projection(projection: ComplianceAuditViewProjection) -> None:
    """Register the ComplianceAuditViewProjection for temporal queries."""
    global _compliance_projection
    _compliance_projection = projection


# ── SLO bounds for health check ───────────────────────────────────────────────

_SLO_BOUNDS_MS: dict[str, int] = {
    "application_summary": 500,
    "compliance_audit_view": 2000,
}


# ── Rate limiting for run_integrity_check_tool ────────────────────────────────

_rate_limit: dict[str, float] = {}  # entity_key → last_call_timestamp


def _check_rate_limit(entity_key: str) -> bool:
    now = time.time()
    if entity_key in _rate_limit and now - _rate_limit[entity_key] < 60:
        return False
    _rate_limit[entity_key] = now
    return True


# ── Error helpers ─────────────────────────────────────────────────────────────

def _occ_error(e: OptimisticConcurrencyError) -> dict:
    return {
        "error_type": "OptimisticConcurrencyError",
        "message": str(e),
        "stream_id": e.stream_id,
        "expected_version": e.expected,
        "actual_version": e.actual,
        "suggested_action": "reload_stream_and_retry",
    }


def _domain_error(e: DomainError) -> dict:
    return {
        "error_type": "DomainError",
        "message": str(e),
        "aggregate_id": e.aggregate_id,
        "rule_violated": e.rule_violated,
        "suggested_action": "check_preconditions_and_retry",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOLS — command side
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def submit_application(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    submission_channel: str = "api",
) -> dict:
    """Submit a new loan application to the ledger."""
    cmd = SubmitApplicationCommand(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=Decimal(str(requested_amount_usd)),
        loan_purpose=loan_purpose,
        submission_channel=submission_channel,
    )
    try:
        position = await handle_submit_application(cmd, _get_store())
        return {"status": "ok", "stream_position": position, "application_id": application_id}
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


@mcp.tool()
async def start_agent_session(
    agent_id: str,
    agent_type: str,
    session_id: str,
    application_id: str,
    model_version: str,
) -> dict:
    """Start a new agent session for a given application."""
    cmd = StartAgentSessionCommand(
        agent_id=agent_id,
        agent_type=agent_type,
        session_id=session_id,
        application_id=application_id,
        model_version=model_version,
    )
    try:
        position = await handle_start_agent_session(cmd, _get_store())
        return {"status": "ok", "stream_position": position, "session_id": session_id}
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


@mcp.tool()
async def record_credit_analysis(
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    confidence_score: float,
    risk_tier: str,
    recommended_limit_usd: float,
) -> dict:
    """Record the result of a credit analysis agent run."""
    cmd = CreditAnalysisCompletedCommand(
        application_id=application_id,
        agent_id=agent_id,
        session_id=session_id,
        model_version=model_version,
        confidence_score=confidence_score,
        risk_tier=risk_tier,
        recommended_limit_usd=Decimal(str(recommended_limit_usd)),
    )
    try:
        position = await handle_credit_analysis_completed(cmd, _get_store())
        return {"status": "ok", "stream_position": position, "application_id": application_id}
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


@mcp.tool()
async def record_fraud_screening(
    application_id: str,
    agent_id: str,
    session_id: str,
    fraud_score: float,
    risk_level: str,
    recommendation: str,
) -> dict:
    """Record the result of a fraud screening agent run."""
    cmd = FraudScreeningCompletedCommand(
        application_id=application_id,
        agent_id=agent_id,
        session_id=session_id,
        fraud_score=fraud_score,
        risk_level=risk_level,
        anomalies_found=0,
        recommendation=recommendation,
        screening_model_version="1.0",
        input_data_hash="",
    )
    try:
        position = await handle_fraud_screening_completed(cmd, _get_store())
        return {"status": "ok", "stream_position": position, "application_id": application_id}
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


@mcp.tool()
async def record_compliance_check(
    application_id: str,
    session_id: str,
    rule_id: str,
    rule_name: str,
    rule_version: str,
    passed: bool,
    is_hard_block: bool = False,
    failure_reason: str | None = None,
) -> dict:
    """Record a single compliance rule evaluation result."""
    cmd = ComplianceCheckCommand(
        application_id=application_id,
        session_id=session_id,
        rule_id=rule_id,
        rule_name=rule_name,
        rule_version=rule_version,
        passed=passed,
        is_hard_block=is_hard_block,
        failure_reason=failure_reason,
    )
    try:
        position = await handle_compliance_check(cmd, _get_store())
        return {"status": "ok", "stream_position": position, "application_id": application_id}
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


@mcp.tool()
async def generate_decision(
    application_id: str,
    orchestrator_session_id: str,
    recommendation: str,
    confidence_score: float,
    executive_summary: str,
    contributing_agent_sessions: list[str],
) -> dict:
    """
    Generate a final decision for a loan application.
    Enforces confidence floor: confidence < 0.6 forces recommendation to REFER.
    """
    # Confidence floor enforced here (also enforced in handler, belt-and-suspenders)
    effective_recommendation = recommendation if confidence_score >= 0.6 else "REFER"

    cmd = GenerateDecisionCommand(
        application_id=application_id,
        orchestrator_session_id=orchestrator_session_id,
        recommendation=effective_recommendation,
        confidence_score=confidence_score,
        approved_amount_usd=None,
        conditions=[],
        executive_summary=executive_summary,
        key_risks=[],
        contributing_agent_sessions=contributing_agent_sessions,
        model_versions={},
    )
    try:
        position = await handle_generate_decision(cmd, _get_store())
        return {
            "status": "ok",
            "stream_position": position,
            "application_id": application_id,
            "recommendation": effective_recommendation,
            "confidence_floor_applied": confidence_score < 0.6,
        }
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


@mcp.tool()
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    override: bool,
    original_recommendation: str,
    final_decision: str,
    override_reason: str | None = None,
) -> dict:
    """Record a human reviewer's decision on a loan application."""
    cmd = HumanReviewCompletedCommand(
        application_id=application_id,
        reviewer_id=reviewer_id,
        override=override,
        original_recommendation=original_recommendation,
        final_decision=final_decision,
        override_reason=override_reason,
    )
    try:
        position = await handle_human_review_completed(cmd, _get_store())
        return {"status": "ok", "stream_position": position, "application_id": application_id}
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


@mcp.tool()
async def run_integrity_check_tool(entity_type: str, entity_id: str) -> dict:
    """
    Run a cryptographic integrity check on an entity's event stream.
    Rate-limited to 1 call per minute per entity.
    """
    entity_key = f"{entity_type}:{entity_id}"
    if not _check_rate_limit(entity_key):
        return {
            "error_type": "RateLimitError",
            "message": f"Integrity check for '{entity_key}' was called less than 60 seconds ago.",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "suggested_action": "wait_60_seconds_and_retry",
        }
    try:
        result = await run_integrity_check(_get_store(), entity_type, entity_id)
        return {
            "status": "ok",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "events_verified": result.events_verified,
            "chain_valid": result.chain_valid,
            "tamper_detected": result.tamper_detected,
            "integrity_hash": result.integrity_hash,
            "check_timestamp": result.check_timestamp.isoformat(),
        }
    except OptimisticConcurrencyError as e:
        return _occ_error(e)
    except DomainError as e:
        return _domain_error(e)


# ═══════════════════════════════════════════════════════════════════════════════
# RESOURCES — query side
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.resource("ledger://applications/{id}")
async def get_application(id: str) -> dict:
    """Get application summary from the read model."""
    store = _get_store()
    if store._pool is None:
        return {"error": "store not connected"}
    async with store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1", id
        )
        if not row:
            return {"error": "not_found", "application_id": id}
        return dict(row)


@mcp.resource("ledger://applications/{id}/compliance")
async def get_compliance(id: str, as_of: str | None = None) -> dict:
    """Get compliance audit view for an application, optionally at a point in time."""
    if as_of is not None:
        # Use the projection's temporal query when as_of is provided
        if _compliance_projection is None:
            return {"error": "compliance_projection_not_configured"}
        try:
            timestamp = datetime.fromisoformat(as_of)
        except ValueError:
            return {"error": "invalid_as_of_format", "detail": "Expected ISO8601 timestamp"}
        try:
            return await _compliance_projection.get_compliance_at(id, timestamp)
        except Exception:
            return {"error": "not_found", "application_id": id}

    # No as_of — return current compliance events from the table
    store = _get_store()
    if store._pool is None:
        return {"error": "store not connected"}
    async with store._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM compliance_audit_events "
            "WHERE application_id = $1 AND is_snapshot_row = FALSE "
            "ORDER BY event_recorded_at ASC",
            id,
        )
        if not rows:
            return {"error": "not_found", "application_id": id}
        return {"application_id": id, "compliance_events": [dict(r) for r in rows]}


@mcp.resource("ledger://applications/{id}/audit-trail")
async def get_audit_trail(id: str, from_pos: int | None = None, to_pos: int | None = None) -> dict:
    """Get the cryptographic audit trail for a loan application."""
    store = _get_store()
    try:
        events = await store.load_stream(
            f"audit-loan-{id}",
            from_position=from_pos if from_pos is not None else 0,
            to_position=to_pos,
        )
    except Exception:
        events = []
    return {"application_id": id, "audit_events": events}


@mcp.resource("ledger://agents/{id}/performance")
async def get_agent_performance(id: str) -> dict:
    """Get agent performance metrics from the read model."""
    store = _get_store()
    if store._pool is None:
        return {"error": "store not connected"}
    async with store._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger WHERE agent_id = $1", id
        )
        if not rows:
            return {"error": "not_found", "agent_id": id}
        return {"agent_id": id, "performance": [dict(r) for r in rows]}


@mcp.resource("ledger://agents/{id}/sessions/{session_id}")
async def get_agent_session(id: str, session_id: str) -> dict:
    """Get the full event stream for an agent session."""
    store = _get_store()
    try:
        events = await store.load_stream(f"agent-{id}-{session_id}")
    except Exception:
        events = []
    if not events:
        return {"error": "not_found", "agent_id": id, "session_id": session_id}
    return {"agent_id": id, "session_id": session_id, "events": events}


@mcp.resource("ledger://ledger/health")
async def get_health() -> dict:
    """Get projection daemon health and lag metrics."""
    if _daemon is None:
        return {"healthy": False, "lags": {}, "error": "daemon_not_configured"}

    lags = await _daemon.get_all_lags()

    # healthy=True only when ALL projections are within their SLO bounds
    healthy = True
    for projection_name, lag_ms in lags.items():
        slo = _SLO_BOUNDS_MS.get(projection_name)
        if slo is not None and lag_ms > slo:
            healthy = False
            break

    return {"lags": lags, "healthy": healthy}
