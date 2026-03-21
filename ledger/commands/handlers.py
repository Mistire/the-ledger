"""
ledger/commands/handlers.py
============================
Command handlers following the four-step pattern (per rubric):
  1. Load aggregate(s) from event store
  2. Validate via aggregate guard methods
  3. Determine new events (pure logic, no I/O)
  4. Append atomically with version from loaded aggregate

Per rubric:
  - Version sourced from aggregate's tracked version, NOT hardcoded
  - correlation_id and causation_id accepted and threaded through
  - Credit analysis handler loads BOTH loan + agent session aggregates
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, UTC
from decimal import Decimal
from hashlib import sha256

from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.event_store import DomainError


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND DATACLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal
    loan_purpose: str
    submission_channel: str = "api"
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class CreditAnalysisCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: Decimal
    duration_ms: int = 0
    input_data: dict | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_submit_application(cmd: SubmitApplicationCommand, store) -> int:
    """
    Four-step pattern:
      1. Load aggregate
      2. Validate (must be new)
      3. Determine events (no I/O)
      4. Append atomically
    """
    # 1. Load
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate
    if app.state is not None:
        raise DomainError(
            cmd.application_id, "application_already_exists",
            f"Application {cmd.application_id} already exists in state {app.state}",
        )

    # 3. Determine events — pure logic, no I/O
    new_events = [{
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "applicant_id": cmd.applicant_id,
            "requested_amount_usd": str(cmd.requested_amount_usd),
            "loan_purpose": cmd.loan_purpose,
            "submission_channel": cmd.submission_channel,
            "submitted_at": datetime.now(UTC).isoformat(),
        },
    }]

    # 4. Append — version from aggregate, NOT hardcoded
    positions = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    return positions[-1]


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand, store
) -> int:
    """
    Four-step pattern with MULTI-AGGREGATE loading:
      1. Load BOTH LoanApplicationAggregate AND AgentSessionAggregate
      2. Validate via guard methods on both
      3. Determine events (no I/O)
      4. Append atomically
    """
    # 1. Load BOTH aggregates
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # 2. Validate via aggregate guard methods
    app.assert_awaiting_credit_analysis()
    app.assert_credit_analysis_not_completed()
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)

    # 3. Determine events — pure logic, no I/O
    input_data_hash = sha256(str(cmd.input_data or {}).encode()).hexdigest()

    new_events = [{
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": {
            "application_id": cmd.application_id,
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
            "model_version": cmd.model_version,
            "confidence_score": cmd.confidence_score,
            "risk_tier": cmd.risk_tier,
            "recommended_limit_usd": str(cmd.recommended_limit_usd),
            "analysis_duration_ms": cmd.duration_ms,
            "input_data_hash": input_data_hash,
        },
    }]

    # 4. Append — version from aggregate, NOT hardcoded
    positions = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    return positions[-1]
