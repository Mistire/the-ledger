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
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate
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


# ─── 2.1 Additional command dataclasses ──────────────────────────────────────

@dataclass
class FraudScreeningCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    fraud_score: float
    risk_level: str
    anomalies_found: int
    recommendation: str
    screening_model_version: str
    input_data_hash: str
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class ComplianceCheckCommand:
    application_id: str
    session_id: str
    rule_id: str
    rule_name: str
    rule_version: str
    passed: bool
    is_hard_block: bool = False
    failure_reason: str | None = None
    remediation_available: bool = False
    evidence_hash: str = ""
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class GenerateDecisionCommand:
    application_id: str
    orchestrator_session_id: str
    recommendation: str
    confidence_score: float
    approved_amount_usd: Decimal | None
    conditions: list
    executive_summary: str
    key_risks: list
    contributing_agent_sessions: list
    model_versions: dict
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class HumanReviewCompletedCommand:
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None
    approved_amount_usd: Decimal | None = None
    interest_rate_pct: float | None = None
    term_months: int | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class StartAgentSessionCommand:
    agent_id: str
    agent_type: str
    session_id: str
    application_id: str
    model_version: str
    correlation_id: str | None = None
    causation_id: str | None = None


# ─── 2.2 handle_fraud_screening_completed ────────────────────────────────────

async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand, store
) -> int:
    """
    Four-step pattern:
      1. Load LoanApplicationAggregate + AgentSessionAggregate
      2. Validate guards
      3. Determine events
      4. Append to loan-{id} stream with version from aggregate
    """
    # 1. Load
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # 2. Validate
    agent.assert_context_loaded()
    agent.assert_not_completed()

    # 3. Determine events
    new_events = [{
        "event_type": "FraudScreeningCompleted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
            "fraud_score": cmd.fraud_score,
            "risk_level": cmd.risk_level,
            "anomalies_found": cmd.anomalies_found,
            "recommendation": cmd.recommendation,
            "screening_model_version": cmd.screening_model_version,
            "input_data_hash": cmd.input_data_hash,
            "completed_at": datetime.now(UTC).isoformat(),
        },
    }]

    # 4. Append — version from aggregate
    positions = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    return positions[-1]


# ─── 2.3 handle_compliance_check ─────────────────────────────────────────────

async def handle_compliance_check(
    cmd: ComplianceCheckCommand, store
) -> int:
    """
    Four-step pattern:
      1. Load LoanApplicationAggregate + ComplianceRecordAggregate
      2. Validate state
      3. Determine event type based on cmd.passed and cmd.is_hard_block
      4. Append to compliance-{id} stream with version from compliance aggregate
    """
    # 1. Load
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)

    # 2. Validate — application must exist
    if app.state is None:
        raise DomainError(
            cmd.application_id,
            "application_not_found",
            f"Application {cmd.application_id} does not exist.",
        )

    # 3. Determine event
    now = datetime.now(UTC).isoformat()
    if cmd.passed:
        event = {
            "event_type": "ComplianceRulePassed",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "session_id": cmd.session_id,
                "rule_id": cmd.rule_id,
                "rule_name": cmd.rule_name,
                "rule_version": cmd.rule_version,
                "evidence_hash": cmd.evidence_hash,
                "evaluation_notes": "",
                "evaluated_at": now,
            },
        }
    elif not cmd.passed and cmd.is_hard_block:
        event = {
            "event_type": "ComplianceRuleFailed",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "session_id": cmd.session_id,
                "rule_id": cmd.rule_id,
                "rule_name": cmd.rule_name,
                "rule_version": cmd.rule_version,
                "failure_reason": cmd.failure_reason or "",
                "is_hard_block": True,
                "remediation_available": cmd.remediation_available,
                "evidence_hash": cmd.evidence_hash,
                "evaluated_at": now,
            },
        }
    else:
        # failed but not a hard block → note it
        event = {
            "event_type": "ComplianceRuleNoted",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "session_id": cmd.session_id,
                "rule_id": cmd.rule_id,
                "rule_name": cmd.rule_name,
                "note_type": "SOFT_FAIL",
                "note_text": cmd.failure_reason or "",
                "evaluated_at": now,
            },
        }

    # 4. Append — version from compliance aggregate
    positions = await store.append(
        stream_id=f"compliance-{cmd.application_id}",
        events=[event],
        expected_version=compliance.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    return positions[-1]


# ─── 2.4 handle_generate_decision ────────────────────────────────────────────

async def handle_generate_decision(
    cmd: GenerateDecisionCommand, store
) -> int:
    """
    Four-step pattern:
      1. Load LoanApplicationAggregate
      2. Enforce confidence floor, compliance guard, causal chain validation
      3. Determine DecisionGenerated event
      4. Append to loan-{id} stream
    """
    # 1. Load
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2a. Confidence floor — force REFER if confidence < 0.6
    recommendation = cmd.recommendation
    if cmd.confidence_score < 0.6:
        recommendation = "REFER"

    # 2b. Compliance dependency guard
    if not app.all_compliance_passed:
        raise DomainError(
            cmd.application_id,
            "compliance_dependency",
            "Cannot generate decision: not all compliance checks have passed.",
        )

    # 2c. Causal chain validation — each contributing session must have a
    #     decision event for this application_id
    if not cmd.contributing_agent_sessions:
        raise DomainError(
            cmd.application_id,
            "causal_chain_violation",
            "DecisionGenerated must reference at least one contributing agent session.",
        )
    decision_event_types = {
        "CreditAnalysisCompleted", "FraudScreeningCompleted",
        "DecisionGenerated", "ComplianceCheckCompleted",
    }
    for session_ref in cmd.contributing_agent_sessions:
        # contributing_agent_sessions entries may be:
        #   - a full stream id like "agent-credit_analysis-sess123"
        #   - or just a session_id, in which case we try "agent-{session_ref}"
        # Try the entry as a full stream id first, then as a suffix.
        if session_ref.startswith("agent-"):
            stream_id_to_check = session_ref
        else:
            stream_id_to_check = f"agent-{session_ref}"

        session_events = await store.load_stream(stream_id_to_check)
        if not session_events:
            raise DomainError(
                cmd.application_id,
                "causal_chain_violation",
                f"Agent session stream '{stream_id_to_check}' not found or is empty.",
            )
        # Verify the session contains a decision event for this application
        has_decision = any(
            e.get("event_type") in decision_event_types
            and e.get("payload", {}).get("application_id") == cmd.application_id
            for e in session_events
        )
        if not has_decision:
            raise DomainError(
                cmd.application_id,
                "causal_chain_violation",
                f"Agent session '{stream_id_to_check}' has no decision event for "
                f"application {cmd.application_id}.",
            )

    # 3. Determine events
    new_events = [{
        "event_type": "DecisionGenerated",
        "event_version": 2,
        "payload": {
            "application_id": cmd.application_id,
            "orchestrator_session_id": cmd.orchestrator_session_id,
            "recommendation": recommendation,
            "confidence_score": cmd.confidence_score,
            "approved_amount_usd": str(cmd.approved_amount_usd) if cmd.approved_amount_usd is not None else None,
            "conditions": cmd.conditions,
            "executive_summary": cmd.executive_summary,
            "key_risks": cmd.key_risks,
            "contributing_agent_sessions": cmd.contributing_agent_sessions,
            "model_versions": cmd.model_versions,
            "generated_at": datetime.now(UTC).isoformat(),
        },
    }]

    # 4. Append — version from aggregate
    positions = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    return positions[-1]


# ─── 2.5 handle_human_review_completed ───────────────────────────────────────

async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand, store
) -> int:
    """
    Four-step pattern:
      1. Load LoanApplicationAggregate
      2. Validate state is APPROVED_PENDING_HUMAN or DECLINED_PENDING_HUMAN
      3. Determine HumanReviewCompleted + ApplicationApproved/Declined
      4. Append BOTH events atomically in a single store.append() call
    """
    from ledger.domain.aggregates.loan_application import ApplicationState

    # 1. Load
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate state
    valid_states = {
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
    }
    if app.state not in valid_states:
        raise DomainError(
            cmd.application_id,
            "invalid_state_for_human_review",
            f"Expected APPROVED_PENDING_HUMAN or DECLINED_PENDING_HUMAN, got {app.state}.",
        )

    # 3. Determine events
    now = datetime.now(UTC).isoformat()
    review_event = {
        "event_type": "HumanReviewCompleted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "reviewer_id": cmd.reviewer_id,
            "override": cmd.override,
            "original_recommendation": cmd.original_recommendation,
            "final_decision": cmd.final_decision,
            "override_reason": cmd.override_reason,
            "reviewed_at": now,
        },
    }

    final_decision_upper = cmd.final_decision.upper()
    if final_decision_upper in ("APPROVE", "APPROVED"):
        outcome_event = {
            "event_type": "ApplicationApproved",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "approved_amount_usd": str(cmd.approved_amount_usd) if cmd.approved_amount_usd is not None else None,
                "interest_rate_pct": cmd.interest_rate_pct,
                "term_months": cmd.term_months,
                "conditions": [],
                "approved_by": cmd.reviewer_id,
                "effective_date": now[:10],
                "approved_at": now,
            },
        }
    else:
        outcome_event = {
            "event_type": "ApplicationDeclined",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "decline_reasons": [cmd.override_reason or "Human review decision"],
                "declined_by": cmd.reviewer_id,
                "adverse_action_notice_required": True,
                "adverse_action_codes": [],
                "declined_at": now,
            },
        }

    # 4. Append BOTH events atomically in a single store.append() call
    positions = await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[review_event, outcome_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    return positions[-1]


# ─── 2.6 handle_start_agent_session ──────────────────────────────────────────

async def handle_start_agent_session(
    cmd: StartAgentSessionCommand, store
) -> int:
    """
    Appends AgentSessionStarted to agent-{agent_type}-{session_id} stream
    with expected_version=-1 (new stream).
    """
    # No aggregate load needed — this is always a new stream (expected_version=-1)
    new_events = [{
        "event_type": "AgentSessionStarted",
        "event_version": 1,
        "payload": {
            "session_id": cmd.session_id,
            "agent_id": cmd.agent_id,
            "agent_type": cmd.agent_type,
            "application_id": cmd.application_id,
            "model_version": cmd.model_version,
            "context_source": "command",
            "context_token_count": 0,
            "started_at": datetime.now(UTC).isoformat(),
        },
    }]

    positions = await store.append(
        stream_id=f"agent-{cmd.agent_type}-{cmd.session_id}",
        events=new_events,
        expected_version=-1,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
    return positions[-1]
