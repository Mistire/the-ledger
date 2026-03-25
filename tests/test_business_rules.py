"""
tests/test_business_rules.py
============================
Property-based and example tests for business rule enforcement.

Validates: Requirements 16.1–16.7 | Properties 2, 3, 4
"""
from __future__ import annotations

import asyncio
import uuid

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ledger.commands.handlers import (
    CreditAnalysisCompletedCommand,
    GenerateDecisionCommand,
    handle_credit_analysis_completed,
    handle_generate_decision,
)
from ledger.event_store import DomainError, InMemoryEventStore


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def new_id() -> str:
    return str(uuid.uuid4())


async def _setup_app_awaiting_analysis(store: InMemoryEventStore, app_id: str) -> None:
    """Append ApplicationSubmitted + CreditAnalysisRequested to put app in AWAITING_ANALYSIS."""
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "ApplicationSubmitted",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "applicant_id": "applicant-001",
                "requested_amount_usd": "500000",
                "loan_purpose": "expansion",
                "submission_channel": "api",
            },
        }],
        expected_version=-1,
    )
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "CreditAnalysisRequested",
            "event_version": 1,
            "payload": {"application_id": app_id},
        }],
        expected_version=0,
    )


async def _setup_app_compliance_passed(
    store: InMemoryEventStore, app_id: str
) -> None:
    """
    Set up a loan application with all_compliance_passed=True.
    Appends: ApplicationSubmitted, CreditAnalysisRequested, CreditAnalysisCompleted,
             ComplianceCheckRequested (with checks_required=["rule1"]),
             ComplianceRulePassed (rule_id="rule1").
    """
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "ApplicationSubmitted",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "applicant_id": "applicant-001",
                "requested_amount_usd": "500000",
                "loan_purpose": "expansion",
                "submission_channel": "api",
            },
        }],
        expected_version=-1,
    )
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "CreditAnalysisRequested",
            "event_version": 1,
            "payload": {"application_id": app_id},
        }],
        expected_version=0,
    )
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "CreditAnalysisCompleted",
            "event_version": 2,
            "payload": {
                "application_id": app_id,
                "agent_id": "credit-agent",
                "session_id": "sess-001",
                "model_version": "v1",
                "confidence_score": 0.85,
                "risk_tier": "LOW",
                "recommended_limit_usd": "500000",
                "analysis_duration_ms": 1000,
                "input_data_hash": "abc123",
            },
        }],
        expected_version=1,
    )
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "ComplianceCheckRequested",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "checks_required": ["rule1"],
            },
        }],
        expected_version=2,
    )
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "ComplianceRulePassed",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "rule_id": "rule1",
                "rule_name": "AML Check",
                "rule_version": "1.0",
                "evidence_hash": "hash1",
                "evaluation_notes": "",
            },
        }],
        expected_version=3,
    )


async def _setup_valid_agent_session(
    store: InMemoryEventStore, app_id: str, session_stream: str
) -> None:
    """
    Append an AgentSessionStarted + CreditAnalysisCompleted to a session stream
    so the causal chain check passes.
    """
    await store.append(
        stream_id=session_stream,
        events=[
            {
                "event_type": "AgentSessionStarted",
                "event_version": 1,
                "payload": {
                    "session_id": "sess-valid",
                    "agent_id": "credit-agent",
                    "agent_type": "credit_analysis",
                    "application_id": app_id,
                    "model_version": "v1",
                    "context_source": "test",
                    "context_token_count": 100,
                },
            },
            {
                "event_type": "CreditAnalysisCompleted",
                "event_version": 2,
                "payload": {
                    "application_id": app_id,
                    "agent_id": "credit-agent",
                    "session_id": "sess-valid",
                    "model_version": "v1",
                    "confidence_score": 0.85,
                    "risk_tier": "LOW",
                    "recommended_limit_usd": "500000",
                    "analysis_duration_ms": 1000,
                    "input_data_hash": "abc123",
                },
            },
        ],
        expected_version=-1,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 2 — Confidence floor
# Validates: Requirements 16.2
# ═══════════════════════════════════════════════════════════════════════════════

@given(st.floats(min_value=0.0, max_value=0.599, allow_nan=False))
@settings(max_examples=50)
def test_confidence_floor_forces_refer(confidence_score):
    """
    **Validates: Requirements 16.2**
    Any confidence_score < 0.6 must produce recommendation="REFER" regardless
    of the originally requested recommendation.
    """
    async def _run():
        store = InMemoryEventStore()
        app_id = new_id()
        session_stream = f"agent-credit_analysis-sess-{app_id}"

        await _setup_app_compliance_passed(store, app_id)
        await _setup_valid_agent_session(store, app_id, session_stream)

        cmd = GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_session_id="orch-sess-001",
            recommendation="APPROVE",  # requested APPROVE, but confidence is too low
            confidence_score=confidence_score,
            approved_amount_usd=None,
            conditions=[],
            executive_summary="Test decision",
            key_risks=[],
            contributing_agent_sessions=[session_stream],
            model_versions={"credit": "v1"},
        )

        # Load the resulting event from the stream
        await handle_generate_decision(cmd, store)
        events = await store.load_stream(f"loan-{app_id}")
        decision_event = next(
            e for e in events if e["event_type"] == "DecisionGenerated"
        )
        assert decision_event["payload"]["recommendation"] == "REFER", (
            f"Expected REFER for confidence={confidence_score}, "
            f"got {decision_event['payload']['recommendation']}"
        )

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 3 — Compliance dependency
# Validates: Requirements 16.3
# ═══════════════════════════════════════════════════════════════════════════════

def test_compliance_dependency_raises_domain_error():
    """
    **Validates: Requirements 16.3**
    Calling handle_generate_decision on an application that has NOT had all
    compliance checks passed must raise DomainError(rule_violated="compliance_dependency").
    """
    async def _run():
        store = InMemoryEventStore()
        app_id = new_id()

        # Set up app in AWAITING_ANALYSIS — no compliance events appended
        await _setup_app_awaiting_analysis(store, app_id)

        cmd = GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_session_id="orch-sess-001",
            recommendation="APPROVE",
            confidence_score=0.85,
            approved_amount_usd=None,
            conditions=[],
            executive_summary="Test",
            key_risks=[],
            contributing_agent_sessions=["agent-credit_analysis-sess-001"],
            model_versions={"credit": "v1"},
        )

        with pytest.raises(DomainError) as exc_info:
            await handle_generate_decision(cmd, store)

        assert exc_info.value.rule_violated == "compliance_dependency"

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 4 — Causal chain violation
# Validates: Requirements 16.4
# ═══════════════════════════════════════════════════════════════════════════════

@given(st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=5))
@settings(max_examples=30)
def test_causal_chain_violation_for_invalid_sessions(session_ids):
    """
    **Validates: Requirements 16.4**
    Calling handle_generate_decision with contributing_agent_sessions pointing to
    non-existent or empty streams must raise DomainError(rule_violated="causal_chain_violation").
    """
    async def _run():
        store = InMemoryEventStore()
        app_id = new_id()

        # Set up app with compliance passed so we get past the compliance check
        await _setup_app_compliance_passed(store, app_id)

        # Use session IDs that are guaranteed not to exist in the store
        # Prefix with "nonexistent-" to avoid any accidental collision
        nonexistent_sessions = [f"nonexistent-{sid}" for sid in session_ids]

        cmd = GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_session_id="orch-sess-001",
            recommendation="APPROVE",
            confidence_score=0.85,
            approved_amount_usd=None,
            conditions=[],
            executive_summary="Test",
            key_risks=[],
            contributing_agent_sessions=nonexistent_sessions,
            model_versions={"credit": "v1"},
        )

        with pytest.raises(DomainError) as exc_info:
            await handle_generate_decision(cmd, store)

        assert exc_info.value.rule_violated == "causal_chain_violation"

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# EXAMPLE — Invalid state transition
# Validates: Requirements 16.1, 16.2
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_invalid_state_transition_raises_domain_error():
    """
    **Validates: Requirements 16.1, 16.2**
    Attempting an invalid state machine transition SUBMITTED → FINAL_APPROVED
    must raise DomainError(rule_violated="invalid_transition").
    """
    from ledger.domain.aggregates.loan_application import (
        ApplicationState,
        LoanApplicationAggregate,
    )

    store = InMemoryEventStore()
    app_id = new_id()

    # Put application in SUBMITTED state
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "ApplicationSubmitted",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "applicant_id": "applicant-001",
                "requested_amount_usd": "500000",
                "loan_purpose": "expansion",
                "submission_channel": "api",
            },
        }],
        expected_version=-1,
    )

    # Load aggregate — state is now SUBMITTED
    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.state == ApplicationState.SUBMITTED

    # Attempt invalid transition SUBMITTED → FINAL_APPROVED
    with pytest.raises(DomainError) as exc_info:
        agg.assert_valid_transition(ApplicationState.FINAL_APPROVED)

    assert exc_info.value.rule_violated == "invalid_transition"


# ═══════════════════════════════════════════════════════════════════════════════
# EXAMPLE — Gas Town violation
# Validates: Requirements 16.5
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_credit_analysis_without_agent_session_raises_gas_town_violation():
    """
    **Validates: Requirements 16.5**
    Calling handle_credit_analysis_completed with a session_id that has no
    AgentSessionStarted event must raise DomainError(rule_violated="gas_town_violation").
    """
    store = InMemoryEventStore()
    app_id = new_id()
    session_id = new_id()
    agent_id = "credit-agent"

    # Set up app in AWAITING_ANALYSIS state
    await _setup_app_awaiting_analysis(store, app_id)

    # Do NOT append any AgentSessionStarted — the session stream is empty
    cmd = CreditAnalysisCompletedCommand(
        application_id=app_id,
        agent_id=agent_id,
        session_id=session_id,
        model_version="v1",
        confidence_score=0.85,
        risk_tier="LOW",
        recommended_limit_usd=500000,
        duration_ms=1000,
    )

    with pytest.raises(DomainError) as exc_info:
        await handle_credit_analysis_completed(cmd, store)

    assert exc_info.value.rule_violated == "gas_town_violation"


# ═══════════════════════════════════════════════════════════════════════════════
# EXAMPLE — Model version lock
# Validates: Requirements 16.6
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_second_credit_analysis_raises_model_version_lock():
    """
    **Validates: Requirements 16.6**
    Attempting a second CreditAnalysisCompleted on an application that already
    has one must raise DomainError(rule_violated="model_version_lock").
    """
    store = InMemoryEventStore()
    app_id = new_id()
    agent_id = "credit-agent"
    session_id = new_id()

    # Set up app in AWAITING_ANALYSIS state
    await _setup_app_awaiting_analysis(store, app_id)

    # Append AgentSessionStarted so the Gas Town check passes
    await store.append(
        stream_id=f"agent-{agent_id}-{session_id}",
        events=[{
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_id": agent_id,
                "agent_type": "credit_analysis",
                "application_id": app_id,
                "model_version": "v1",
                "context_source": "test",
                "context_token_count": 100,
            },
        }],
        expected_version=-1,
    )

    cmd = CreditAnalysisCompletedCommand(
        application_id=app_id,
        agent_id=agent_id,
        session_id=session_id,
        model_version="v1",
        confidence_score=0.85,
        risk_tier="LOW",
        recommended_limit_usd=500000,
        duration_ms=1000,
    )

    # First call succeeds
    await handle_credit_analysis_completed(cmd, store)

    # Directly append CreditAnalysisCompleted again to simulate a second attempt
    # (the app is now in ANALYSIS_COMPLETE, so we need to reset state for the guard check)
    # Instead, we directly append the event to the loan stream to trigger model_version_lock
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[{
            "event_type": "CreditAnalysisRequested",
            "event_version": 1,
            "payload": {"application_id": app_id},
        }],
        expected_version=2,  # after ApplicationSubmitted(0), CreditAnalysisRequested(1), CreditAnalysisCompleted(2)
    )

    # Now try the second credit analysis — should fail with model_version_lock
    # because credit_analysis_completed=True on the aggregate
    with pytest.raises(DomainError) as exc_info:
        await handle_credit_analysis_completed(cmd, store)

    assert exc_info.value.rule_violated == "model_version_lock"
