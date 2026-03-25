"""
tests/test_gas_town.py
======================
Property-based and example tests for Gas Town crash recovery.

Validates: Requirements 19.1–19.3 | Properties 18, 19
"""
from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ledger.event_store import InMemoryEventStore
from ledger.integrity.gas_town import reconstruct_agent_context


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def new_id() -> str:
    return str(uuid.uuid4())


# Decision-type events that trigger NEEDS_RECONCILIATION
DECISION_EVENT_TYPES = [
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "DecisionGenerated",
    "ComplianceCheckCompleted",
]


def _make_session_started_event(agent_id: str, session_id: str, app_id: str) -> dict:
    return {
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
    }


def _make_decision_event(event_type: str, agent_id: str, session_id: str, app_id: str) -> dict:
    return {
        "event_type": event_type,
        "event_version": 1,
        "payload": {
            "application_id": app_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "model_version": "v1",
        },
    }


def _make_generic_event(n: int, app_id: str) -> dict:
    """Create a generic agent event for padding the stream."""
    return {
        "event_type": "AgentNodeExecuted",
        "event_version": 1,
        "payload": {
            "application_id": app_id,
            "node_name": f"node_{n}",
            "step": n,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════════

def decision_event_strategy():
    """Generate one of the four decision-type event types."""
    return st.sampled_from(DECISION_EVENT_TYPES)


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 18 — NEEDS_RECONCILIATION when last event is decision-type
# Validates: Requirements 19.1, 19.3
# ═══════════════════════════════════════════════════════════════════════════════

@given(decision_event_strategy())
@settings(max_examples=20)
def test_needs_reconciliation_when_decision_event_without_completion(event_type: str):
    """
    **Validates: Requirements 19.1, 19.3**

    When the last event in an agent session stream is a decision-type event
    (CreditAnalysisCompleted, FraudScreeningCompleted, DecisionGenerated,
    ComplianceCheckCompleted) and there is no AgentSessionCompleted event,
    reconstruct_agent_context must return session_health_status="NEEDS_RECONCILIATION".
    """
    async def _run():
        store = InMemoryEventStore()
        agent_id = new_id()
        session_id = new_id()
        app_id = new_id()
        stream_id = f"agent-{agent_id}-{session_id}"

        # Append AgentSessionStarted first
        await store.append(
            stream_id=stream_id,
            events=[_make_session_started_event(agent_id, session_id, app_id)],
            expected_version=-1,
        )

        # Append the decision-type event — no AgentSessionCompleted follows
        await store.append(
            stream_id=stream_id,
            events=[_make_decision_event(event_type, agent_id, session_id, app_id)],
            expected_version=0,
        )

        ctx = await reconstruct_agent_context(store, agent_id, session_id)

        assert ctx.session_health_status == "NEEDS_RECONCILIATION", (
            f"Expected NEEDS_RECONCILIATION for last event={event_type}, "
            f"got {ctx.session_health_status}"
        )

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 19 — Last 3 events preserved verbatim under small token budget
# Validates: Requirements 19.2
# ═══════════════════════════════════════════════════════════════════════════════

@given(st.integers(min_value=5, max_value=50))
@settings(max_examples=20)
def test_last_3_events_preserved_verbatim_under_small_token_budget(n: int):
    """
    **Validates: Requirements 19.2**

    When N events are appended to an agent session stream and
    reconstruct_agent_context is called with a very small token_budget (10),
    the last 3 events must appear verbatim in context_text.
    """
    async def _run():
        store = InMemoryEventStore()
        agent_id = new_id()
        session_id = new_id()
        app_id = new_id()
        stream_id = f"agent-{agent_id}-{session_id}"

        # Append N generic events
        all_events = []
        for i in range(n):
            evt = _make_generic_event(i, app_id)
            all_events.append(evt)

        # Append in batches of 1 to track expected_version
        for i, evt in enumerate(all_events):
            await store.append(
                stream_id=stream_id,
                events=[evt],
                expected_version=i - 1,
            )

        # Call with a very small token_budget to force summarisation
        ctx = await reconstruct_agent_context(store, agent_id, session_id, token_budget=10)

        # Load the stream to get the actual stored events (with store-assigned fields)
        stored_events = await store.load_stream(stream_id)
        last_3 = stored_events[-3:]

        # Each of the last 3 events must appear verbatim in context_text
        for evt in last_3:
            evt_json = json.dumps(evt, default=str)
            assert evt_json in ctx.context_text, (
                f"Expected last 3 events to appear verbatim in context_text. "
                f"Missing: {evt_json[:80]}..."
            )

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# EXAMPLE — 5 events appended, last_event_position=4
# Validates: Requirements 19.1
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_last_event_position_is_zero_indexed():
    """
    **Validates: Requirements 19.1**

    When exactly 5 events are appended to an agent session stream,
    reconstruct_agent_context must return last_event_position=4 (0-indexed).
    """
    store = InMemoryEventStore()
    agent_id = new_id()
    session_id = new_id()
    app_id = new_id()
    stream_id = f"agent-{agent_id}-{session_id}"

    # Append exactly 5 events
    for i in range(5):
        await store.append(
            stream_id=stream_id,
            events=[_make_generic_event(i, app_id)],
            expected_version=i - 1,
        )

    ctx = await reconstruct_agent_context(store, agent_id, session_id)

    assert ctx.last_event_position == 4, (
        f"Expected last_event_position=4 for 5 events (0-indexed), "
        f"got {ctx.last_event_position}"
    )
