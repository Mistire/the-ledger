"""
tests/test_gas_town.py
======================
Property-based and example tests for Gas Town crash recovery.

Validates: Requirements 19.3 | Properties 18, 19
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
# STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════════

def decision_event_strategy():
    """
    Generate decision-type events that trigger NEEDS_RECONCILIATION when
    they are the last event in a session stream (no AgentSessionCompleted).
    """
    decision_event_types = [
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "DecisionGenerated",
        "ComplianceCheckCompleted",
    ]
    return st.fixed_dictionaries({
        "event_type": st.sampled_from(decision_event_types),
        "event_version": st.just(1),
        "payload": st.fixed_dictionaries({
            "application_id": st.uuids().map(str),
            "agent_id": st.just("test-agent"),
            "session_id": st.uuids().map(str),
        }),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def new_id() -> str:
    return str(uuid.uuid4())


def _make_generic_event(n: int) -> dict:
    """Build a generic agent event for stream population."""
    return {
        "event_type": "AgentInputValidated",
        "event_version": 1,
        "payload": {
            "step": n,
            "application_id": "app-test",
            "agent_id": "test-agent",
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 18 — NEEDS_RECONCILIATION when decision event is last
# Validates: Requirements 19.3
# ═══════════════════════════════════════════════════════════════════════════════

@given(decision_event_strategy())
@settings(max_examples=30)
def test_decision_event_without_completion_triggers_needs_reconciliation(event):
    """
    **Validates: Requirements 19.3**

    For any decision-type event appended after AgentSessionStarted with no
    AgentSessionCompleted, reconstruct_agent_context must return
    session_health_status="NEEDS_RECONCILIATION".
    """
    async def _run():
        store = InMemoryEventStore()
        agent_id = "test-agent"
        session_id = new_id()
        stream_id = f"agent-{agent_id}-{session_id}"

        # Append AgentSessionStarted
        await store.append(
            stream_id=stream_id,
            events=[{
                "event_type": "AgentSessionStarted",
                "event_version": 1,
                "payload": {
                    "session_id": session_id,
                    "agent_id": agent_id,
                    "agent_type": "credit_analysis",
                    "application_id": event["payload"].get("application_id", "app-001"),
                    "model_version": "v1",
                    "context_source": "test",
                    "context_token_count": 100,
                },
            }],
            expected_version=-1,
        )

        # Append the decision event (no AgentSessionCompleted follows)
        await store.append(
            stream_id=stream_id,
            events=[event],
            expected_version=0,
        )

        context = await reconstruct_agent_context(store, agent_id, session_id)

        assert context.session_health_status == "NEEDS_RECONCILIATION", (
            f"Expected NEEDS_RECONCILIATION for last event_type={event['event_type']!r}, "
            f"got {context.session_health_status!r}"
        )

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 19 — Token budget preserves last 3 events verbatim
# Validates: Requirements 19.3
# ═══════════════════════════════════════════════════════════════════════════════

@given(st.integers(min_value=5, max_value=50))
@settings(max_examples=20)
def test_token_budget_preserves_last_3_events(n_events):
    """
    **Validates: Requirements 19.3**

    Append N events to an agent session stream, then call
    reconstruct_agent_context with a very small token_budget (10 tokens)
    to force summarisation. The last 3 events must appear verbatim as
    JSON strings in context_text.
    """
    async def _run():
        store = InMemoryEventStore()
        agent_id = "test-agent"
        session_id = new_id()
        stream_id = f"agent-{agent_id}-{session_id}"

        # Append N generic events
        for i in range(n_events):
            await store.append(
                stream_id=stream_id,
                events=[_make_generic_event(i)],
                expected_version=i - 1,
            )

        # Load the stream to get the actual stored events (with store-assigned fields)
        stored_events = await store.load_stream(stream_id)
        last_3 = stored_events[-3:]

        # Call with a very small token_budget to force summarisation
        context = await reconstruct_agent_context(
            store, agent_id, session_id, token_budget=10
        )

        # Each of the last 3 events must appear verbatim in context_text
        for event in last_3:
            event_json = json.dumps(event, default=str)
            assert event_json in context.context_text, (
                f"Last 3 events not preserved verbatim in context_text.\n"
                f"Missing event: {event_json[:100]}...\n"
                f"context_text snippet: {context.context_text[:200]}"
            )

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# EXAMPLE — last_event_position is correct
# Validates: Requirements 19.3
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_last_event_position_is_correct():
    """
    **Validates: Requirements 19.3**

    Append 5 events to an agent session stream.
    reconstruct_agent_context must return last_event_position=4 (0-indexed).
    """
    store = InMemoryEventStore()
    agent_id = "test-agent"
    session_id = new_id()
    stream_id = f"agent-{agent_id}-{session_id}"

    for i in range(5):
        await store.append(
            stream_id=stream_id,
            events=[_make_generic_event(i)],
            expected_version=i - 1,
        )

    context = await reconstruct_agent_context(store, agent_id, session_id)

    assert context.last_event_position == 4, (
        f"Expected last_event_position=4 (0-indexed) for 5 events, "
        f"got {context.last_event_position}"
    )
