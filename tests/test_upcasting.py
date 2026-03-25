"""
tests/test_upcasting.py
=======================
Property-based tests for upcasting immutability and chained upcasting.

Validates: Requirements 17.1–17.3 | Properties 14, 15
"""
from __future__ import annotations

import copy
import uuid

import pytest

from ledger.event_store import InMemoryEventStore, UpcasterRegistry
from ledger.upcasters import registry


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _v1_credit_analysis_event(app_id: str) -> dict:
    """Build a v1 CreditAnalysisCompleted payload — no v2 fields."""
    return {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 1,
        "payload": {
            "application_id": app_id,
            "agent_id": "credit-agent",
            "session_id": "sess-001",
            "model_version": "v1",
            "risk_tier": "LOW",
            "recommended_limit_usd": "500000",
            "analysis_duration_ms": 1000,
            "input_data_hash": "abc123",
            # Intentionally absent: model_versions, confidence_score, regulatory_basis
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 14 — Upcasting is in-memory only (raw payload unchanged)
# Validates: Requirements 17.1, 17.2
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_upcasting_does_not_mutate_raw_stored_event():
    """
    **Validates: Requirements 17.1, 17.2**

    Insert a v1 CreditAnalysisCompleted directly into InMemoryEventStore.
    Load through store.load_stream() and assert v2 fields are present on the
    loaded event. Then verify the raw stored event in store._streams still has
    the original v1 payload — upcasting must be in-memory only.
    """
    store = InMemoryEventStore(upcaster_registry=registry)
    app_id = str(uuid.uuid4())
    stream_id = f"credit-{app_id}"

    v1_event = _v1_credit_analysis_event(app_id)
    # Capture a deep copy of the original payload before appending
    original_payload = copy.deepcopy(v1_event["payload"])

    await store.append(
        stream_id=stream_id,
        events=[v1_event],
        expected_version=-1,
    )

    # Load through store — upcasting should be applied
    loaded_events = await store.load_stream(stream_id)
    assert len(loaded_events) == 1
    loaded = loaded_events[0]

    # v2 fields must be present after upcasting
    assert "model_versions" in loaded["payload"], "model_versions missing after upcast"
    assert "confidence_score" in loaded["payload"], "confidence_score missing after upcast"
    assert "regulatory_basis" in loaded["payload"], "regulatory_basis missing after upcast"

    # Verify defaults match the upcaster spec
    assert loaded["payload"]["model_versions"] == {}
    assert loaded["payload"]["confidence_score"] is None
    assert loaded["payload"]["regulatory_basis"] == []

    # Raw stored event in _streams must still have the original v1 payload
    raw_stored = store._streams[stream_id][0]
    assert "model_versions" not in raw_stored["payload"], (
        "model_versions should NOT be in raw stored payload — upcasting must be in-memory only"
    )
    assert "confidence_score" not in raw_stored["payload"], (
        "confidence_score should NOT be in raw stored payload"
    )
    assert "regulatory_basis" not in raw_stored["payload"], (
        "regulatory_basis should NOT be in raw stored payload"
    )

    # Raw payload must exactly match the original v1 payload
    assert raw_stored["payload"] == original_payload, (
        "Raw stored payload was mutated — upcasting must never modify stored data"
    )

    # Raw event version must still be 1
    assert raw_stored["event_version"] == 1, (
        "Raw stored event_version was mutated — must remain 1"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 15 — Chained upcasting (v1 → v2 → v3)
# Validates: Requirements 17.3
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_chained_upcasting_v1_to_v3():
    """
    **Validates: Requirements 17.3**

    Register a v2→v3 upcaster on a fresh UpcasterRegistry (alongside the
    existing v1→v2 upcaster). Insert a v1 CreditAnalysisCompleted event,
    load through the store, and assert that v3 fields are present — confirming
    the full v1→v2→v3 chain was applied.
    """
    # Build a fresh registry with both v1→v2 and v2→v3 upcasters
    chained_registry = UpcasterRegistry()

    @chained_registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
    def upcast_v1_to_v2(payload: dict) -> dict:
        payload = dict(payload)
        payload.setdefault("model_versions", {})
        payload.setdefault("confidence_score", None)
        payload.setdefault("regulatory_basis", [])
        return payload

    @chained_registry.upcaster("CreditAnalysisCompleted", from_version=2, to_version=3)
    def upcast_v2_to_v3(payload: dict) -> dict:
        payload = dict(payload)
        payload["upcasted_to_v3"] = True
        return payload

    store = InMemoryEventStore(upcaster_registry=chained_registry)
    app_id = str(uuid.uuid4())
    stream_id = f"credit-{app_id}"

    v1_event = _v1_credit_analysis_event(app_id)
    await store.append(
        stream_id=stream_id,
        events=[v1_event],
        expected_version=-1,
    )

    loaded_events = await store.load_stream(stream_id)
    assert len(loaded_events) == 1
    loaded = loaded_events[0]

    # v2 fields must be present (v1→v2 step)
    assert "model_versions" in loaded["payload"], "model_versions missing — v1→v2 step failed"
    assert "confidence_score" in loaded["payload"], "confidence_score missing — v1→v2 step failed"
    assert "regulatory_basis" in loaded["payload"], "regulatory_basis missing — v1→v2 step failed"

    # v3 field must be present (v2→v3 step)
    assert loaded["payload"].get("upcasted_to_v3") is True, (
        "upcasted_to_v3 missing — v2→v3 chained upcasting step failed"
    )

    # Final version should be 3
    assert loaded["event_version"] == 3, (
        f"Expected event_version=3 after chained upcasting, got {loaded['event_version']}"
    )

    # Raw stored event must still be v1 with original payload
    raw_stored = store._streams[stream_id][0]
    assert raw_stored["event_version"] == 1, "Raw stored event_version must remain 1"
    assert "upcasted_to_v3" not in raw_stored["payload"], (
        "upcasted_to_v3 must NOT appear in raw stored payload"
    )
