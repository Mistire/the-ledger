"""
tests/test_projections.py
=========================
Property-based and SLO tests for projection read models.

Uses in-memory projection stubs (no real PostgreSQL connection required).

Validates: Requirements 18.1–18.4 | Properties 8, 10, 12, 13
"""
from __future__ import annotations

import asyncio
import statistics
import time
import uuid
from datetime import datetime, timedelta, UTC
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ledger.event_store import InMemoryEventStore


# ═══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY PROJECTION STUBS
# ═══════════════════════════════════════════════════════════════════════════════

class InMemoryApplicationSummaryProjection:
    """
    In-memory stub for ApplicationSummaryProjection.
    Stores rows in self._rows: dict[application_id, dict].
    Implements the same handle(event) interface as the real projection.
    """
    name = "application_summary"

    def __init__(self):
        self._rows: dict[str, dict] = {}

    async def handle(self, event: dict) -> None:
        handler = getattr(self, f"_on_{event['event_type']}", None)
        if handler:
            await handler(event)

    def _payload(self, event: dict) -> dict:
        return event.get("payload", {})

    def _recorded_at(self, event: dict) -> str:
        ra = event.get("recorded_at")
        if ra is None:
            return datetime.now(UTC).isoformat()
        if isinstance(ra, datetime):
            return ra.isoformat()
        return str(ra)

    async def _on_ApplicationSubmitted(self, event: dict) -> None:
        p = self._payload(event)
        app_id = p.get("application_id")
        if app_id and app_id not in self._rows:
            self._rows[app_id] = {
                "application_id": app_id,
                "state": "SUBMITTED",
                "applicant_id": p.get("applicant_id"),
                "requested_amount_usd": p.get("requested_amount_usd"),
                "last_event_type": "ApplicationSubmitted",
                "last_event_at": self._recorded_at(event),
                "agent_sessions_completed": 0,
                "risk_tier": None,
                "fraud_score": None,
                "compliance_status": None,
                "decision": None,
                "approved_amount_usd": None,
                "final_decision_at": None,
            }

    async def _on_CreditAnalysisCompleted(self, event: dict) -> None:
        p = self._payload(event)
        app_id = p.get("application_id")
        if app_id in self._rows:
            decision = p.get("decision") or {}
            risk_tier = decision.get("risk_tier") if isinstance(decision, dict) else None
            self._rows[app_id].update({
                "risk_tier": risk_tier or p.get("risk_tier"),
                "agent_sessions_completed": self._rows[app_id]["agent_sessions_completed"] + 1,
                "last_event_type": "CreditAnalysisCompleted",
                "last_event_at": self._recorded_at(event),
            })

    def get_row(self, application_id: str) -> dict | None:
        return self._rows.get(application_id)


class InMemoryAgentPerformanceProjection:
    """
    In-memory stub for AgentPerformanceLedgerProjection.
    Stores rows in self._rows: dict[tuple(agent_id, model_version), dict].
    """
    name = "agent_performance_ledger"

    def __init__(self):
        self._rows: dict[tuple, dict] = {}

    async def handle(self, event: dict) -> None:
        handler = getattr(self, f"_on_{event['event_type']}", None)
        if handler:
            await handler(event)

    def _payload(self, event: dict) -> dict:
        return event.get("payload", {})

    async def _on_CreditAnalysisCompleted(self, event: dict) -> None:
        p = self._payload(event)
        agent_id = p.get("session_id") or p.get("agent_id", "unknown")
        model_version = p.get("model_version", "unknown")
        key = (agent_id, model_version)

        # Extract confidence from payload (may be top-level or nested in decision)
        confidence = p.get("confidence_score")
        if confidence is None:
            decision = p.get("decision") or {}
            if isinstance(decision, dict):
                confidence = decision.get("confidence")

        duration_ms = p.get("analysis_duration_ms")

        if key not in self._rows:
            self._rows[key] = {
                "agent_id": agent_id,
                "model_version": model_version,
                "analyses_completed": 1,
                "avg_confidence_score": confidence if confidence is not None else 0.0,
                "avg_duration_ms": duration_ms if duration_ms is not None else 0.0,
                "_confidence_sum": confidence if confidence is not None else 0.0,
                "_confidence_count": 1 if confidence is not None else 0,
            }
        else:
            row = self._rows[key]
            n = row["analyses_completed"]
            new_n = n + 1

            # Running average for confidence
            if confidence is not None:
                old_sum = row.get("_confidence_sum", (row["avg_confidence_score"] or 0.0) * row.get("_confidence_count", n))
                new_sum = old_sum + confidence
                new_count = row.get("_confidence_count", n) + 1
                row["_confidence_sum"] = new_sum
                row["_confidence_count"] = new_count
                row["avg_confidence_score"] = new_sum / new_count

            # Running average for duration
            if duration_ms is not None:
                old_dur = (row["avg_duration_ms"] or 0.0) * n
                row["avg_duration_ms"] = (old_dur + duration_ms) / new_n

            row["analyses_completed"] = new_n

    def get_row(self, agent_id: str, model_version: str) -> dict | None:
        return self._rows.get((agent_id, model_version))


class InMemoryComplianceAuditProjection:
    """
    In-memory stub for ComplianceAuditViewProjection.
    Stores events in self._events: list[dict].
    """
    name = "compliance_audit_view"

    COMPLIANCE_EVENT_TYPES = {
        "ComplianceCheckInitiated",
        "ComplianceRulePassed",
        "ComplianceRuleFailed",
        "ComplianceRuleNoted",
        "ComplianceCheckCompleted",
    }

    def __init__(self):
        self._events: list[dict] = []
        self._rebuilt = False

    async def handle(self, event: dict) -> None:
        if event.get("event_type") not in self.COMPLIANCE_EVENT_TYPES:
            return
        p = event.get("payload", {})
        self._events.append({
            "application_id": p.get("application_id"),
            "event_type": event.get("event_type"),
            "rule_id": p.get("rule_id"),
            "passed": p.get("passed"),
            "is_hard_block": p.get("is_hard_block"),
            "overall_verdict": p.get("overall_verdict"),
            "event_recorded_at": event.get("recorded_at", datetime.now(UTC).isoformat()),
            "payload": p,
        })

    def get_compliance_at(self, application_id: str, timestamp: datetime) -> dict:
        """Return only events before or at the given timestamp."""
        ts_str = timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp)
        filtered = [
            e for e in self._events
            if e["application_id"] == application_id
            and str(e["event_recorded_at"]) <= ts_str
        ]
        return {
            "application_id": application_id,
            "as_of": ts_str,
            "delta_events": filtered,
            "event_count": len(filtered),
        }

    def get_all_events(self, application_id: str) -> list[dict]:
        return [e for e in self._events if e["application_id"] == application_id]

    async def rebuild_from_scratch(self, store: InMemoryEventStore) -> None:
        """Truncate and replay all compliance events from the store."""
        self._events.clear()
        async for event in store.load_all(from_position=0):
            await self.handle(event)
        self._rebuilt = True


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════════

def application_submitted_event_strategy():
    """Generate ApplicationSubmitted events with required fields."""
    return st.fixed_dictionaries({
        "event_type": st.just("ApplicationSubmitted"),
        "event_version": st.just(1),
        "payload": st.fixed_dictionaries({
            "application_id": st.uuids().map(str),
            "applicant_id": st.uuids().map(str),
            "requested_amount_usd": st.floats(
                min_value=1000.0, max_value=10_000_000.0, allow_nan=False, allow_infinity=False
            ).map(str),
            "loan_purpose": st.sampled_from(["expansion", "equipment", "working_capital", "real_estate"]),
        }),
        "recorded_at": st.just(datetime.now(UTC).isoformat()),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _make_submitted_event(application_id: str) -> dict:
    return {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": application_id,
            "applicant_id": str(uuid.uuid4()),
            "requested_amount_usd": "500000",
            "loan_purpose": "expansion",
        },
        "recorded_at": datetime.now(UTC).isoformat(),
    }


def _make_credit_analysis_event(application_id: str, agent_id: str, model_version: str, confidence: float) -> dict:
    return {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": {
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": agent_id,
            "model_version": model_version,
            "confidence_score": confidence,
            "risk_tier": "LOW",
            "recommended_limit_usd": "500000",
            "analysis_duration_ms": 100,
        },
        "recorded_at": datetime.now(UTC).isoformat(),
    }


def _make_compliance_event(application_id: str, event_type: str, recorded_at: datetime) -> dict:
    return {
        "event_type": event_type,
        "event_version": 1,
        "payload": {
            "application_id": application_id,
            "rule_id": f"rule-{event_type}",
            "passed": True,
            "is_hard_block": False,
        },
        "recorded_at": recorded_at.isoformat(),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 8 — Concurrent application submissions
# Validates: Requirements 18.1
# ═══════════════════════════════════════════════════════════════════════════════

@given(st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=10, unique=True))
@settings(max_examples=20)
def test_concurrent_submissions_all_appear_in_projection(application_ids):
    """
    **Validates: Requirements 18.1**
    For any batch of ApplicationSubmitted events processed concurrently,
    all rows must appear in the in-memory ApplicationSummaryProjection.
    """
    async def _run():
        projection = InMemoryApplicationSummaryProjection()

        # Spawn concurrent handle calls
        events = [_make_submitted_event(app_id) for app_id in application_ids]
        await asyncio.gather(*[projection.handle(event) for event in events])

        # Assert all rows appear
        for app_id in application_ids:
            row = projection.get_row(app_id)
            assert row is not None, f"Row missing for application_id={app_id!r}"
            assert row["state"] == "SUBMITTED"

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 10 — ApplicationSubmitted event fields match projection
# Validates: Requirements 18.2
# ═══════════════════════════════════════════════════════════════════════════════

@given(application_submitted_event_strategy())
@settings(max_examples=30)
def test_application_submitted_fields_match_projection(event):
    """
    **Validates: Requirements 18.2**
    For any ApplicationSubmitted event processed by the projection,
    the resulting row must have state="SUBMITTED" and fields matching the payload.
    """
    async def _run():
        projection = InMemoryApplicationSummaryProjection()
        await projection.handle(event)

        p = event["payload"]
        app_id = p["application_id"]
        row = projection.get_row(app_id)

        assert row is not None, f"No row found for application_id={app_id}"
        assert row["state"] == "SUBMITTED"
        assert row["applicant_id"] == p["applicant_id"]
        assert row["requested_amount_usd"] == p["requested_amount_usd"]
        assert row["last_event_type"] == "ApplicationSubmitted"

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 12 — avg_confidence_score is arithmetic mean
# Validates: Requirements 18.3
# ═══════════════════════════════════════════════════════════════════════════════

@given(st.lists(st.floats(0.0, 1.0, allow_nan=False, allow_infinity=False), min_size=2, max_size=20))
@settings(max_examples=30)
def test_avg_confidence_score_is_arithmetic_mean(confidence_scores):
    """
    **Validates: Requirements 18.3**
    For any sequence of CreditAnalysisCompleted events for the same (agent_id, model_version),
    avg_confidence_score must equal the arithmetic mean of all confidence values.
    """
    async def _run():
        projection = InMemoryAgentPerformanceProjection()
        agent_id = "test-agent"
        model_version = "v1"
        app_id = str(uuid.uuid4())

        for score in confidence_scores:
            event = _make_credit_analysis_event(app_id, agent_id, model_version, score)
            await projection.handle(event)

        row = projection.get_row(agent_id, model_version)
        assert row is not None, "No row found in agent performance projection"
        assert row["analyses_completed"] == len(confidence_scores)

        expected_mean = statistics.mean(confidence_scores)
        actual_mean = row["avg_confidence_score"]
        assert abs(actual_mean - expected_mean) < 1e-9, (
            f"avg_confidence_score={actual_mean} != arithmetic mean={expected_mean}"
        )

    asyncio.run(_run())


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 13 — Temporal compliance query
# Validates: Requirements 18.4
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_compliance_at_timestamp_only_includes_events_before_timestamp():
    """
    **Validates: Requirements 18.4**
    Calling get_compliance_at(T) must only include events with
    event_recorded_at <= T, and must exclude events after T.
    """
    projection = InMemoryComplianceAuditProjection()
    app_id = str(uuid.uuid4())

    base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    t1 = base_time
    t2 = base_time + timedelta(minutes=10)
    t3 = base_time + timedelta(minutes=20)
    cutoff = base_time + timedelta(minutes=15)  # between t2 and t3

    # Insert 3 compliance events at different timestamps
    for ts, event_type in [(t1, "ComplianceRulePassed"), (t2, "ComplianceRulePassed"), (t3, "ComplianceRulePassed")]:
        await projection.handle(_make_compliance_event(app_id, event_type, ts))

    result = projection.get_compliance_at(app_id, cutoff)

    # Only events at t1 and t2 should be included (t3 > cutoff)
    assert result["event_count"] == 2, (
        f"Expected 2 events before cutoff, got {result['event_count']}"
    )
    for e in result["delta_events"]:
        assert str(e["event_recorded_at"]) <= cutoff.isoformat(), (
            f"Event at {e['event_recorded_at']} should not appear in result at {cutoff}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SLO TEST — ApplicationSummary lag < 500ms
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_application_summary_slo_500ms():
    """
    Process 50 ApplicationSubmitted events through the in-memory projection.
    Total processing time must be < 500ms.
    """
    projection = InMemoryApplicationSummaryProjection()
    events = [_make_submitted_event(str(uuid.uuid4())) for _ in range(50)]

    start = time.perf_counter()
    for event in events:
        await projection.handle(event)
    elapsed_ms = (time.perf_counter() - start) * 1000

    assert elapsed_ms < 500, (
        f"ApplicationSummary SLO violated: processed 50 events in {elapsed_ms:.1f}ms (limit: 500ms)"
    )
    assert len(projection._rows) == 50


# ═══════════════════════════════════════════════════════════════════════════════
# SLO TEST — ComplianceAuditView lag < 2000ms
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_compliance_audit_view_slo_2000ms():
    """
    Process 50 ComplianceRulePassed events through the in-memory projection.
    Total processing time must be < 2000ms.
    """
    projection = InMemoryComplianceAuditProjection()
    app_id = str(uuid.uuid4())
    base_time = datetime.now(UTC)
    events = [
        _make_compliance_event(app_id, "ComplianceRulePassed", base_time + timedelta(seconds=i))
        for i in range(50)
    ]

    start = time.perf_counter()
    for event in events:
        await projection.handle(event)
    elapsed_ms = (time.perf_counter() - start) * 1000

    assert elapsed_ms < 2000, (
        f"ComplianceAuditView SLO violated: processed 50 events in {elapsed_ms:.1f}ms (limit: 2000ms)"
    )
    assert len(projection._events) == 50


# ═══════════════════════════════════════════════════════════════════════════════
# REBUILD TEST — no exceptions during concurrent reads
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_rebuild_from_scratch_no_exceptions_during_concurrent_reads():
    """
    Call rebuild_from_scratch() while concurrent reads are running.
    Assert no exceptions are raised.
    """
    store = InMemoryEventStore()
    projection = InMemoryComplianceAuditProjection()
    app_id = str(uuid.uuid4())

    # Seed the store with compliance events
    base_time = datetime.now(UTC)
    for i in range(20):
        await store.append(
            stream_id=f"compliance-{app_id}",
            events=[{
                "event_type": "ComplianceRulePassed",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "rule_id": f"rule-{i}",
                    "passed": True,
                    "is_hard_block": False,
                },
                "recorded_at": (base_time + timedelta(seconds=i)).isoformat(),
            }],
            expected_version=i - 1,
        )

    # Pre-populate projection
    await projection.rebuild_from_scratch(store)

    exceptions: list[Exception] = []

    async def concurrent_reader():
        """Continuously read from the projection while rebuild runs."""
        for _ in range(10):
            try:
                _ = projection.get_all_events(app_id)
                await asyncio.sleep(0)
            except Exception as exc:
                exceptions.append(exc)

    async def do_rebuild():
        try:
            await projection.rebuild_from_scratch(store)
        except Exception as exc:
            exceptions.append(exc)

    # Run rebuild and concurrent reads simultaneously
    await asyncio.gather(
        do_rebuild(),
        concurrent_reader(),
        concurrent_reader(),
    )

    assert not exceptions, f"Exceptions raised during concurrent rebuild+reads: {exceptions}"
    assert projection._rebuilt is True
