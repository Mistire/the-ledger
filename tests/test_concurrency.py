"""
tests/test_concurrency.py
=========================
Double-decision concurrency test + full EventStore test suite.

All 3 rubric-required assertions in the concurrency test:
  1. Exactly one concurrent append succeeds
  2. Exactly one raises OptimisticConcurrencyError
  3. Final stream version is correct
"""
import asyncio
import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


# ─── THE CRITICAL CONCURRENCY TEST ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_double_decision_concurrency():
    """
    Two AI agents simultaneously attempt to append to the same stream.
    Both pass expected_version=2. Exactly one must win.
    """
    store = InMemoryEventStore()

    stream_id = "loan-APEX-TEST-001"
    await store.append(stream_id, [_ev("ApplicationSubmitted", application_id="APEX-TEST-001")], expected_version=-1)
    await store.append(stream_id, [_ev("CreditAnalysisRequested", application_id="APEX-TEST-001")], expected_version=0)
    await store.append(stream_id, [_ev("FraudScreeningCompleted", application_id="APEX-TEST-001")], expected_version=1)
    assert await store.stream_version(stream_id) == 2

    results = []

    async def agent_append(agent_name: str):
        try:
            await store.append(
                stream_id,
                [_ev("CreditAnalysisCompleted", application_id="APEX-TEST-001", agent_id=agent_name)],
                expected_version=2,
            )
            results.append(("success", agent_name))
        except OptimisticConcurrencyError as e:
            results.append(("occ", agent_name, e))

    await asyncio.gather(agent_append("agent-001"), agent_append("agent-002"))

    # ASSERTION 1: Exactly one succeeds
    successes = [r for r in results if r[0] == "success"]
    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}"

    # ASSERTION 2: Exactly one OCC error
    occ_errors = [r for r in results if r[0] == "occ"]
    assert len(occ_errors) == 1, f"Expected 1 OCC error, got {len(occ_errors)}"
    occ_exc = occ_errors[0][2]
    assert occ_exc.stream_id == stream_id
    assert occ_exc.expected == 2
    assert occ_exc.actual == 3

    # ASSERTION 3: Final stream version correct
    assert await store.stream_version(stream_id) == 3
    all_events = await store.load_stream(stream_id)
    assert len(all_events) == 4


# ─── OCC STRUCTURED FIELDS ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_occ_error_has_structured_fields():
    store = InMemoryEventStore()
    await store.append("s", [_ev("E1")], expected_version=-1)

    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await store.append("s", [_ev("E2")], expected_version=-1)

    err = exc_info.value
    assert err.stream_id == "s"
    assert err.expected == -1
    assert err.actual == 0
    assert err.error_type == "OptimisticConcurrencyError"
    assert err.suggested_action == "reload_stream_and_retry"


# ─── CORE EVENT STORE TESTS ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_new_stream_version_is_minus_one():
    store = InMemoryEventStore()
    assert await store.stream_version("nonexistent") == -1


@pytest.mark.asyncio
async def test_append_new_stream_succeeds():
    store = InMemoryEventStore()
    positions = await store.append(
        "loan-APEX-0001",
        [_ev("ApplicationSubmitted", application_id="APEX-0001")],
        expected_version=-1,
    )
    assert positions == [0]
    assert await store.stream_version("loan-APEX-0001") == 0


@pytest.mark.asyncio
async def test_append_increments_version():
    store = InMemoryEventStore()
    await store.append("s", [_ev("E1")], expected_version=-1)
    await store.append("s", [_ev("E2")], expected_version=0)
    await store.append("s", [_ev("E3")], expected_version=1)
    assert await store.stream_version("s") == 2


@pytest.mark.asyncio
async def test_load_stream_returns_events_in_order():
    store = InMemoryEventStore()
    for i in range(5):
        ver = await store.stream_version("s")
        await store.append("s", [_ev(f"Event{i}", seq=i)], expected_version=ver)
    events = await store.load_stream("s")
    assert len(events) == 5
    for i, ev in enumerate(events):
        assert ev["stream_position"] == i
        assert ev["event_type"] == f"Event{i}"


@pytest.mark.asyncio
async def test_load_stream_with_position_range():
    store = InMemoryEventStore()
    for i in range(5):
        ver = await store.stream_version("s")
        await store.append("s", [_ev(f"E{i}")], expected_version=ver)
    events = await store.load_stream("s", from_position=2)
    assert len(events) == 3
    assert events[0]["stream_position"] == 2


@pytest.mark.asyncio
async def test_load_all_yields_all_events_globally():
    store = InMemoryEventStore()
    await store.append("s1", [_ev("E1"), _ev("E2")], expected_version=-1)
    await store.append("s2", [_ev("E3")], expected_version=-1)
    all_events = [e async for e in store.load_all(from_position=0)]
    assert len(all_events) == 3


@pytest.mark.asyncio
async def test_append_multiple_events_in_one_call():
    store = InMemoryEventStore()
    events = [_ev(f"E{i}") for i in range(3)]
    positions = await store.append("s", events, expected_version=-1)
    assert positions == [0, 1, 2]
    assert await store.stream_version("s") == 2


@pytest.mark.asyncio
async def test_causation_and_correlation_ids_threaded():
    store = InMemoryEventStore()
    await store.append(
        "s", [_ev("E1")], expected_version=-1,
        correlation_id="corr-123", causation_id="cause-456",
    )
    events = await store.load_stream("s")
    assert events[0]["metadata"]["correlation_id"] == "corr-123"
    assert events[0]["metadata"]["causation_id"] == "cause-456"


@pytest.mark.asyncio
async def test_checkpoints_persist():
    store = InMemoryEventStore()
    assert await store.load_checkpoint("proj_a") == 0
    await store.save_checkpoint("proj_a", 42)
    assert await store.load_checkpoint("proj_a") == 42
