"""
tests/test_archive_stream.py
============================
Tests for archive_stream() on InMemoryEventStore.
No PostgreSQL required — uses InMemoryEventStore throughout.
"""
from __future__ import annotations

import uuid

import pytest

from ledger.event_store import InMemoryEventStore, DomainError


def _event(etype: str = "TestEvent") -> list[dict]:
    return [{"event_type": etype, "event_version": 1, "payload": {}}]


@pytest.mark.asyncio
async def test_archive_stream_prevents_append():
    """Archiving a stream must prevent future appends with DomainError(rule_violated='stream_archived')."""
    store = InMemoryEventStore()
    stream_id = f"test-archive-{uuid.uuid4()}"
    await store.append(stream_id, _event(), expected_version=-1)
    await store.archive_stream(stream_id)
    with pytest.raises(DomainError) as exc_info:
        await store.append(stream_id, _event(), expected_version=1)
    assert exc_info.value.rule_violated == "stream_archived"


@pytest.mark.asyncio
async def test_archived_stream_still_readable():
    """Archiving a stream must not prevent load_stream() from returning existing events."""
    store = InMemoryEventStore()
    stream_id = f"test-archive-read-{uuid.uuid4()}"
    await store.append(
        stream_id,
        [{"event_type": "TestEvent", "event_version": 1, "payload": {"x": 1}}],
        expected_version=-1,
    )
    await store.archive_stream(stream_id)
    events = await store.load_stream(stream_id)
    assert len(events) == 1
    assert events[0]["payload"]["x"] == 1


@pytest.mark.asyncio
async def test_get_stream_metadata_shows_archived_at():
    """After archiving, get_stream_metadata().archived_at must not be None."""
    store = InMemoryEventStore()
    stream_id = f"test-archive-meta-{uuid.uuid4()}"
    await store.append(stream_id, _event(), expected_version=-1)
    await store.archive_stream(stream_id)
    meta = await store.get_stream_metadata(stream_id)
    assert meta is not None
    assert meta.archived_at is not None
