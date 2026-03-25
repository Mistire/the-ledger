"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
Append-only event store with optimistic concurrency control.

6 async methods (per rubric):
  1. stream_version()      — current version or -1 if not found
  2. append()              — atomic append with OCC + outbox write
  3. load_stream()         — load events by stream, with upcasting
  4. load_all()            — async generator for projection daemon
  5. get_event()           — single event lookup by ID
  6. get_stream_metadata() — stream-level state
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass, field as dc_field
from datetime import datetime, UTC
from typing import AsyncGenerator
from uuid import UUID, uuid4

import asyncpg


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS — Typed, structured (per rubric)
# ═══════════════════════════════════════════════════════════════════════════════

class OptimisticConcurrencyError(Exception):
    """
    Raised when expected_version doesn't match current stream version.
    Structured fields for LLM-consumable error handling.
    """
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        self.error_type = "OptimisticConcurrencyError"
        self.suggested_action = "reload_stream_and_retry"
        super().__init__(
            f"OCC on '{stream_id}': expected v{expected}, actual v{actual}"
        )


class DomainError(Exception):
    """
    Raised when a business rule is violated in aggregate domain logic.
    Structured fields for precise error reporting.
    """
    def __init__(self, aggregate_id: str, rule_violated: str, message: str):
        self.aggregate_id = aggregate_id
        self.rule_violated = rule_violated
        self.error_type = "DomainError"
        super().__init__(f"Domain error on '{aggregate_id}' [{rule_violated}]: {message}")


# ═══════════════════════════════════════════════════════════════════════════════
# STORED EVENT — Store-assigned envelope (separate from BaseEvent)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class StoredEvent:
    """
    Store-assigned envelope wrapping a domain event after persistence.
    Cleanly separated from BaseEvent — domain fields live in payload.
    """
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: dict
    metadata: dict
    recorded_at: datetime

    @classmethod
    def from_row(cls, row: dict) -> "StoredEvent":
        return cls(
            event_id=row["event_id"],
            stream_id=row["stream_id"],
            stream_position=row["stream_position"],
            global_position=row.get("global_position", 0),
            event_type=row["event_type"],
            event_version=row.get("event_version", 1),
            payload=dict(row.get("payload", {})),
            metadata=dict(row.get("metadata", {})),
            recorded_at=row.get("recorded_at", datetime.now(UTC)),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# STREAM METADATA
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class StreamMetadata:
    """Stream-level state model."""
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None = None
    metadata: dict = dc_field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════════
# UPCASTER REGISTRY — Phase 4
# ═══════════════════════════════════════════════════════════════════════════════

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.
    """

    def __init__(self):
        self._upcasters: dict[str, dict[int, callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        """Apply chain of upcasters until latest version reached."""
        et = event["event_type"]
        v = event.get("event_version", 1)
        chain = self._upcasters.get(et, {})
        while v in chain:
            event["payload"] = chain[v](dict(event["payload"]))
            v += 1
            event["event_version"] = v
        return event


# ═══════════════════════════════════════════════════════════════════════════════
# POSTGRESQL EVENT STORE
# ═══════════════════════════════════════════════════════════════════════════════

class EventStore:
    """
    Append-only PostgreSQL event store with optimistic concurrency control.
    All agents and projections use this class.
    """

    def __init__(self, db_url: str, upcaster_registry: UpcasterRegistry | None = None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=10)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    # ── 1. stream_version ────────────────────────────────────────────────────

    async def stream_version(self, stream_id: str) -> int:
        """Returns current version, or -1 if stream doesn't exist."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return row["current_version"] if row else -1

    # ── 2. append ────────────────────────────────────────────────────────────

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Atomically appends events with OCC enforcement at DATABASE level.
        Writes to both events AND outbox in a single transaction.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row at DB level (SELECT ... FOR UPDATE)
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE",
                    stream_id,
                )

                # 2. OCC check — enforced at database level
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(
                        stream_id, expected_version, current
                    )

                # 3. Create stream if new
                if row is None:
                    aggregate_type = stream_id.split("-")[0]
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id, aggregate_type,
                    )

                # 4. Build metadata with causal chain
                meta = {**(metadata or {})}
                if correlation_id:
                    meta["correlation_id"] = correlation_id
                if causation_id:
                    meta["causation_id"] = causation_id

                # 5. Insert each event + outbox entry in same transaction
                positions = []
                for i, event in enumerate(events):
                    pos = expected_version + 1 + i
                    event_id = uuid4()
                    recorded_at = datetime.now(UTC)

                    await conn.execute(
                        "INSERT INTO events(event_id, stream_id, stream_position,"
                        " event_type, event_version, payload, metadata, recorded_at)"
                        " VALUES($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)",
                        event_id, stream_id, pos,
                        event["event_type"],
                        event.get("event_version", 1),
                        json.dumps(event.get("payload", {})),
                        json.dumps(meta),
                        recorded_at,
                    )

                    # Outbox write — same transaction for guaranteed delivery
                    await conn.execute(
                        "INSERT INTO outbox(event_id, destination, payload)"
                        " VALUES($1, $2, $3::jsonb)",
                        event_id, "default",
                        json.dumps({
                            "event_type": event["event_type"],
                            "stream_id": stream_id,
                            "stream_position": pos,
                            "payload": event.get("payload", {}),
                        }),
                    )

                    positions.append(pos)

                # 6. Update stream version
                new_version = expected_version + len(events)
                await conn.execute(
                    "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                    new_version, stream_id,
                )
                return positions

    # ── 3. load_stream ───────────────────────────────────────────────────────

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        """Loads events from a stream with optional upcasting."""
        async with self._pool.acquire() as conn:
            q = (
                "SELECT event_id, stream_id, stream_position, event_type,"
                " event_version, payload, metadata, recorded_at"
                " FROM events WHERE stream_id=$1 AND stream_position>=$2"
            )
            params: list = [stream_id, from_position]
            if to_position is not None:
                q += " AND stream_position<=$3"
                params.append(to_position)
            q += " ORDER BY stream_position ASC"
            rows = await conn.fetch(q, *params)
            events = []
            for row in rows:
                e = {
                    **dict(row),
                    "payload": dict(row["payload"]),
                    "metadata": dict(row["metadata"]),
                }
                if self.upcasters:
                    e = self.upcasters.upcast(e)
                events.append(e)
            return events

    # ── 4. load_all ──────────────────────────────────────────────────────────

    async def load_all(
        self, from_position: int = 0, batch_size: int = 500
    ) -> AsyncGenerator[dict, None]:
        """Async generator for projection daemon catch-up."""
        async with self._pool.acquire() as conn:
            pos = from_position
            while True:
                rows = await conn.fetch(
                    "SELECT global_position, event_id, stream_id, stream_position,"
                    " event_type, event_version, payload, metadata, recorded_at"
                    " FROM events WHERE global_position > $1"
                    " ORDER BY global_position ASC LIMIT $2",
                    pos, batch_size,
                )
                if not rows:
                    break
                for row in rows:
                    e = {
                        **dict(row),
                        "payload": dict(row["payload"]),
                        "metadata": dict(row["metadata"]),
                    }
                    if self.upcasters:
                        e = self.upcasters.upcast(e)
                    yield e
                pos = rows[-1]["global_position"]
                if len(rows) < batch_size:
                    break

    # ── 5. get_event ─────────────────────────────────────────────────────────

    async def get_event(self, event_id: UUID) -> dict | None:
        """Loads one event by UUID. Used for causation chain lookups."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT event_id, stream_id, stream_position, global_position,"
                " event_type, event_version, payload, metadata, recorded_at"
                " FROM events WHERE event_id=$1",
                event_id,
            )
            if not row:
                return None
            return {
                **dict(row),
                "payload": dict(row["payload"]),
                "metadata": dict(row["metadata"]),
            }

    # ── 6. get_stream_metadata ───────────────────────────────────────────────

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        """Returns stream-level state model."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT stream_id, aggregate_type, current_version,"
                " created_at, archived_at, metadata"
                " FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            if not row:
                return None
            return StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=dict(row["metadata"]) if row["metadata"] else {},
            )


# ═══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY EVENT STORE — for tests only (thread-safe with asyncio locks)
# ═══════════════════════════════════════════════════════════════════════════════

class InMemoryEventStore:
    """
    Thread-safe in-memory event store for unit tests.
    Identical interface to EventStore — swap transparently in conftest.py.
    """

    def __init__(self, upcaster_registry: UpcasterRegistry | None = None):
        self.upcasters = upcaster_registry
        self._streams: dict[str, list[dict]] = defaultdict(list)
        self._versions: dict[str, int] = {}
        self._global: list[dict] = []
        self._checkpoints: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(
                    stream_id, expected_version, current
                )

            meta = {**(metadata or {})}
            if correlation_id:
                meta["correlation_id"] = correlation_id
            if causation_id:
                meta["causation_id"] = causation_id

            positions = []
            for i, event in enumerate(events):
                pos = current + 1 + i
                stored = {
                    "event_id": str(uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global),
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                    "metadata": meta,
                    "recorded_at": datetime.now(UTC).isoformat(),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = current + len(events)
            return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        events = [
            e for e in self._streams.get(stream_id, [])
            if e["stream_position"] >= from_position
            and (to_position is None or e["stream_position"] <= to_position)
        ]
        result = sorted(events, key=lambda e: e["stream_position"])
        if self.upcasters:
            result = [self.upcasters.upcast(dict(e)) for e in result]
        return result

    async def load_all(self, from_position: int = 0, batch_size: int = 500):
        for e in self._global:
            if e["global_position"] >= from_position:
                yield dict(e)

    async def get_event(self, event_id) -> dict | None:
        eid = str(event_id)
        for e in self._global:
            if e["event_id"] == eid:
                return dict(e)
        return None

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        if stream_id not in self._versions:
            return None
        return StreamMetadata(
            stream_id=stream_id,
            aggregate_type=stream_id.split("-")[0],
            current_version=self._versions[stream_id],
            created_at=datetime.now(UTC),
        )

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
