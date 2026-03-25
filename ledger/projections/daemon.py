"""
ledger/projections/daemon.py — ProjectionDaemon
================================================
Async polling daemon that reads from the event store and dispatches
events to registered projections. Runs as a background asyncio task.

Polling strategy:
  - Read min last_position across all registered projections' checkpoints
  - Fetch next batch from global_position > min_checkpoint
  - Dispatch each event to every registered projection independently
  - Update per-projection checkpoint after each successful batch

Dead-letter log: in-memory list[dict] — not persisted.
Checkpoint persistence: projection_checkpoints table.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, UTC
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Polls the events table by global_position, dispatches to registered projections.
    Runs as a background asyncio task.
    """

    def __init__(
        self,
        store,
        pool: asyncpg.Pool,
        batch_size: int = 500,
        poll_interval_s: float = 0.1,
        max_retries: int = 3,
    ):
        self.store = store
        self.pool = pool
        self.batch_size = batch_size
        self.poll_interval_s = poll_interval_s
        self.max_retries = max_retries

        self._projections: list[Any] = []
        self._checkpoints: dict[str, int] = {}          # projection_name → last global_position
        self._last_processed_at: dict[str, datetime] = {}  # projection_name → timestamp
        self._dead_letter: list[dict] = []
        self._running = False
        self._task: asyncio.Task | None = None

    # ── Registration ─────────────────────────────────────────────────────────

    def register(self, projection) -> None:
        """Register a projection. Must implement handle(event) and have a name attribute."""
        if not hasattr(projection, "handle") or not hasattr(projection, "name"):
            raise ValueError(
                f"Projection {projection!r} must implement handle(event) and have a name attribute"
            )
        self._projections.append(projection)
        # Initialise checkpoint to 0 if not already loaded
        if projection.name not in self._checkpoints:
            self._checkpoints[projection.name] = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the polling loop as an asyncio task."""
        await self._load_checkpoints()
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("ProjectionDaemon started with %d projections", len(self._projections))

    async def stop(self) -> None:
        """Gracefully stop the polling loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ProjectionDaemon stopped")

    # ── Lag reporting ─────────────────────────────────────────────────────────

    async def get_lag(self, projection_name: str) -> int:
        """Returns milliseconds between latest event recorded_at and last processed."""
        latest_at = await self._get_latest_event_recorded_at()
        last_at = self._last_processed_at.get(projection_name)
        if latest_at is None or last_at is None:
            return 0
        delta = latest_at - last_at
        return max(0, int(delta.total_seconds() * 1000))

    async def get_all_lags(self) -> dict[str, int]:
        """Returns lag in milliseconds for every registered projection."""
        result = {}
        for proj in self._projections:
            result[proj.name] = await self.get_lag(proj.name)
        return result

    # ── Main polling loop ─────────────────────────────────────────────────────

    async def _run(self) -> None:
        """Main polling loop. Reads from lowest checkpoint across all projections."""
        while self._running:
            try:
                if not self._projections:
                    await asyncio.sleep(self.poll_interval_s)
                    continue

                min_checkpoint = min(self._checkpoints.get(p.name, 0) for p in self._projections)

                batch = await self._fetch_batch(min_checkpoint)
                if not batch:
                    await asyncio.sleep(self.poll_interval_s)
                    continue

                # Dispatch each event to every projection independently
                for event in batch:
                    for projection in self._projections:
                        proj_checkpoint = self._checkpoints.get(projection.name, 0)
                        # Only process events the projection hasn't seen yet
                        if event["global_position"] > proj_checkpoint:
                            await self._process_event(projection, event)

                # Update checkpoints per-projection
                for projection in self._projections:
                    new_pos = max(
                        (e["global_position"] for e in batch
                         if e["global_position"] > self._checkpoints.get(projection.name, 0)),
                        default=None,
                    )
                    if new_pos is not None:
                        await self._save_checkpoint(projection.name, new_pos)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Unexpected error in ProjectionDaemon._run: %s", exc)
                await asyncio.sleep(self.poll_interval_s)

    async def _fetch_batch(self, from_position: int) -> list[dict]:
        """Fetch next batch of events from global_position > from_position."""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT global_position, event_id, stream_id, stream_position,"
                    " event_type, event_version, payload, metadata, recorded_at"
                    " FROM events WHERE global_position > $1"
                    " ORDER BY global_position ASC LIMIT $2",
                    from_position,
                    self.batch_size,
                )
                return [
                    {
                        **dict(row),
                        "payload": dict(row["payload"]),
                        "metadata": dict(row["metadata"]),
                    }
                    for row in rows
                ]
        except Exception:
            # Fall back to store.load_all for InMemoryEventStore (tests)
            batch = []
            async for event in self.store.load_all(from_position=from_position, batch_size=self.batch_size):
                batch.append(event)
            return batch

    # ── Per-event retry loop ──────────────────────────────────────────────────

    async def _process_event(self, projection, event: dict) -> None:
        """Retry loop with exponential backoff. Records to dead-letter on exhaustion."""
        delays = [0.1, 0.2, 0.4]
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                await projection.handle(event)
                # Track last processed timestamp for lag calculation
                recorded_at = event.get("recorded_at")
                if recorded_at is not None:
                    if isinstance(recorded_at, str):
                        try:
                            recorded_at = datetime.fromisoformat(recorded_at)
                        except ValueError:
                            recorded_at = None
                    if recorded_at is not None:
                        self._last_processed_at[projection.name] = recorded_at
                return
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(delays[attempt])
                    logger.warning(
                        "Projection %s failed on event %s (attempt %d/%d): %s",
                        projection.name,
                        event.get("event_type"),
                        attempt + 1,
                        self.max_retries,
                        exc,
                    )

        # All retries exhausted — dead-letter
        self._dead_letter.append({
            "projection_name": projection.name,
            "event": event,
            "attempts": self.max_retries,
            "last_error": str(last_error),
            "skipped_at": datetime.now(UTC).isoformat(),
        })
        logger.error(
            "Dead-lettered event %s for projection %s after %d attempts: %s",
            event.get("event_type"),
            projection.name,
            self.max_retries,
            last_error,
        )

    # ── Checkpoint persistence ────────────────────────────────────────────────

    async def _load_checkpoints(self) -> None:
        """Load checkpoints from projection_checkpoints table (or in-memory store)."""
        for projection in self._projections:
            try:
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT last_position FROM projection_checkpoints"
                        " WHERE projection_name = $1",
                        projection.name,
                    )
                    if row:
                        self._checkpoints[projection.name] = row["last_position"]
                    else:
                        self._checkpoints[projection.name] = 0
            except Exception:
                # Fall back to in-memory store checkpoint (for tests)
                try:
                    pos = await self.store.load_checkpoint(projection.name)
                    self._checkpoints[projection.name] = pos
                except Exception:
                    self._checkpoints[projection.name] = 0

    async def _save_checkpoint(self, projection_name: str, position: int) -> None:
        """Persist checkpoint to projection_checkpoints table."""
        self._checkpoints[projection_name] = position
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE projection_checkpoints"
                    " SET last_position = $1, updated_at = NOW()"
                    " WHERE projection_name = $2",
                    position,
                    projection_name,
                )
        except Exception:
            # Fall back to in-memory store (for tests)
            try:
                await self.store.save_checkpoint(projection_name, position)
            except Exception:
                pass

    async def _get_latest_event_recorded_at(self) -> datetime | None:
        """Get the recorded_at of the most recent event in the store."""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT recorded_at FROM events ORDER BY global_position DESC LIMIT 1"
                )
                return row["recorded_at"] if row else None
        except Exception:
            # In-memory fallback
            return None
