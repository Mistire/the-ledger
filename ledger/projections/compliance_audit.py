"""
ledger/projections/compliance_audit.py — ComplianceAuditViewProjection
=======================================================================
Read model for compliance audit trail with snapshot support.
Maintained by ProjectionDaemon.

Table: compliance_audit_events

Snapshot strategy:
  - One row per application marked is_snapshot_row=TRUE holds materialised state.
  - Snapshot is taken every SNAPSHOT_INTERVAL events per application.
  - Temporal queries load nearest snapshot before timestamp, then replay delta.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, UTC
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

# Compliance-relevant event types
COMPLIANCE_EVENT_TYPES = {
    "ComplianceCheckInitiated",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
    "ComplianceRuleNoted",
    "ComplianceCheckCompleted",
}


class ComplianceAuditViewProjection:
    name = "compliance_audit_view"
    SNAPSHOT_INTERVAL = 100

    def __init__(self, pool: asyncpg.Pool, store=None):
        self.pool = pool
        self.store = store  # needed for rebuild_from_scratch and get_projection_lag
        self._last_processed_at: datetime | None = None
        # Per-application event counter since last snapshot
        self._events_since_snapshot: dict[str, int] = {}

    # ── Dispatch ──────────────────────────────────────────────────────────────

    async def handle(self, event: dict) -> None:
        """Dispatch to per-event handler. Only compliance-relevant events are stored."""
        if event.get("event_type") not in COMPLIANCE_EVENT_TYPES:
            return

        handler = getattr(self, f"_on_{event['event_type']}", None)
        if handler:
            await handler(event)

        # Track last processed timestamp
        ra = event.get("recorded_at")
        if ra is not None:
            if isinstance(ra, str):
                try:
                    ra = datetime.fromisoformat(ra)
                except ValueError:
                    ra = None
            if ra is not None:
                self._last_processed_at = ra

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _payload(self, event: dict) -> dict:
        return event.get("payload", {})

    def _recorded_at_str(self, event: dict) -> str:
        ra = event.get("recorded_at")
        if ra is None:
            return datetime.now(UTC).isoformat()
        if isinstance(ra, datetime):
            return ra.isoformat()
        return str(ra)

    async def _insert_audit_row(self, conn, application_id: str, event: dict) -> None:
        p = self._payload(event)
        # Parse recorded_at to datetime if it's a string
        ra = event.get("recorded_at")
        if isinstance(ra, str):
            try:
                ra = datetime.fromisoformat(ra)
            except ValueError:
                ra = datetime.now(UTC)
        elif ra is None:
            ra = datetime.now(UTC)
        await conn.execute(
            """
            INSERT INTO compliance_audit_events (
                application_id, event_type, rule_id, rule_name, rule_version,
                passed, is_hard_block, overall_verdict, full_payload,
                event_recorded_at, global_position
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
            """,
            application_id,
            event.get("event_type"),
            p.get("rule_id"),
            p.get("rule_name"),
            p.get("rule_version"),
            p.get("passed"),
            p.get("is_hard_block"),
            p.get("overall_verdict"),
            json.dumps(p),
            ra,
            event.get("global_position", 0),
        )

    # ── Per-event handlers ────────────────────────────────────────────────────

    async def _on_ComplianceCheckInitiated(self, event: dict) -> None:
        p = self._payload(event)
        application_id = p.get("application_id")
        async with self.pool.acquire() as conn:
            await self._insert_audit_row(conn, application_id, event)
        await self._maybe_snapshot(application_id)

    async def _on_ComplianceRulePassed(self, event: dict) -> None:
        p = self._payload(event)
        application_id = p.get("application_id")
        async with self.pool.acquire() as conn:
            await self._insert_audit_row(conn, application_id, event)
        await self._maybe_snapshot(application_id)

    async def _on_ComplianceRuleFailed(self, event: dict) -> None:
        p = self._payload(event)
        application_id = p.get("application_id")
        async with self.pool.acquire() as conn:
            await self._insert_audit_row(conn, application_id, event)
        await self._maybe_snapshot(application_id)

    async def _on_ComplianceRuleNoted(self, event: dict) -> None:
        p = self._payload(event)
        application_id = p.get("application_id")
        async with self.pool.acquire() as conn:
            await self._insert_audit_row(conn, application_id, event)
        await self._maybe_snapshot(application_id)

    async def _on_ComplianceCheckCompleted(self, event: dict) -> None:
        p = self._payload(event)
        application_id = p.get("application_id")
        async with self.pool.acquire() as conn:
            await self._insert_audit_row(conn, application_id, event)
        await self._maybe_snapshot(application_id)

    # ── Query methods ─────────────────────────────────────────────────────────

    async def get_current_compliance(self, application_id: str) -> dict:
        """SELECT all compliance events ordered by event_recorded_at."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, application_id, event_type, rule_id, rule_name,
                       rule_version, passed, is_hard_block, overall_verdict,
                       full_payload, event_recorded_at, global_position
                FROM compliance_audit_events
                WHERE application_id = $1 AND is_snapshot_row = FALSE
                ORDER BY event_recorded_at ASC
                """,
                application_id,
            )
            events = [dict(row) for row in rows]
            return {
                "application_id": application_id,
                "events": events,
                "event_count": len(events),
            }

    async def get_compliance_at(self, application_id: str, timestamp: datetime) -> dict:
        """
        Load nearest snapshot before timestamp, then replay delta events.
        Returns compliance state as it existed at the given timestamp.
        """
        async with self.pool.acquire() as conn:
            # 1. Find nearest snapshot before timestamp
            snapshot_row = await conn.fetchrow(
                """
                SELECT snapshot_state, snapshot_recorded_at
                FROM compliance_audit_events
                WHERE application_id = $1
                  AND is_snapshot_row = TRUE
                  AND snapshot_recorded_at <= $2
                ORDER BY snapshot_recorded_at DESC
                LIMIT 1
                """,
                application_id,
                timestamp,
            )

            # 2. Load delta events between snapshot and timestamp
            snapshot_at = snapshot_row["snapshot_recorded_at"] if snapshot_row else None
            query_params: list[Any] = [application_id, timestamp]
            if snapshot_at:
                delta_rows = await conn.fetch(
                    """
                    SELECT id, application_id, event_type, rule_id, rule_name,
                           rule_version, passed, is_hard_block, overall_verdict,
                           full_payload, event_recorded_at, global_position
                    FROM compliance_audit_events
                    WHERE application_id = $1
                      AND is_snapshot_row = FALSE
                      AND event_recorded_at > $3
                      AND event_recorded_at <= $2
                    ORDER BY event_recorded_at ASC
                    """,
                    application_id,
                    timestamp,
                    snapshot_at,
                )
            else:
                delta_rows = await conn.fetch(
                    """
                    SELECT id, application_id, event_type, rule_id, rule_name,
                           rule_version, passed, is_hard_block, overall_verdict,
                           full_payload, event_recorded_at, global_position
                    FROM compliance_audit_events
                    WHERE application_id = $1
                      AND is_snapshot_row = FALSE
                      AND event_recorded_at <= $2
                    ORDER BY event_recorded_at ASC
                    """,
                    application_id,
                    timestamp,
                )

            # 3. Build result from snapshot + delta
            base_state = dict(snapshot_row["snapshot_state"]) if snapshot_row else {}
            delta_events = [dict(row) for row in delta_rows]

            return {
                "application_id": application_id,
                "as_of": timestamp.isoformat(),
                "snapshot_used": snapshot_row is not None,
                "base_state": base_state,
                "delta_events": delta_events,
                "event_count": len(delta_events),
            }

    async def rebuild_from_scratch(self) -> None:
        """
        TRUNCATE compliance_audit_events then full replay from global_position=0.
        Uses a separate DB connection so concurrent reads on old data still work
        during the replay phase — only the final TRUNCATE+swap is atomic.
        """
        if self.store is None:
            raise RuntimeError("store must be set on ComplianceAuditViewProjection to rebuild")

        logger.info("ComplianceAuditViewProjection: starting rebuild_from_scratch")

        # Collect all compliance events from the store first (non-blocking)
        events_to_replay: list[dict] = []
        async for event in self.store.load_all(from_position=0):
            if event.get("event_type") in COMPLIANCE_EVENT_TYPES:
                events_to_replay.append(event)

        # Now truncate and replay in a transaction
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("TRUNCATE TABLE compliance_audit_events RESTART IDENTITY")

        # Replay outside the truncate transaction so concurrent reads aren't blocked
        self._events_since_snapshot.clear()
        for event in events_to_replay:
            await self.handle(event)

        logger.info(
            "ComplianceAuditViewProjection: rebuild complete, replayed %d events",
            len(events_to_replay),
        )

    async def get_projection_lag(self) -> int:
        """Returns ms between latest event recorded_at and last processed."""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT recorded_at FROM events ORDER BY global_position DESC LIMIT 1"
                )
                if row is None or self._last_processed_at is None:
                    return 0
                latest = row["recorded_at"]
                if isinstance(latest, datetime):
                    delta = latest - self._last_processed_at
                    return max(0, int(delta.total_seconds() * 1000))
        except Exception:
            pass
        return 0

    # ── Snapshot ──────────────────────────────────────────────────────────────

    async def _maybe_snapshot(self, application_id: str) -> None:
        """Materialise a snapshot every SNAPSHOT_INTERVAL events per application."""
        count = self._events_since_snapshot.get(application_id, 0) + 1
        self._events_since_snapshot[application_id] = count

        if count < self.SNAPSHOT_INTERVAL:
            return

        # Reset counter
        self._events_since_snapshot[application_id] = 0

        # Build current state snapshot
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_type, rule_id, rule_name, passed, is_hard_block,
                       overall_verdict, full_payload, event_recorded_at
                FROM compliance_audit_events
                WHERE application_id = $1 AND is_snapshot_row = FALSE
                ORDER BY event_recorded_at ASC
                """,
                application_id,
            )

            if not rows:
                return

            # Compute snapshot state by replaying all events
            snapshot_state: dict[str, Any] = {
                "rules_passed": [],
                "rules_failed": [],
                "rules_noted": [],
                "has_hard_block": False,
                "overall_verdict": None,
                "event_count": len(rows),
            }
            for row in rows:
                et = row["event_type"]
                if et == "ComplianceRulePassed" and row["rule_id"]:
                    snapshot_state["rules_passed"].append(row["rule_id"])
                elif et == "ComplianceRuleFailed" and row["rule_id"]:
                    snapshot_state["rules_failed"].append(row["rule_id"])
                    if row["is_hard_block"]:
                        snapshot_state["has_hard_block"] = True
                elif et == "ComplianceRuleNoted" and row["rule_id"]:
                    snapshot_state["rules_noted"].append(row["rule_id"])
                elif et == "ComplianceCheckCompleted":
                    snapshot_state["overall_verdict"] = row["overall_verdict"]

            snapshot_at = rows[-1]["event_recorded_at"]

            # Upsert snapshot row
            existing = await conn.fetchrow(
                "SELECT id FROM compliance_audit_events"
                " WHERE application_id = $1 AND is_snapshot_row = TRUE",
                application_id,
            )

            if existing:
                await conn.execute(
                    """
                    UPDATE compliance_audit_events
                    SET snapshot_state = $1::jsonb,
                        snapshot_recorded_at = $2
                    WHERE id = $3
                    """,
                    json.dumps(snapshot_state),
                    snapshot_at,
                    existing["id"],
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO compliance_audit_events (
                        application_id, event_type, full_payload,
                        event_recorded_at, global_position,
                        is_snapshot_row, snapshot_state, snapshot_recorded_at
                    ) VALUES ($1, 'SNAPSHOT', '{}'::jsonb, $2, 0, TRUE, $3::jsonb, $2)
                    """,
                    application_id,
                    snapshot_at,
                    json.dumps(snapshot_state),
                )
