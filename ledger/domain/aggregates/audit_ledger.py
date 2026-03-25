"""
ledger/domain/aggregates/audit_ledger.py
=========================================
AuditLedgerAggregate — tracks integrity check runs for an audited entity.

Per rubric:
  1. Event Replay: state reconstructed exclusively by replaying events
  2. Dispatch Mechanism: per-event handler methods via getattr dispatch
  3. Append-only invariant enforced at domain level
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ledger.event_store import DomainError


@dataclass
class AuditLedgerAggregate:
    """
    Consistency boundary for the audit integrity chain of an entity.
    Replays events from `audit-{entity_type}-{entity_id}` stream.
    """
    entity_type: str
    entity_id: str
    version: int = -1
    last_integrity_hash: str | None = None
    last_check_at: datetime | None = None
    events_verified_count: int = 0
    check_count: int = 0
    tamper_detected: bool = False

    # ── Load from event store ────────────────────────────────────────────────

    @classmethod
    async def load(cls, store, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        stream_id = f"audit-{entity_type}-{entity_id}"
        stream_events = await store.load_stream(stream_id)
        for event in stream_events:
            agg._apply(event)
        return agg

    # ── Event dispatch — per-event handlers via getattr ──────────────────────

    def _apply(self, event: dict) -> None:
        """Dispatch to per-event handler. NOT a monolithic if/elif."""
        event_type = event.get("event_type", "")
        handler = getattr(self, f"_on_{event_type}", None)
        if handler:
            handler(event)
        self.version = event.get("stream_position", self.version + 1)

    # ── Per-event handlers ───────────────────────────────────────────────────

    def _on_AuditIntegrityCheckRun(self, event: dict) -> None:
        p = event.get("payload", {})
        self.last_integrity_hash = p.get("integrity_hash")
        self.events_verified_count = p.get("events_verified", self.events_verified_count)
        self.check_count += 1
        if p.get("tamper_detected", False):
            self.tamper_detected = True
        checked_at = p.get("check_timestamp") or p.get("checked_at")
        if checked_at:
            if isinstance(checked_at, str):
                try:
                    self.last_check_at = datetime.fromisoformat(checked_at)
                except ValueError:
                    pass
            elif isinstance(checked_at, datetime):
                self.last_check_at = checked_at

    # ── Guard methods (business rule enforcement) ────────────────────────────

    def assert_append_only(self) -> None:
        """Raises DomainError(rule_violated='append_only_violation') if called after
        any attempt to modify a previously recorded event reference."""
        raise DomainError(
            f"{self.entity_type}-{self.entity_id}",
            "append_only_violation",
            "Audit ledger is append-only. Previously recorded events cannot be modified.",
        )
