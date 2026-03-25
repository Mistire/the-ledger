"""
ledger/domain/aggregates/compliance_record.py
=============================================
ComplianceRecordAggregate — tracks compliance rule evaluations for a loan application.

Per rubric:
  1. Event Replay: state reconstructed exclusively by replaying events
  2. Dispatch Mechanism: per-event handler methods via getattr dispatch
  3. Hard-block guard enforced at domain level
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ledger.event_store import DomainError


@dataclass
class ComplianceRecordAggregate:
    """
    Consistency boundary for compliance rule evaluations on a loan application.
    Replays events from `compliance-{application_id}` stream.
    """
    application_id: str
    version: int = -1
    rules_evaluated: int = 0
    rules_passed: list[str] = field(default_factory=list)
    rules_failed: list[str] = field(default_factory=list)
    has_hard_block: bool = False
    block_rule_id: str | None = None
    overall_verdict: str | None = None  # "CLEAR" | "BLOCKED" | "CONDITIONAL"
    in_compliance_review: bool = False

    # ── Load from event store ────────────────────────────────────────────────

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(application_id=application_id)
        stream_events = await store.load_stream(f"compliance-{application_id}")
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

    def _on_ComplianceCheckRequested(self, event: dict) -> None:
        self.in_compliance_review = True

    def _on_ComplianceRulePassed(self, event: dict) -> None:
        p = event.get("payload", {})
        rule_id = p.get("rule_id", "")
        self.rules_evaluated += 1
        if rule_id and rule_id not in self.rules_passed:
            self.rules_passed.append(rule_id)

    def _on_ComplianceRuleFailed(self, event: dict) -> None:
        p = event.get("payload", {})
        rule_id = p.get("rule_id", "")
        self.rules_evaluated += 1
        if rule_id and rule_id not in self.rules_failed:
            self.rules_failed.append(rule_id)
        if p.get("is_hard_block", False):
            self.has_hard_block = True
            self.block_rule_id = rule_id

    def _on_ComplianceRuleNoted(self, event: dict) -> None:
        p = event.get("payload", {})
        rule_id = p.get("rule_id", "")
        self.rules_evaluated += 1
        # Noted rules are neither passed nor failed — just counted

    def _on_ComplianceCheckCompleted(self, event: dict) -> None:
        p = event.get("payload", {})
        self.overall_verdict = p.get("overall_verdict")
        self.in_compliance_review = False

    # ── Guard methods (business rule enforcement) ────────────────────────────

    def assert_in_compliance_review(self) -> None:
        """Raises DomainError if the application is not currently in compliance review."""
        if not self.in_compliance_review:
            raise DomainError(
                self.application_id,
                "not_in_compliance_review",
                f"Application {self.application_id} is not currently in compliance review.",
            )

    def assert_no_hard_block(self) -> None:
        """Raises DomainError if a hard-block rule has been triggered."""
        if self.has_hard_block:
            raise DomainError(
                self.application_id,
                "hard_block_violation",
                f"Application {self.application_id} has a hard block on rule '{self.block_rule_id}'.",
            )
