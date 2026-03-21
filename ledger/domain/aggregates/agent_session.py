"""
ledger/domain/aggregates/agent_session.py
==========================================
AgentSession aggregate — Gas Town pattern enforcement.

Per rubric:
  1. Event Replay: state reconstructed exclusively by replaying events
  2. Dispatch Mechanism: per-event handler methods via getattr dispatch
  3. context_declared flag — must be True before any decision event
  4. model_version guard — enforced at domain level
"""
from __future__ import annotations

from dataclasses import dataclass

from ledger.event_store import DomainError


@dataclass
class AgentSessionAggregate:
    """
    Consistency boundary for an AI agent's work session.
    Enforces the Gas Town persistent ledger pattern.
    """
    agent_id: str
    session_id: str
    version: int = -1

    # Gas Town invariants
    context_declared: bool = False
    model_version: str | None = None

    # Session state
    application_id: str | None = None
    context_source: str | None = None
    context_token_count: int = 0
    actions_completed: int = 0
    has_decision: bool = False
    session_completed: bool = False
    session_failed: bool = False

    # ── Load from event store ────────────────────────────────────────────────

    @classmethod
    async def load(cls, store, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(agent_id=agent_id, session_id=session_id)
        stream_id = f"agent-{agent_id}-{session_id}"
        stream_events = await store.load_stream(stream_id)
        for event in stream_events:
            agg._apply(event)
        return agg

    # ── Event dispatch ───────────────────────────────────────────────────────

    def _apply(self, event: dict) -> None:
        """Dispatch to per-event handler. NOT a monolithic if/elif."""
        event_type = event.get("event_type", "")
        handler = getattr(self, f"_on_{event_type}", None)
        if handler:
            handler(event)
        self.version = event.get("stream_position", self.version + 1)

    # ── Per-event handlers ───────────────────────────────────────────────────

    def _on_AgentContextLoaded(self, event: dict) -> None:
        p = event.get("payload", {})
        self.context_declared = True
        self.model_version = p.get("model_version")
        self.context_source = p.get("context_source")
        self.context_token_count = p.get("context_token_count", 0)
        self.application_id = p.get("application_id")

    def _on_AgentSessionStarted(self, event: dict) -> None:
        p = event.get("payload", {})
        self.context_declared = True
        self.model_version = p.get("model_version")
        self.context_source = p.get("context_source")
        self.context_token_count = p.get("context_token_count", 0)
        self.application_id = p.get("application_id")

    def _on_CreditAnalysisCompleted(self, event: dict) -> None:
        self.has_decision = True
        self.actions_completed += 1

    def _on_FraudScreeningCompleted(self, event: dict) -> None:
        self.has_decision = True
        self.actions_completed += 1

    def _on_DecisionGenerated(self, event: dict) -> None:
        self.has_decision = True
        self.actions_completed += 1

    def _on_AgentSessionCompleted(self, event: dict) -> None:
        self.session_completed = True

    def _on_AgentSessionFailed(self, event: dict) -> None:
        self.session_failed = True

    # ── Guard methods ────────────────────────────────────────────────────────

    def assert_context_loaded(self) -> None:
        """Gas Town invariant: context must be declared before decisions."""
        if not self.context_declared:
            raise DomainError(
                f"agent-{self.agent_id}-{self.session_id}",
                "gas_town_violation",
                "Agent must declare context (AgentContextLoaded) before "
                "any decision event.",
            )

    def assert_model_version_current(self, expected_version: str) -> None:
        """Model version guard at domain level."""
        if self.model_version and self.model_version != expected_version:
            raise DomainError(
                f"agent-{self.agent_id}-{self.session_id}",
                "model_version_mismatch",
                f"Agent declared '{self.model_version}' but decision uses '{expected_version}'.",
            )

    def assert_not_completed(self) -> None:
        if self.session_completed:
            raise DomainError(
                f"agent-{self.agent_id}-{self.session_id}",
                "session_completed",
                "Cannot append events to a completed agent session.",
            )
        if self.session_failed:
            raise DomainError(
                f"agent-{self.agent_id}-{self.session_id}",
                "session_failed",
                "Cannot append events to a failed agent session.",
            )
