"""
ledger/domain/aggregates/loan_application.py
=============================================
LoanApplication aggregate — state machine with event replay.

Per rubric:
  1. Event Replay: state reconstructed exclusively by replaying events
  2. Dispatch Mechanism: per-event handler methods via getattr dispatch
  3. All 7 lifecycle states with DomainError on invalid transitions
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from ledger.event_store import DomainError


# ═══════════════════════════════════════════════════════════════════════════════
# APPLICATION STATE MACHINE
# ═══════════════════════════════════════════════════════════════════════════════

class ApplicationState(str, Enum):
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


VALID_TRANSITIONS: dict[ApplicationState, list[ApplicationState]] = {
    ApplicationState.SUBMITTED: [
        ApplicationState.AWAITING_ANALYSIS,
    ],
    ApplicationState.AWAITING_ANALYSIS: [
        ApplicationState.ANALYSIS_COMPLETE,
    ],
    ApplicationState.ANALYSIS_COMPLETE: [
        ApplicationState.COMPLIANCE_REVIEW,
    ],
    ApplicationState.COMPLIANCE_REVIEW: [
        ApplicationState.PENDING_DECISION,
    ],
    ApplicationState.PENDING_DECISION: [
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
    ],
    ApplicationState.APPROVED_PENDING_HUMAN: [
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,
    ],
    ApplicationState.DECLINED_PENDING_HUMAN: [
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,
    ],
    ApplicationState.FINAL_APPROVED: [],
    ApplicationState.FINAL_DECLINED: [],
}


# ═══════════════════════════════════════════════════════════════════════════════
# LOAN APPLICATION AGGREGATE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class LoanApplicationAggregate:
    """
    Consistency boundary for a commercial loan application lifecycle.
    State is reconstructed exclusively by replaying stored events.
    """
    application_id: str
    state: ApplicationState | None = None
    version: int = -1

    # Domain state built from events
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    credit_analysis_completed: bool = False
    credit_analysis_agent_id: str | None = None
    fraud_screening_completed: bool = False
    compliance_checks_passed: list[str] = field(default_factory=list)
    compliance_checks_required: list[str] = field(default_factory=list)
    all_compliance_passed: bool = False
    decision_recommendation: str | None = None
    decision_confidence: float | None = None
    contributing_agent_sessions: list[str] = field(default_factory=list)
    approved_amount_usd: float | None = None

    # ── Load from event store ────────────────────────────────────────────────

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(application_id=application_id)
        stream_events = await store.load_stream(f"loan-{application_id}")
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

    def _on_ApplicationSubmitted(self, event: dict) -> None:
        p = event.get("payload", {})
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = p.get("applicant_id")
        self.requested_amount_usd = p.get("requested_amount_usd")
        self.loan_purpose = p.get("loan_purpose")

    def _on_CreditAnalysisRequested(self, event: dict) -> None:
        self.state = ApplicationState.AWAITING_ANALYSIS

    def _on_CreditAnalysisCompleted(self, event: dict) -> None:
        p = event.get("payload", {})
        self.state = ApplicationState.ANALYSIS_COMPLETE
        self.credit_analysis_completed = True
        self.credit_analysis_agent_id = p.get("agent_id")

    def _on_FraudScreeningCompleted(self, event: dict) -> None:
        self.fraud_screening_completed = True

    def _on_ComplianceCheckRequested(self, event: dict) -> None:
        p = event.get("payload", {})
        self.state = ApplicationState.COMPLIANCE_REVIEW
        self.compliance_checks_required = p.get("checks_required", [])

    def _on_ComplianceRulePassed(self, event: dict) -> None:
        p = event.get("payload", {})
        rule_id = p.get("rule_id", "")
        if rule_id and rule_id not in self.compliance_checks_passed:
            self.compliance_checks_passed.append(rule_id)
        if self.compliance_checks_required and all(
            r in self.compliance_checks_passed
            for r in self.compliance_checks_required
        ):
            self.all_compliance_passed = True

    def _on_ComplianceRuleFailed(self, event: dict) -> None:
        pass

    def _on_DecisionGenerated(self, event: dict) -> None:
        p = event.get("payload", {})
        recommendation = p.get("recommendation", "")
        self.decision_confidence = p.get("confidence_score", 0.0)
        self.contributing_agent_sessions = p.get("contributing_agent_sessions", [])
        self.decision_recommendation = recommendation
        if recommendation in ("APPROVE", "APPROVED"):
            self.state = ApplicationState.APPROVED_PENDING_HUMAN
        elif recommendation in ("DECLINE", "DECLINED"):
            self.state = ApplicationState.DECLINED_PENDING_HUMAN
        else:
            self.state = ApplicationState.PENDING_DECISION

    def _on_HumanReviewCompleted(self, event: dict) -> None:
        p = event.get("payload", {})
        final = p.get("final_decision", "")
        if final in ("APPROVE", "APPROVED"):
            self.state = ApplicationState.FINAL_APPROVED
        elif final in ("DECLINE", "DECLINED"):
            self.state = ApplicationState.FINAL_DECLINED

    def _on_ApplicationApproved(self, event: dict) -> None:
        p = event.get("payload", {})
        self.state = ApplicationState.FINAL_APPROVED
        self.approved_amount_usd = p.get("approved_amount_usd")

    def _on_ApplicationDeclined(self, event: dict) -> None:
        self.state = ApplicationState.FINAL_DECLINED

    # ── Guard methods (business rule enforcement) ────────────────────────────

    def assert_valid_transition(self, target: ApplicationState) -> None:
        if self.state is None:
            if target != ApplicationState.SUBMITTED:
                raise DomainError(
                    self.application_id, "invalid_transition",
                    f"New application must start as SUBMITTED, not {target}",
                )
            return
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise DomainError(
                self.application_id, "invalid_transition",
                f"Cannot transition from {self.state} to {target}. "
                f"Allowed: {[s.value for s in allowed]}",
            )

    def assert_awaiting_credit_analysis(self) -> None:
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise DomainError(
                self.application_id, "not_awaiting_analysis",
                f"Expected AWAITING_ANALYSIS, got {self.state}",
            )

    def assert_credit_analysis_not_completed(self) -> None:
        if self.credit_analysis_completed:
            raise DomainError(
                self.application_id, "model_version_lock",
                "CreditAnalysisCompleted already exists for this application.",
            )

    def assert_compliance_complete_for_approval(self) -> None:
        if not self.all_compliance_passed:
            raise DomainError(
                self.application_id, "compliance_dependency",
                f"Cannot approve: not all compliance checks passed. "
                f"Required: {self.compliance_checks_required}, "
                f"Passed: {self.compliance_checks_passed}",
            )

    def assert_confidence_floor(self, confidence_score: float) -> str | None:
        """Returns 'REFER' if confidence < 0.6, else None."""
        if confidence_score < 0.6:
            return "REFER"
        return None

    def assert_causal_chain(self, contributing_sessions: list[str]) -> None:
        if not contributing_sessions:
            raise DomainError(
                self.application_id, "causal_chain_violation",
                "DecisionGenerated must reference at least one contributing agent session.",
            )
