"""
ledger/projections/application_summary.py — ApplicationSummaryProjection
=========================================================================
Read model for loan application state. Maintained by ProjectionDaemon.

Table: application_summary
"""
from __future__ import annotations

import logging
from datetime import datetime, UTC

import asyncpg

logger = logging.getLogger(__name__)


class ApplicationSummaryProjection:
    name = "application_summary"

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    # ── Dispatch ──────────────────────────────────────────────────────────────

    async def handle(self, event: dict) -> None:
        """Dispatch to per-event handler via getattr. Unknown events are silently ignored."""
        handler = getattr(self, f"_on_{event['event_type']}", None)
        if handler:
            await handler(event)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _recorded_at(self, event: dict) -> str:
        """Return ISO timestamp from event, falling back to now."""
        ra = event.get("recorded_at")
        if ra is None:
            return datetime.now(UTC).isoformat()
        if isinstance(ra, datetime):
            return ra.isoformat()
        return str(ra)

    def _payload(self, event: dict) -> dict:
        return event.get("payload", {})

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _on_ApplicationSubmitted(self, event: dict) -> None:
        p = self._payload(event)
        recorded_at = self._recorded_at(event)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO application_summary (
                    application_id, state, applicant_id, requested_amount_usd,
                    last_event_type, last_event_at
                ) VALUES ($1, 'SUBMITTED', $2, $3, 'ApplicationSubmitted', $4)
                ON CONFLICT (application_id) DO NOTHING
                """,
                p.get("application_id"),
                p.get("applicant_id"),
                p.get("requested_amount_usd"),
                recorded_at,
            )

    async def _on_CreditAnalysisCompleted(self, event: dict) -> None:
        p = self._payload(event)
        recorded_at = self._recorded_at(event)
        decision = p.get("decision") or {}
        risk_tier = decision.get("risk_tier") if isinstance(decision, dict) else None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET risk_tier = $1,
                    agent_sessions_completed = agent_sessions_completed + 1,
                    last_event_type = 'CreditAnalysisCompleted',
                    last_event_at = $2,
                    updated_at = NOW()
                WHERE application_id = $3
                """,
                risk_tier,
                recorded_at,
                p.get("application_id"),
            )

    async def _on_FraudScreeningCompleted(self, event: dict) -> None:
        p = self._payload(event)
        recorded_at = self._recorded_at(event)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET fraud_score = $1,
                    last_event_type = 'FraudScreeningCompleted',
                    last_event_at = $2,
                    updated_at = NOW()
                WHERE application_id = $3
                """,
                p.get("fraud_score"),
                recorded_at,
                p.get("application_id"),
            )

    async def _on_ComplianceCheckCompleted(self, event: dict) -> None:
        p = self._payload(event)
        recorded_at = self._recorded_at(event)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET compliance_status = $1,
                    last_event_type = 'ComplianceCheckCompleted',
                    last_event_at = $2,
                    updated_at = NOW()
                WHERE application_id = $3
                """,
                p.get("overall_verdict"),
                recorded_at,
                p.get("application_id"),
            )

    async def _on_DecisionGenerated(self, event: dict) -> None:
        p = self._payload(event)
        recorded_at = self._recorded_at(event)
        recommendation = p.get("recommendation")
        # Map recommendation to state
        state_map = {
            "APPROVE": "APPROVED",
            "DECLINE": "DECLINED",
            "REFER": "PENDING_HUMAN_REVIEW",
        }
        state = state_map.get(recommendation, recommendation)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET decision = $1,
                    risk_tier = COALESCE($2, risk_tier),
                    state = $3,
                    last_event_type = 'DecisionGenerated',
                    last_event_at = $4,
                    updated_at = NOW()
                WHERE application_id = $5
                """,
                recommendation,
                p.get("risk_tier"),
                state,
                recorded_at,
                p.get("application_id"),
            )

    async def _on_ApplicationApproved(self, event: dict) -> None:
        p = self._payload(event)
        recorded_at = self._recorded_at(event)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET state = 'FINAL_APPROVED',
                    approved_amount_usd = $1,
                    final_decision_at = $2,
                    last_event_type = 'ApplicationApproved',
                    last_event_at = $2,
                    updated_at = NOW()
                WHERE application_id = $3
                """,
                p.get("approved_amount_usd"),
                recorded_at,
                p.get("application_id"),
            )

    async def _on_ApplicationDeclined(self, event: dict) -> None:
        p = self._payload(event)
        recorded_at = self._recorded_at(event)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET state = 'FINAL_DECLINED',
                    final_decision_at = $1,
                    last_event_type = 'ApplicationDeclined',
                    last_event_at = $1,
                    updated_at = NOW()
                WHERE application_id = $2
                """,
                recorded_at,
                p.get("application_id"),
            )
