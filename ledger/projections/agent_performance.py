"""
ledger/projections/agent_performance.py — AgentPerformanceLedgerProjection
===========================================================================
Read model tracking per-(agent_id, model_version) performance metrics.
Maintained by ProjectionDaemon.

Table: agent_performance_ledger
"""
from __future__ import annotations

import logging
from datetime import datetime, UTC

import asyncpg

logger = logging.getLogger(__name__)


class AgentPerformanceLedgerProjection:
    name = "agent_performance_ledger"

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    # ── Dispatch ──────────────────────────────────────────────────────────────

    async def handle(self, event: dict) -> None:
        """Dispatch to per-event handler via getattr. Unknown events are silently ignored."""
        handler = getattr(self, f"_on_{event['event_type']}", None)
        if handler:
            await handler(event)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _payload(self, event: dict) -> dict:
        return event.get("payload", {})

    def _recorded_at(self, event: dict) -> datetime:
        ra = event.get("recorded_at")
        if ra is None:
            return datetime.now(UTC)
        if isinstance(ra, datetime):
            return ra
        try:
            return datetime.fromisoformat(str(ra))
        except ValueError:
            return datetime.now(UTC)

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _on_CreditAnalysisCompleted(self, event: dict) -> None:
        """UPSERT (agent_id, model_version) row with running averages."""
        p = self._payload(event)
        agent_id = p.get("session_id") or p.get("agent_id")  # session_id used as agent identifier
        model_version = p.get("model_version", "unknown")
        recorded_at = self._recorded_at(event)

        decision = p.get("decision") or {}
        confidence = None
        if isinstance(decision, dict):
            confidence = decision.get("confidence")
        duration_ms = p.get("analysis_duration_ms")

        async with self.pool.acquire() as conn:
            # Fetch existing row to compute running average
            row = await conn.fetchrow(
                "SELECT analyses_completed, avg_confidence_score, avg_duration_ms"
                " FROM agent_performance_ledger WHERE agent_id = $1 AND model_version = $2",
                agent_id,
                model_version,
            )

            if row is None:
                # First event — insert
                await conn.execute(
                    """
                    INSERT INTO agent_performance_ledger (
                        agent_id, model_version, analyses_completed,
                        avg_confidence_score, avg_duration_ms,
                        first_seen_at, last_seen_at
                    ) VALUES ($1, $2, 1, $3, $4, $5, $5)
                    """,
                    agent_id,
                    model_version,
                    confidence,
                    duration_ms,
                    recorded_at,
                )
            else:
                n = row["analyses_completed"]
                new_n = n + 1

                # Running average: new_avg = (old_avg * n + new_val) / new_n
                old_conf = row["avg_confidence_score"]
                new_conf = (
                    ((old_conf or 0.0) * n + (confidence or 0.0)) / new_n
                    if confidence is not None
                    else old_conf
                )

                old_dur = row["avg_duration_ms"]
                new_dur = (
                    ((old_dur or 0.0) * n + (duration_ms or 0.0)) / new_n
                    if duration_ms is not None
                    else old_dur
                )

                await conn.execute(
                    """
                    UPDATE agent_performance_ledger
                    SET analyses_completed = $1,
                        avg_confidence_score = $2,
                        avg_duration_ms = $3,
                        last_seen_at = $4
                    WHERE agent_id = $5 AND model_version = $6
                    """,
                    new_n,
                    new_conf,
                    new_dur,
                    recorded_at,
                    agent_id,
                    model_version,
                )

    async def _on_DecisionGenerated(self, event: dict) -> None:
        """Increment decisions_generated and update approve/decline/refer rates."""
        p = self._payload(event)
        # DecisionGenerated is on the loan stream; agent is identified via contributing_sessions
        # We update the orchestrator agent row using orchestrator_session_id as agent_id
        agent_id = p.get("orchestrator_session_id") or p.get("session_id")
        if not agent_id:
            return

        recommendation = (p.get("recommendation") or "").upper()
        recorded_at = self._recorded_at(event)

        async with self.pool.acquire() as conn:
            # Find any row for this agent (model_version may vary; use wildcard match)
            rows = await conn.fetch(
                "SELECT agent_id, model_version, decisions_generated,"
                " approve_rate, decline_rate, refer_rate"
                " FROM agent_performance_ledger WHERE agent_id = $1",
                agent_id,
            )

            for row in rows:
                n = row["decisions_generated"]
                new_n = n + 1

                # Recompute rates as running proportions
                old_approve = row["approve_rate"] * n
                old_decline = row["decline_rate"] * n
                old_refer = row["refer_rate"] * n

                if recommendation == "APPROVE":
                    old_approve += 1
                elif recommendation == "DECLINE":
                    old_decline += 1
                elif recommendation == "REFER":
                    old_refer += 1

                await conn.execute(
                    """
                    UPDATE agent_performance_ledger
                    SET decisions_generated = $1,
                        approve_rate = $2,
                        decline_rate = $3,
                        refer_rate = $4,
                        last_seen_at = $5
                    WHERE agent_id = $6 AND model_version = $7
                    """,
                    new_n,
                    old_approve / new_n,
                    old_decline / new_n,
                    old_refer / new_n,
                    recorded_at,
                    row["agent_id"],
                    row["model_version"],
                )

    async def _on_HumanReviewCompleted(self, event: dict) -> None:
        """Increment human_override_rate only when override=True."""
        p = self._payload(event)
        if not p.get("override", False):
            return

        reviewer_id = p.get("reviewer_id")
        if not reviewer_id:
            return

        recorded_at = self._recorded_at(event)

        async with self.pool.acquire() as conn:
            # human_override_rate is a running proportion of reviews that were overrides
            # We track it against the reviewer's agent row if it exists
            rows = await conn.fetch(
                "SELECT agent_id, model_version, decisions_generated, human_override_rate"
                " FROM agent_performance_ledger WHERE agent_id = $1",
                reviewer_id,
            )

            for row in rows:
                n = row["decisions_generated"] or 1
                old_overrides = row["human_override_rate"] * n
                new_rate = (old_overrides + 1) / n

                await conn.execute(
                    """
                    UPDATE agent_performance_ledger
                    SET human_override_rate = $1,
                        last_seen_at = $2
                    WHERE agent_id = $3 AND model_version = $4
                    """,
                    new_rate,
                    recorded_at,
                    row["agent_id"],
                    row["model_version"],
                )
