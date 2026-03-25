"""
scripts/backfill_projections.py
================================
One-shot script to replay all events through projections.
Run this after seeding the DB to populate projection tables.

Usage:
    python scripts/backfill_projections.py
"""
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env", override=True)

import asyncpg
from ledger.event_store import EventStore
from ledger.upcasters import registry as upcaster_registry
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection


async def main():
    db_url = os.environ.get("DATABASE_URL", "postgresql://localhost/apex_ledger")
    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=5)
    store = EventStore(db_url, upcaster_registry=upcaster_registry)
    await store.connect()

    app_summary = ApplicationSummaryProjection(pool)
    agent_perf = AgentPerformanceLedgerProjection(pool)
    compliance = ComplianceAuditViewProjection(pool, store=store)

    projections = [app_summary, agent_perf, compliance]

    print("Replaying all events through projections...")
    count = 0
    async for event in store.load_all(from_position=0):
        # Ensure recorded_at is a datetime object, not a string
        ra = event.get("recorded_at")
        if isinstance(ra, str):
            from datetime import datetime
            try:
                event = {**event, "recorded_at": datetime.fromisoformat(ra)}
            except ValueError:
                pass
        for proj in projections:
            try:
                await proj.handle(event)
            except Exception as e:
                print(f"  [WARN] {proj.name} failed on {event.get('event_type')}: {e}")
        count += 1
        if count % 100 == 0:
            print(f"  {count} events processed...")

    print(f"Done. {count} events replayed.")

    # Show summary
    async with pool.acquire() as conn:
        n_apps = await conn.fetchval("SELECT count(*) FROM application_summary")
        n_compliance = await conn.fetchval("SELECT count(*) FROM compliance_audit_events WHERE is_snapshot_row = FALSE")
        n_agents = await conn.fetchval("SELECT count(*) FROM agent_performance_ledger")
        print(f"\nProjection state:")
        print(f"  application_summary:   {n_apps} rows")
        print(f"  compliance_audit_events: {n_compliance} rows")
        print(f"  agent_performance_ledger: {n_agents} rows")

    await store.close()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
