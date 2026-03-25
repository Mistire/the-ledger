"""
scripts/demo.py — Live demo script for the Week 5 challenge video.
===================================================================
Shows the full loan application lifecycle with visible LangGraph node
execution, LLM calls, token counts, and event store writes.

Usage:
    python scripts/demo.py --application APEX-0014

What it shows:
  1. The Week Standard — full event history query (must be < 60s)
  2. Live pipeline run — LangGraph nodes + LLM calls visible in terminal
  3. Event store populated — psql query showing all events
  4. Projection populated — application_summary row
  5. Compliance audit trail — temporal query
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
# Force override — ensures .env values take precedence over shell env
load_dotenv(Path(__file__).parent.parent / ".env", override=True)

import asyncpg
from openai import AsyncOpenAI
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.text import Text
from rich import box

from ledger.event_store import EventStore
from ledger.upcasters import registry as upcaster_registry
from ledger.registry.client import ApplicantRegistryClient

console = Console()

DB_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost:5433/apex_ledger")
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL = os.environ.get("OPENAI_MODEL", "anthropic/claude-3.5-sonnet")


# ── Patched base agent that prints live output ────────────────────────────────

def _patch_agent_for_demo(agent):
    """Monkey-patch _record_node_execution to print live output."""
    original_record = agent._record_node_execution
    original_call_llm = agent._call_llm

    async def patched_record_node(name, in_keys, out_keys, ms, tok_in=None, tok_out=None, cost=None):
        if tok_in:
            console.print(
                f"  [bold cyan]→ NODE[/bold cyan] [white]{name}[/white]  "
                f"[dim]({ms}ms)[/dim]  "
                f"[yellow]🤖 LLM: {tok_in}in/{tok_out}out tokens  ${cost:.4f}[/yellow]"
            )
        else:
            console.print(
                f"  [bold green]→ NODE[/bold green] [white]{name}[/white]  "
                f"[dim]({ms}ms)[/dim]"
            )
        return await original_record(name, in_keys, out_keys, ms, tok_in, tok_out, cost)

    async def patched_call_llm(system, user, max_tokens=1024):
        console.print(f"    [dim]  ↳ calling LLM ({MODEL})...[/dim]")
        result = await original_call_llm(system, user, max_tokens)
        return result

    agent._record_node_execution = patched_record_node
    agent._call_llm = patched_call_llm
    return agent


# ── Step 1: Week Standard query ───────────────────────────────────────────────

async def show_event_history(pool: asyncpg.Pool, application_id: str, title: str | None = None):
    if title:
        console.print()
        console.print(Panel(title, border_style="cyan"))
    else:
        console.print()
        console.print(Panel(
            f"[bold white]STEP 1 — THE WEEK STANDARD[/bold white]\n"
            f"[dim]Show complete decision history of {application_id} in < 60 seconds[/dim]",
            border_style="cyan"
        ))

    t0 = time.perf_counter()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT stream_id, event_type, stream_position, recorded_at "
            "FROM events WHERE stream_id LIKE $1 "
            "ORDER BY global_position",
            f"%{application_id}%"
        )
    elapsed = time.perf_counter() - t0

    if not rows:
        console.print(f"[yellow]No events found for {application_id} yet — will populate after pipeline run.[/yellow]")
        return

    table = Table(title=f"Event History — {application_id}", box=box.SIMPLE_HEAVY)
    table.add_column("Stream", style="cyan", no_wrap=True)
    table.add_column("Event Type", style="white")
    table.add_column("Pos", justify="right", style="dim")
    table.add_column("Recorded At", style="dim")

    for row in rows:
        table.add_row(
            row["stream_id"],
            row["event_type"],
            str(row["stream_position"]),
            str(row["recorded_at"])[:19],
        )

    console.print(table)
    console.print(f"[bold green]✓ {len(rows)} events retrieved in {elapsed*1000:.0f}ms[/bold green]")


# ── Step 2: Live pipeline run ─────────────────────────────────────────────────

async def run_live_pipeline(application_id: str, phase: str, store: EventStore, registry, client):
    from ledger.agents.stub_agents import (
        FraudDetectionAgent, ComplianceAgent, DecisionOrchestratorAgent
    )
    from ledger.agents.credit_analysis_agent import CreditAnalysisAgent

    agent_map = {
        "credit":     (CreditAnalysisAgent,        "credit_analysis"),
        "fraud":      (FraudDetectionAgent,         "fraud_detection"),
        "compliance": (ComplianceAgent,             "compliance"),
        "decision":   (DecisionOrchestratorAgent,   "decision_orchestrator"),
    }

    phases = ["credit", "fraud", "compliance", "decision"] if phase == "all" else [phase]

    for p in phases:
        AgentClass, agent_type = agent_map[p]
        console.print()
        console.print(Panel(
            f"[bold white]STEP 2 — LANGGRAPH AGENT: {agent_type.upper().replace('_', ' ')}[/bold white]\n"
            f"[dim]Application: {application_id}[/dim]",
            border_style="yellow"
        ))

        agent = AgentClass(
            agent_id=f"demo-{agent_type}",
            agent_type=agent_type,
            store=store,
            registry=registry,
            client=client,
            model=MODEL,
        )
        _patch_agent_for_demo(agent)

        t0 = time.perf_counter()
        try:
            await agent.process_application(application_id)
            elapsed = time.perf_counter() - t0
            console.print(
                f"\n[bold green]✓ {agent_type} completed in {elapsed:.1f}s[/bold green]  "
                f"[dim]LLM calls: {agent._llm_calls}  "
                f"Tokens: {agent._tokens}  "
                f"Cost: ${agent._cost:.4f}[/dim]"
            )
        except Exception as e:
            err = str(e)
            if "OCC" in err or "expected v-1" in err or "already exists" in err.lower():
                console.print(f"  [yellow]⚠ {agent_type} already completed for this application — skipping[/yellow]")
            else:
                console.print(f"[bold red]✗ {agent_type} failed: {e}[/bold red]")
                raise


# ── Step 3: Show projection state ─────────────────────────────────────────────

async def show_projection_state(pool: asyncpg.Pool, application_id: str):
    console.print()
    console.print(Panel(
        "[bold white]STEP 4 — PROJECTION STATE (CQRS Read Model)[/bold white]\n"
        "[dim]application_summary table — no event replay needed[/dim]",
        border_style="green"
    ))

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT application_id, state, risk_tier, fraud_score, "
            "compliance_status, decision, final_decision_at "
            "FROM application_summary WHERE application_id = $1",
            application_id
        )

    if row:
        table = Table(box=box.SIMPLE)
        for col in ["application_id", "state", "risk_tier", "fraud_score",
                    "compliance_status", "decision", "final_decision_at"]:
            table.add_column(col, style="cyan" if col == "state" else "white")
        table.add_row(*[str(row[c] or "") for c in
                        ["application_id", "state", "risk_tier", "fraud_score",
                         "compliance_status", "decision", "final_decision_at"]])
        console.print(table)
        console.print(f"[bold green]✓ Final state: {row['state']}[/bold green]")
    else:
        console.print("[yellow]No projection row yet — run backfill_projections.py[/yellow]")


# ── Step 4: Compliance audit trail ────────────────────────────────────────────

async def show_compliance_trail(pool: asyncpg.Pool, application_id: str):
    console.print()
    console.print(Panel(
        "[bold white]STEP 5 — COMPLIANCE AUDIT TRAIL (Temporal Query)[/bold white]\n"
        "[dim]compliance_audit_events — every rule evaluation recorded[/dim]",
        border_style="magenta"
    ))

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT event_type, rule_id, passed, is_hard_block, event_recorded_at "
            "FROM compliance_audit_events "
            "WHERE application_id = $1 AND is_snapshot_row = FALSE "
            "ORDER BY event_recorded_at",
            application_id
        )

    if rows:
        table = Table(box=box.SIMPLE)
        table.add_column("Event Type", style="white")
        table.add_column("Rule ID", style="cyan")
        table.add_column("Passed", style="green")
        table.add_column("Hard Block", style="red")
        table.add_column("Recorded At", style="dim")
        for row in rows:
            table.add_row(
                row["event_type"],
                row["rule_id"] or "",
                "✓" if row["passed"] else ("✗" if row["passed"] is False else ""),
                "YES" if row["is_hard_block"] else "",
                str(row["event_recorded_at"])[:19],
            )
        console.print(table)
        console.print(f"[bold green]✓ {len(rows)} compliance events recorded[/bold green]")
    else:
        console.print("[yellow]No compliance events yet — run backfill_projections.py after pipeline[/yellow]")


# ── Main ──────────────────────────────────────────────────────────────────────

async def _count_events(pool: asyncpg.Pool, application_id: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM events WHERE stream_id LIKE $1",
            f"%{application_id}%"
        ) or 0


async def main():
    parser = argparse.ArgumentParser(description="The Ledger — Live Demo")
    parser.add_argument("--application", default="APEX-0014", help="Application ID to demo")
    parser.add_argument(
        "--phase", default="credit",
        choices=["credit", "fraud", "compliance", "decision", "all"],
        help="Which agent phase(s) to run live"
    )
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="Skip pipeline run, just show existing data")
    args = parser.parse_args()

    console.print(Panel(
        "[bold cyan]THE LEDGER — LIVE DEMO[/bold cyan]\n"
        "[dim]Agentic Event Store & Enterprise Audit Infrastructure[/dim]\n\n"
        f"Application: [bold]{args.application}[/bold]  |  Phase: [bold]{args.phase}[/bold]",
        border_style="cyan", expand=False
    ))

    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=5)
    store = EventStore(DB_URL, upcaster_registry=upcaster_registry)
    await store.connect()

    registry_pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=3)
    registry = ApplicantRegistryClient(registry_pool)

    client = AsyncOpenAI(
        api_key=OPENROUTER_KEY,
        base_url="https://openrouter.ai/api/v1"
    )

    try:
        # Step 1: Show existing event history
        events_before = await _count_events(pool, args.application)
        await show_event_history(pool, args.application)

        if not args.skip_pipeline:
            # Step 2: Run live pipeline with visible LangGraph output
            await run_live_pipeline(args.application, args.phase, store, registry, client)

            # Backfill projections after pipeline
            console.print("\n[dim]Updating projections...[/dim]")
            from ledger.projections.application_summary import ApplicationSummaryProjection
            from ledger.projections.compliance_audit import ComplianceAuditViewProjection
            from ledger.projections.agent_performance import AgentPerformanceLedgerProjection

            proj_pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=3)
            app_proj = ApplicationSummaryProjection(proj_pool)
            comp_proj = ComplianceAuditViewProjection(proj_pool, store=store)
            agent_proj = AgentPerformanceLedgerProjection(proj_pool)

            async for event in store.load_all(from_position=0):
                ra = event.get("recorded_at")
                if isinstance(ra, str):
                    from datetime import datetime
                    try:
                        event = {**event, "recorded_at": datetime.fromisoformat(ra)}
                    except ValueError:
                        pass
                for proj in [app_proj, comp_proj, agent_proj]:
                    try:
                        await proj.handle(event)
                    except Exception:
                        pass
            await proj_pool.close()

            # Step 3: Only show updated history if new events were written
            events_after = await _count_events(pool, args.application)
            if events_after > events_before:
                console.print()
                console.print(Panel(
                    f"[bold white]STEP 3 — UPDATED EVENT HISTORY[/bold white]\n"
                    f"[dim]{events_after - events_before} new events written by agents[/dim]",
                    border_style="cyan"
                ))
                await show_event_history(pool, args.application,
                                         title=f"[bold white]{events_after} total events (was {events_before})[/bold white]")

        # Projection state
        await show_projection_state(pool, args.application)

        # Compliance trail
        await show_compliance_trail(pool, args.application)
    finally:
        await store.close()
        await pool.close()
        await registry_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
