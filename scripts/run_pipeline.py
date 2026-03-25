"""
scripts/run_pipeline.py — Process one application through all 5 agents.

Usage:
    python scripts/run_pipeline.py --application APEX-0007 [--phase all|document|credit|fraud|compliance|decision]

Reads DATABASE_URL and OPENAI_API_KEY from environment (or .env file).
Pipeline sequence: DocumentProcessing → CreditAnalysis → FraudDetection → Compliance → DecisionOrchestrator
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env", override=True)

import asyncpg
from openai import AsyncOpenAI

from ledger.event_store import EventStore
from ledger.upcasters import registry as upcaster_registry
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.stub_agents import (
    DocumentProcessingAgent,
    FraudDetectionAgent,
    ComplianceAgent,
    DecisionOrchestratorAgent,
)
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent


async def run_pipeline(application_id: str, phase: str = "all") -> None:
    """
    Runs the full 5-agent pipeline (or a single phase) for a given application_id.
    Agents run in sequence: DocumentProcessing → CreditAnalysis → FraudDetection
                            → Compliance → DecisionOrchestrator.
    """
    db_url = os.environ.get("DATABASE_URL", "postgresql://localhost/apex_ledger")
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENROUTER_API_KEY", "")
    model = os.environ.get("OPENAI_MODEL", "anthropic/claude-3.5-sonnet")
    base_url = os.environ.get("OPENAI_BASE_URL", "https://openrouter.ai/api/v1") if not os.environ.get("OPENAI_API_KEY") else None

    # Initialize shared resources
    store = EventStore(db_url, upcaster_registry=upcaster_registry)
    await store.connect()

    registry_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    registry = ApplicantRegistryClient(registry_pool)

    client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    agent_id = f"pipeline-runner"

    try:
        phases_to_run = _resolve_phases(phase)

        for phase_name in phases_to_run:
            print(f"[{application_id}] Running phase: {phase_name}")
            agent = _build_agent(phase_name, agent_id, store, registry, client, model)
            await agent.process_application(application_id)
            print(f"[{application_id}] Phase {phase_name} completed.")

    finally:
        await store.close()
        await registry_pool.close()


def _resolve_phases(phase: str) -> list[str]:
    """Map phase argument to ordered list of phase names."""
    all_phases = ["document", "credit", "fraud", "compliance", "decision"]
    if phase == "all":
        return all_phases
    if phase in all_phases:
        return [phase]
    raise ValueError(f"Unknown phase: {phase!r}. Choose from: all, {', '.join(all_phases)}")


def _build_agent(phase: str, agent_id: str, store, registry, client, model: str):
    """Instantiate the correct agent for the given phase."""
    if phase == "document":
        return DocumentProcessingAgent(
            agent_id=agent_id, agent_type="document_processing",
            store=store, registry=registry, client=client, model=model,
        )
    elif phase == "credit":
        return CreditAnalysisAgent(
            agent_id=agent_id, agent_type="credit_analysis",
            store=store, registry=registry, client=client, model=model,
        )
    elif phase == "fraud":
        return FraudDetectionAgent(
            agent_id=agent_id, agent_type="fraud_detection",
            store=store, registry=registry, client=client, model=model,
        )
    elif phase == "compliance":
        return ComplianceAgent(
            agent_id=agent_id, agent_type="compliance",
            store=store, registry=registry, client=client, model=model,
        )
    elif phase == "decision":
        return DecisionOrchestratorAgent(
            agent_id=agent_id, agent_type="decision_orchestrator",
            store=store, registry=registry, client=client, model=model,
        )
    else:
        raise ValueError(f"Unknown phase: {phase!r}")


async def main() -> None:
    p = argparse.ArgumentParser(description="Run the Ledger agent pipeline for an application.")
    p.add_argument("--application", required=True, help="Application ID (e.g. APEX-0007)")
    p.add_argument(
        "--phase", default="all",
        choices=["all", "document", "credit", "fraud", "compliance", "decision"],
        help="Which phase(s) to run (default: all)",
    )
    args = p.parse_args()

    print(f"Starting pipeline for application: {args.application}, phase: {args.phase}")
    await run_pipeline(args.application, args.phase)
    print("Pipeline complete.")


if __name__ == "__main__":
    asyncio.run(main())
