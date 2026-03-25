"""
ledger/upcasters.py — Shared UpcasterRegistry instance
=======================================================
Registers all known event upcasters against a shared registry.

Upcasters transform old event versions to the current version ON READ.
They NEVER write to the events table. Immutability is non-negotiable.

The shared `registry` instance is passed to EventStore and InMemoryEventStore
so that load_stream() and load_all() apply upcasters automatically.
"""
from __future__ import annotations

from ledger.event_store import UpcasterRegistry

registry = UpcasterRegistry()


@registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    payload = dict(payload)
    payload.setdefault("model_versions", {})
    payload.setdefault("confidence_score", None)   # not fabricated
    payload.setdefault("regulatory_basis", [])
    return payload


@registry.upcaster("DecisionGenerated", from_version=1, to_version=2)
def upcast_decision_v1_to_v2(payload: dict) -> dict:
    # model_versions reconstructed lazily — set to {} here,
    # callers that need it must load contributing session streams
    payload = dict(payload)
    payload.setdefault("model_versions", {})
    return payload
