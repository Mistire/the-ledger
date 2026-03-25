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
    """
    Upcast CreditAnalysisCompleted from v1 to v2.

    Inference strategy:
    - model_versions: if `recorded_at` is present in the payload metadata, we
      record it as evidence of when the event was written so downstream tooling
      can correlate model deployments by date. Otherwise we leave it empty.
      We do NOT fabricate a model identifier — that would be a lie in the audit
      trail.
    - confidence_score: kept as None. The v1 schema did not capture this field
      and there is no safe way to reconstruct it post-hoc. Returning None is
      the honest answer; callers must treat None as "unknown, not zero".
    - regulatory_basis: kept as [] — no regulatory tags were recorded in v1.
    """
    payload = dict(payload)
    recorded_at = payload.get("recorded_at")
    if recorded_at:
        payload.setdefault(
            "model_versions",
            {"inferred_from_recorded_at": True, "recorded_at": str(recorded_at)},
        )
    else:
        payload.setdefault("model_versions", {})
    payload.setdefault("confidence_score", None)   # genuinely unknown — do not fabricate
    payload.setdefault("regulatory_basis", [])
    return payload


@registry.upcaster("DecisionGenerated", from_version=1, to_version=2)
def upcast_decision_v1_to_v2(payload: dict) -> dict:
    """
    Upcast DecisionGenerated from v1 to v2.

    model_versions is set to a sentinel dict rather than {} so that callers
    can distinguish "not yet loaded" from "genuinely empty". Any caller that
    needs the real per-model version map must load the contributing agent
    session streams and replay them — this is intentionally lazy to avoid
    N+1 stream loads on every decision read. The sentinel makes that contract
    explicit rather than silently returning an empty dict.
    """
    payload = dict(payload)
    payload.setdefault(
        "model_versions",
        {"_note": "reconstruct_from_contributing_sessions"},
    )
    return payload
