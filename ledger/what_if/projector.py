"""
ledger/what_if/projector.py — Counterfactual (what-if) projector
================================================================
Runs two parallel timelines against InMemoryEventStore instances
and returns the divergence between them.

Algorithm:
  1. Load all events for loan-{application_id}.
  2. Find first occurrence of branch_at_event_type.
  3. Split: pre_branch = events before branch point.
  4. Identify causally dependent events (causation_id traces back to
     branch point or any later event).
  5. Post-branch independent = events after branch point with no
     causal dependency on the branch.
  6. Real timeline      = all events.
     Counterfactual     = pre_branch + counterfactual_events + post_branch_independent.
  7. Apply both timelines to separate InMemoryEventStore instances
     (NEVER the real store).
  8. Run each projection against both timelines.
  9. Return WhatIfResult.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ledger.event_store import InMemoryEventStore


# ═══════════════════════════════════════════════════════════════════════════════
# RESULT TYPE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WhatIfResult:
    """Outcome of a what-if counterfactual projection run."""
    real_outcome: dict            # Last event from real timeline's loan-{id} stream
    counterfactual_outcome: dict  # Last event from counterfactual timeline's loan-{id} stream
    divergence_events: list[dict] = field(default_factory=list)  # Events only in counterfactual


# ═══════════════════════════════════════════════════════════════════════════════
# CAUSAL DEPENDENCY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _build_event_id_index(events: list[dict]) -> dict[str, dict]:
    """Build a mapping from event_id → event for fast lookup."""
    return {str(e["event_id"]): e for e in events}


def _is_causally_dependent(
    event: dict,
    branch_event_ids: set[str],
    id_index: dict[str, dict],
) -> bool:
    """
    Walk the causation_id chain upward.
    Returns True if any ancestor's causation_id points to a branch event.
    Events without a causation_id in metadata are treated as independent.
    """
    visited: set[str] = set()
    current = event

    while True:
        meta = current.get("metadata") or {}
        causation_id = meta.get("causation_id")

        if not causation_id:
            return False

        causation_id = str(causation_id)

        # Cycle guard
        if causation_id in visited:
            return False
        visited.add(causation_id)

        # Direct hit — this event was caused by a branch-point event
        if causation_id in branch_event_ids:
            return True

        # Walk up the chain
        parent = id_index.get(causation_id)
        if parent is None:
            return False

        current = parent


# ═══════════════════════════════════════════════════════════════════════════════
# TIMELINE REPLAY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def _replay_timeline(
    stream_id: str,
    events: list[dict],
    projections: list,
) -> dict:
    """
    Append *events* into a fresh InMemoryEventStore under *stream_id*,
    run each projection's handle() for every event, and return the last
    stored event as the outcome dict.
    """
    mem_store = InMemoryEventStore()

    # Seed the stream — use expected_version=-1 for the first batch
    if events:
        # Append one at a time to keep stream_position monotonic and
        # avoid OCC conflicts when replaying events that already have
        # assigned positions.
        for event in events:
            version = await mem_store.stream_version(stream_id)
            await mem_store.append(
                stream_id=stream_id,
                events=[{
                    "event_type": event.get("event_type", ""),
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                }],
                expected_version=version,
                metadata=dict(event.get("metadata") or {}),
            )

    # Run projections against the stored events
    stored = await mem_store.load_stream(stream_id)
    for stored_event in stored:
        for projection in projections:
            await projection.handle(stored_event)

    # Outcome = last event in the stream (or empty dict if no events)
    outcome = stored[-1] if stored else {}
    return dict(outcome)


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════

async def run_what_if(
    store,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[dict],
    projections: list,
) -> WhatIfResult:
    """
    Run a counterfactual what-if analysis for a loan application.

    Parameters
    ----------
    store:
        The real event store (read-only — never written to).
    application_id:
        The loan application identifier (stream = ``loan-{application_id}``).
    branch_at_event_type:
        The event type at which the timeline diverges.
    counterfactual_events:
        Replacement events injected after the branch point.
    projections:
        Projection instances with an async ``handle(event)`` method.

    Returns
    -------
    WhatIfResult
    """
    stream_id = f"loan-{application_id}"

    # ── 1. Load all real events ───────────────────────────────────────────────
    all_events: list[dict] = await store.load_stream(stream_id)

    # ── 2. Find first occurrence of branch_at_event_type ─────────────────────
    branch_index: int | None = None
    for i, event in enumerate(all_events):
        if event.get("event_type") == branch_at_event_type:
            branch_index = i
            break

    # If the branch event is not found, treat the entire stream as pre-branch
    # and append counterfactual events at the end.
    if branch_index is None:
        pre_branch = list(all_events)
        post_branch_all: list[dict] = []
    else:
        pre_branch = all_events[:branch_index]
        post_branch_all = all_events[branch_index:]  # includes the branch event itself

    # ── 3 & 4. Identify causally dependent post-branch events ─────────────────
    # Build an index of ALL events for causation chain walking
    id_index = _build_event_id_index(all_events)

    # The "branch event IDs" are the event_ids of the branch event and all
    # events that come after it in the real timeline.
    branch_event_ids: set[str] = {
        str(e["event_id"]) for e in post_branch_all
    }

    # ── 5. Post-branch independent events ────────────────────────────────────
    post_branch_independent: list[dict] = [
        e for e in post_branch_all
        if not _is_causally_dependent(e, branch_event_ids, id_index)
        and e.get("event_type") != branch_at_event_type  # exclude the branch event itself
    ]

    # ── 6. Build timelines ────────────────────────────────────────────────────
    real_timeline = list(all_events)
    counterfactual_timeline = pre_branch + counterfactual_events + post_branch_independent

    # ── 7 & 8. Replay both timelines through fresh InMemoryEventStore instances
    real_outcome = await _replay_timeline(stream_id, real_timeline, projections)
    counterfactual_outcome = await _replay_timeline(
        stream_id, counterfactual_timeline, projections
    )

    # ── 9. Compute divergence events ─────────────────────────────────────────
    # Events in counterfactual that are NOT in the real timeline
    # Comparison is by (event_type, position-in-list) to handle duplicates.
    real_event_types = [e.get("event_type") for e in real_timeline]
    divergence_events: list[dict] = []
    for i, event in enumerate(counterfactual_timeline):
        et = event.get("event_type")
        # An event diverges if it doesn't appear at the same position in the
        # real timeline, or if the real timeline is shorter.
        if i >= len(real_event_types) or real_event_types[i] != et:
            divergence_events.append(dict(event))

    return WhatIfResult(
        real_outcome=real_outcome,
        counterfactual_outcome=counterfactual_outcome,
        divergence_events=divergence_events,
    )
