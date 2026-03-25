"""
ledger/integrity/gas_town.py — Gas Town Crash Recovery
=======================================================
Provides reconstruct_agent_context() which replays an agent session stream
and produces a compact, token-budget-aware context for crash recovery.

Algorithm:
  1. Load full agent-{agent_id}-{session_id} stream.
  2. Identify last completed action, pending work, current application state.
  3. If total token count > token_budget:
     - Preserve verbatim: last 3 events + any PENDING/ERROR events
     - Summarise older events into compact prose
  4. Set session_health_status:
     - "FAILED" if AgentSessionFailed event present
     - "NEEDS_RECONCILIATION" if last event is decision-type with no AgentSessionCompleted
     - "HEALTHY" otherwise
  5. Return AgentContext.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field


# Decision-type events that trigger NEEDS_RECONCILIATION when they are the last event
DECISION_EVENT_TYPES = frozenset({
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "DecisionGenerated",
    "ComplianceCheckCompleted",
})

# Events that indicate pending or error states — always preserved verbatim
PENDING_OR_ERROR_EVENT_TYPES = frozenset({
    "AgentInputValidationFailed",
    "AgentSessionFailed",
    "ExtractionFailed",
    "FraudAnomalyDetected",
})

# ~1 token ≈ 4 characters of JSON
_CHARS_PER_TOKEN = 4


def _estimate_tokens(event: dict) -> int:
    """Estimate token count for a single event."""
    return max(1, len(json.dumps(event)) // _CHARS_PER_TOKEN)


def _event_to_text(event: dict) -> str:
    """Serialise an event to a compact JSON string for context."""
    return json.dumps(event, default=str)


def _summarise_events(events: list[dict]) -> str:
    """
    Produce a compact summary of a list of older events.

    Format: "[Summarised {n} events: {agent_type} processed {application_id}.
              Nodes executed: {node_names}. Tools called: {tool_names}.]"
    """
    if not events:
        return ""

    n = len(events)

    # Extract agent_type and application_id from any available payload
    agent_type = "unknown"
    application_id = "unknown"
    node_names: list[str] = []
    tool_names: list[str] = []

    for e in events:
        payload = e.get("payload", {})
        if not agent_type or agent_type == "unknown":
            agent_type = payload.get("agent_type", agent_type)
        if not application_id or application_id == "unknown":
            application_id = payload.get("application_id", application_id)

        # Collect node names from event types (strip agent-specific prefixes)
        et = e.get("event_type", "")
        if et and et not in ("AgentSessionStarted", "AgentSessionCompleted",
                              "AgentSessionFailed", "AgentInputValidated",
                              "AgentInputValidationFailed"):
            node_names.append(et)

        # Collect tool names from payload if present
        tools = payload.get("tools_called", [])
        if isinstance(tools, list):
            tool_names.extend(tools)

    unique_nodes = list(dict.fromkeys(node_names))  # preserve order, deduplicate
    unique_tools = list(dict.fromkeys(tool_names))

    nodes_str = ", ".join(unique_nodes) if unique_nodes else "none"
    tools_str = ", ".join(unique_tools) if unique_tools else "none"

    return (
        f"[Summarised {n} events: {agent_type} processed {application_id}. "
        f"Nodes executed: {nodes_str}. Tools called: {tools_str}.]"
    )


@dataclass
class AgentContext:
    """Reconstructed agent context for crash recovery."""
    context_text: str
    last_event_position: int
    pending_work: list[str] = field(default_factory=list)
    session_health_status: str = "HEALTHY"  # "HEALTHY" | "NEEDS_RECONCILIATION" | "FAILED"


async def reconstruct_agent_context(
    store,
    agent_id: str,
    session_id: str,
    token_budget: int = 4000,
) -> AgentContext:
    """
    Reconstruct agent context from the session event stream for crash recovery.

    Validates: Requirements 10.1–10.6 | Properties 18, 19
    """
    stream_id = f"agent-{agent_id}-{session_id}"

    # 1. Load full agent session stream
    events = await store.load_stream(stream_id)

    if not events:
        return AgentContext(
            context_text="No events found for this agent session.",
            last_event_position=-1,
            pending_work=[],
            session_health_status="HEALTHY",
        )

    last_event_position = events[-1].get("stream_position", len(events) - 1)

    # 2. Determine session health status
    has_failed = any(e.get("event_type") == "AgentSessionFailed" for e in events)
    has_completed = any(e.get("event_type") == "AgentSessionCompleted" for e in events)
    last_event_type = events[-1].get("event_type", "")

    if has_failed:
        session_health_status = "FAILED"
    elif not has_completed and last_event_type in DECISION_EVENT_TYPES:
        session_health_status = "NEEDS_RECONCILIATION"
    else:
        session_health_status = "HEALTHY"

    # 3. Identify pending work — events that indicate incomplete actions
    pending_work: list[str] = []
    for e in events:
        et = e.get("event_type", "")
        payload = e.get("payload", {})
        if et == "AgentInputValidationFailed":
            missing = payload.get("missing_inputs", [])
            errors = payload.get("validation_errors", [])
            if missing:
                pending_work.append(f"Missing inputs: {', '.join(missing)}")
            if errors:
                pending_work.append(f"Validation errors: {', '.join(errors)}")
        elif et == "AgentSessionFailed":
            reason = payload.get("failure_reason", "unknown")
            pending_work.append(f"Session failed: {reason}")
        elif session_health_status == "NEEDS_RECONCILIATION" and et == last_event_type:
            pending_work.append(f"Pending completion after: {et}")

    # 4. Apply token budget — decide which events to summarise vs preserve verbatim
    total_tokens = sum(_estimate_tokens(e) for e in events)

    if total_tokens <= token_budget:
        # All events fit within budget — include verbatim
        context_parts = [_event_to_text(e) for e in events]
        context_text = "\n".join(context_parts)
    else:
        # Identify events to always preserve verbatim:
        #   - last 3 events
        #   - any PENDING/ERROR events
        preserve_indices: set[int] = set()

        # Last 3 events
        for i in range(max(0, len(events) - 3), len(events)):
            preserve_indices.add(i)

        # PENDING/ERROR events
        for i, e in enumerate(events):
            if e.get("event_type") in PENDING_OR_ERROR_EVENT_TYPES:
                preserve_indices.add(i)

        # Split into summarisable vs verbatim
        summarise_events = [e for i, e in enumerate(events) if i not in preserve_indices]
        verbatim_events = [(i, e) for i, e in enumerate(events) if i in preserve_indices]

        context_parts: list[str] = []

        # Add summary of older events first (if any)
        if summarise_events:
            context_parts.append(_summarise_events(summarise_events))

        # Add verbatim events in original order
        for _, e in sorted(verbatim_events, key=lambda x: x[0]):
            context_parts.append(_event_to_text(e))

        context_text = "\n".join(context_parts)

    return AgentContext(
        context_text=context_text,
        last_event_position=last_event_position,
        pending_work=pending_work,
        session_health_status=session_health_status,
    )
