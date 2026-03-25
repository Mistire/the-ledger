"""
ledger/regulatory/package.py — Regulatory Examination Package Generator
========================================================================
Produces a self-contained, JSON-serialisable dict for regulatory examination.
"""
from __future__ import annotations

from datetime import datetime, UTC
from decimal import Decimal

from ledger.integrity.audit_chain import run_integrity_check


# ── Narrative templates ───────────────────────────────────────────────────────

def _build_narrative(events: list[dict]) -> list[str]:
    """Generate one sentence per significant event type found in the stream."""
    sentences: list[str] = []
    seen_types: set[str] = set()

    for event in events:
        et = event.get("event_type")
        p = event.get("payload", {})

        if et in seen_types:
            continue

        sentence: str | None = None

        if et == "ApplicationSubmitted":
            date = event.get("recorded_at", "")
            if isinstance(date, datetime):
                date = date.isoformat()
            sentence = (
                f"ApplicationSubmitted: Application {p.get('application_id')} submitted "
                f"by {p.get('applicant_id')} for ${p.get('requested_amount_usd')} on {date}."
            )

        elif et == "CreditAnalysisCompleted":
            decision = p.get("decision") or {}
            risk_tier = decision.get("risk_tier") if isinstance(decision, dict) else p.get("risk_tier")
            limit = decision.get("recommended_limit_usd") if isinstance(decision, dict) else p.get("recommended_limit_usd")
            confidence = decision.get("confidence") if isinstance(decision, dict) else p.get("confidence_score")
            conf_str = f"{confidence:.0%}" if isinstance(confidence, (int, float)) else str(confidence)
            sentence = (
                f"CreditAnalysisCompleted: Credit analysis completed with {risk_tier} risk, "
                f"${limit} recommended limit, {conf_str} confidence."
            )

        elif et == "FraudScreeningCompleted":
            score = p.get("fraud_score")
            score_str = f"{score:.2f}" if isinstance(score, (int, float)) else str(score)
            sentence = (
                f"FraudScreeningCompleted: Fraud screening completed with score {score_str}, "
                f"recommendation {p.get('recommendation')}."
            )

        elif et == "ComplianceCheckCompleted":
            verdict = p.get("overall_verdict")
            passed = p.get("rules_passed", 0)
            evaluated = p.get("rules_evaluated", 0)
            sentence = (
                f"ComplianceCheckCompleted: Compliance check completed with verdict {verdict}. "
                f"{passed}/{evaluated} rules passed."
            )

        elif et == "DecisionGenerated":
            rec = p.get("recommendation")
            confidence = p.get("confidence_score")
            conf_str = f"{confidence:.0%}" if isinstance(confidence, (int, float)) else str(confidence)
            sentence = (
                f"DecisionGenerated: Decision generated with recommendation {rec} "
                f"and {conf_str} confidence."
            )

        elif et == "HumanReviewCompleted":
            override = p.get("override", False)
            override_note = " (override)" if override else ""
            sentence = (
                f"HumanReviewCompleted: Human review by {p.get('reviewer_id')}: "
                f"{p.get('final_decision')}{override_note}."
            )

        elif et == "ApplicationApproved":
            amount = p.get("approved_amount_usd")
            rate = p.get("interest_rate_pct")
            term = p.get("term_months")
            sentence = (
                f"ApplicationApproved: Application approved for ${amount} "
                f"at {rate}% for {term} months."
            )

        elif et == "ApplicationDeclined":
            reasons = p.get("decline_reasons") or p.get("reasons") or []
            reasons_str = ", ".join(reasons) if reasons else "see event payload"
            sentence = f"ApplicationDeclined: Application declined. Reasons: {reasons_str}."

        elif et == "AgentSessionStarted":
            sentence = (
                f"AgentSessionStarted: Agent session started for agent {p.get('agent_id')} "
                f"({p.get('agent_type')}) with model {p.get('model_version')}."
            )

        if sentence:
            sentences.append(sentence)
            seen_types.add(et)

    return sentences


# ── Serialisation helper ──────────────────────────────────────────────────────

def _serialise(obj):
    """Recursively make an object JSON-serialisable."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialise(v) for v in obj]
    return obj


# ── Main function ─────────────────────────────────────────────────────────────

async def generate_regulatory_package(
    application_id: str,
    examination_date: datetime,
    store,
    projections: dict,
) -> dict:
    """
    Produces a self-contained JSON package for regulatory examination.

    Parameters
    ----------
    application_id : str
        The loan application identifier.
    examination_date : datetime
        The point-in-time for which projections are evaluated.
    store : EventStore | InMemoryEventStore
        The event store to load events from.
    projections : dict
        Optional projection instances keyed by name:
        "application_summary", "compliance_audit", "agent_performance".
    """
    stream_id = f"loan-{application_id}"

    # 1. Load full event stream
    raw_events = await store.load_stream(stream_id)
    event_stream = [_serialise(e) for e in raw_events]

    # 2. Projections at examination date
    proj_application_summary: dict = {}
    proj_compliance_audit: dict = {}
    proj_agent_performance: dict = {}

    if "application_summary" in projections:
        proj = projections["application_summary"]
        try:
            result = await proj.get_application_at(application_id, examination_date)
            proj_application_summary = _serialise(result) if result else {}
        except (AttributeError, Exception):
            # Fallback: try get_current if temporal query not available
            try:
                result = await proj.get_current(application_id)
                proj_application_summary = _serialise(result) if result else {}
            except Exception:
                proj_application_summary = {}

    if "compliance_audit" in projections:
        proj = projections["compliance_audit"]
        try:
            result = await proj.get_compliance_at(application_id, examination_date)
            proj_compliance_audit = _serialise(result) if result else {}
        except Exception:
            try:
                result = await proj.get_current_compliance(application_id)
                proj_compliance_audit = _serialise(result) if result else {}
            except Exception:
                proj_compliance_audit = {}

    if "agent_performance" in projections:
        proj = projections["agent_performance"]
        try:
            result = await proj.get_performance_at(application_id, examination_date)
            proj_agent_performance = _serialise(result) if result else {}
        except (AttributeError, Exception):
            proj_agent_performance = {}

    # 3. Integrity verification
    integrity_result = await run_integrity_check(store, "loan", application_id)
    integrity_verification = {
        "integrity_hash": integrity_result.integrity_hash,
        "chain_valid": integrity_result.chain_valid,
        "tamper_detected": integrity_result.tamper_detected,
        "events_verified": integrity_result.events_verified,
    }

    # 4. Narrative
    narrative_sentences = _build_narrative(raw_events)
    narrative = " ".join(narrative_sentences)

    # 5. Agent model versions — from AgentSessionStarted events
    agent_model_versions: dict[str, str] = {}
    for event in raw_events:
        if event.get("event_type") == "AgentSessionStarted":
            p = event.get("payload", {})
            session_id = p.get("session_id")
            model_version = p.get("model_version")
            if session_id and model_version:
                agent_model_versions[session_id] = model_version

    # 6. Input data hashes — from CreditAnalysisCompleted and FraudScreeningCompleted
    input_data_hashes: dict[str, str] = {}
    for event in raw_events:
        et = event.get("event_type")
        if et in ("CreditAnalysisCompleted", "FraudScreeningCompleted"):
            p = event.get("payload", {})
            session_id = p.get("session_id") or p.get("agent_id")
            input_data_hash = p.get("input_data_hash")
            if session_id and input_data_hash:
                input_data_hashes[session_id] = input_data_hash

    # 7. Verification instructions
    verification_instructions = (
        "To verify this package: (1) Load the event_stream list into an InMemoryEventStore "
        "by appending each event's payload to stream 'loan-{application_id}'. "
        "(2) Call run_integrity_check(store, 'loan', '{application_id}'). "
        "(3) Compare the returned integrity_hash with the value in integrity_verification.integrity_hash. "
        "A match confirms the event stream has not been tampered with. "
        "The SHA-256 hash is computed over the concatenated JSON payloads (sort_keys=True) "
        "of all events in stream_position order."
    ).format(application_id=application_id)

    return {
        "application_id": application_id,
        "examination_date": examination_date.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "event_stream": event_stream,
        "projections_at_examination_date": {
            "application_summary": proj_application_summary,
            "compliance_audit": proj_compliance_audit,
            "agent_performance": proj_agent_performance,
        },
        "integrity_verification": integrity_verification,
        "narrative": narrative,
        "agent_model_versions": agent_model_versions,
        "input_data_hashes": input_data_hashes,
        "verification_instructions": verification_instructions,
    }
