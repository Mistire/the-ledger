"""
tests/test_integrity.py
=======================
Property-based and example tests for the cryptographic audit chain.

Validates: Requirements 19.1–19.2 | Properties 16, 17
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from ledger.event_store import InMemoryEventStore
from ledger.integrity.audit_chain import run_integrity_check


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def new_id() -> str:
    return str(uuid.uuid4())


async def _seed_loan_stream(store: InMemoryEventStore, loan_id: str) -> None:
    """Append a minimal ApplicationSubmitted event to loan-{loan_id}."""
    await store.append(
        stream_id=f"loan-{loan_id}",
        events=[{
            "event_type": "ApplicationSubmitted",
            "event_version": 1,
            "payload": {
                "application_id": loan_id,
                "applicant_id": "applicant-001",
                "requested_amount_usd": "500000",
                "loan_purpose": "expansion",
                "submission_channel": "api",
            },
        }],
        expected_version=-1,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 16 — Hash chain continuity
# Validates: Requirements 19.1
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_hash_chain_continuity():
    """
    **Validates: Requirements 19.1**

    Run run_integrity_check twice on the same entity.
    The second run's previous_hash must equal the first run's integrity_hash,
    proving the hash chain is continuous across consecutive checks.
    """
    store = InMemoryEventStore()
    loan_id = new_id()

    await _seed_loan_stream(store, loan_id)

    # First integrity check
    first_result = await run_integrity_check(store, "loan", loan_id)

    # Second integrity check — previous_hash must equal first run's integrity_hash
    second_result = await run_integrity_check(store, "loan", loan_id)

    # Load the audit stream to inspect the second check's stored previous_hash
    audit_stream = f"audit-loan-{loan_id}"
    audit_events = await store.load_stream(audit_stream)

    # There should be exactly 2 AuditIntegrityCheckRun events
    check_events = [e for e in audit_events if e["event_type"] == "AuditIntegrityCheckRun"]
    assert len(check_events) == 2, f"Expected 2 audit events, got {len(check_events)}"

    second_check_payload = check_events[1]["payload"]

    assert second_check_payload["previous_hash"] == first_result.integrity_hash, (
        f"Hash chain broken: second run's previous_hash={second_check_payload['previous_hash']!r} "
        f"!= first run's integrity_hash={first_result.integrity_hash!r}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY 17 — Tamper detection
# Validates: Requirements 19.2
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_tamper_detection():
    """
    **Validates: Requirements 19.2**

    Run run_integrity_check once to establish the chain.
    Directly mutate the stored event payload in InMemoryEventStore._streams
    to simulate tampering.
    Run run_integrity_check again and assert tamper_detected=True and
    chain_valid=False.
    """
    store = InMemoryEventStore()
    loan_id = new_id()

    await _seed_loan_stream(store, loan_id)

    # First integrity check — establishes the chain
    first_result = await run_integrity_check(store, "loan", loan_id)
    assert first_result.tamper_detected is False
    assert first_result.chain_valid is True

    # Directly mutate the stored event payload to simulate tampering
    stream_key = f"loan-{loan_id}"
    store._streams[stream_key][0]["payload"]["requested_amount_usd"] = "TAMPERED"

    # Second integrity check — must detect the tampering
    second_result = await run_integrity_check(store, "loan", loan_id)

    assert second_result.tamper_detected is True, (
        "Expected tamper_detected=True after payload mutation"
    )
    assert second_result.chain_valid is False, (
        "Expected chain_valid=False after payload mutation"
    )
