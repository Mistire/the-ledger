"""
ledger/integrity/audit_chain.py — Cryptographic Audit Chain
============================================================
Provides run_integrity_check() which computes a SHA-256 hash chain over
an entity's primary event stream and appends an AuditIntegrityCheckRun
event to the audit stream.

Algorithm:
  1. Load all events from the entity's primary stream in stream_position order.
  2. Load last AuditIntegrityCheckRun from audit-{entity_type}-{entity_id}.
  3. Compute SHA-256 over: previous_hash + concatenated JSON payloads (sort_keys=True).
  4. Compare against stored hash — if recomputed hash differs from previous_hash
     in last check, set tamper_detected=True.
  5. Append AuditIntegrityCheckRun event to audit stream.
  6. Return IntegrityCheckResult.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, UTC


@dataclass
class IntegrityCheckResult:
    """Result of a single integrity check run."""
    events_verified: int
    chain_valid: bool
    tamper_detected: bool
    integrity_hash: str
    check_timestamp: datetime


async def run_integrity_check(
    store,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    Run a cryptographic integrity check on an entity's primary event stream.

    Validates: Requirements 9.1–9.5 | Properties 16, 17
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    # 1. Load all events from the primary stream in stream_position order
    events = await store.load_stream(primary_stream)

    # 2. Load last AuditIntegrityCheckRun from the audit stream
    audit_events = await store.load_stream(audit_stream)
    last_check = None
    for e in reversed(audit_events):
        if e.get("event_type") == "AuditIntegrityCheckRun":
            last_check = e
            break

    previous_hash: str | None = None
    if last_check:
        previous_hash = last_check.get("payload", {}).get("integrity_hash")

    # 3. Compute SHA-256 over: previous_hash + concatenated JSON payloads
    h = hashlib.sha256()
    if previous_hash:
        h.update(previous_hash.encode())
    for event in events:
        h.update(json.dumps(event["payload"], sort_keys=True).encode())
    integrity_hash = h.hexdigest()

    # 4. Tamper detection: if there was a previous check, recompute from scratch
    #    and compare the recomputed hash against the stored previous_hash.
    #    If they differ, the chain has been tampered with.
    tamper_detected = False
    chain_valid = True

    if last_check:
        stored_hash = last_check.get("payload", {}).get("integrity_hash")
        # Recompute hash from scratch (no previous_hash seed) to verify the chain
        h_verify = hashlib.sha256()
        # The previous check's hash was computed with its own previous_hash seed.
        # To detect tampering: recompute using the same seed as the last check used.
        last_previous_hash = last_check.get("payload", {}).get("previous_hash")
        if last_previous_hash:
            h_verify.update(last_previous_hash.encode())
        # Load events that were present at the time of the last check
        # (all events up to and including the last check's verified count)
        last_events_verified = last_check.get("payload", {}).get("events_verified", 0)
        events_at_last_check = events[:last_events_verified]
        for event in events_at_last_check:
            h_verify.update(json.dumps(event["payload"], sort_keys=True).encode())
        recomputed = h_verify.hexdigest()

        if stored_hash and recomputed != stored_hash:
            tamper_detected = True
            chain_valid = False

    # 5. Append AuditIntegrityCheckRun event to the audit stream
    check_timestamp = datetime.now(UTC)
    audit_version = await store.stream_version(audit_stream)

    await store.append(
        audit_stream,
        [
            {
                "event_type": "AuditIntegrityCheckRun",
                "event_version": 1,
                "payload": {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "events_verified": len(events),
                    "integrity_hash": integrity_hash,
                    "previous_hash": previous_hash,
                    "tamper_detected": tamper_detected,
                    "chain_valid": chain_valid,
                    "check_timestamp": check_timestamp.isoformat(),
                },
            }
        ],
        expected_version=audit_version,
    )

    # 6. Return IntegrityCheckResult
    return IntegrityCheckResult(
        events_verified=len(events),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        integrity_hash=integrity_hash,
        check_timestamp=check_timestamp,
    )
