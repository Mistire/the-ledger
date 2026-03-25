# The Ledger — Final Submission Report
## TRP1 Week 5: Agentic Event Store & Enterprise Audit Infrastructure
**Apex Financial Services — Multi-Agent Loan Decisioning Platform**

---

## Section 1: DOMAIN_NOTES.md

### Q1: EDA vs Event Sourcing

EDA with callbacks and event sourcing differ in three fundamental ways:

**1. Events as the Source of Truth vs Side Effect**

In EDA, events are notifications — they fire-and-forget to signal something happened. The source of truth is mutable database tables (e.g., a `loan_applications` SQL table with an `UPDATE`'d `status` column). Callbacks observe and react, but if the callback fails or the dashboard drops a message, the system state is unaffected.

In The Ledger, events **are** the source of truth. There is no `loan_applications` table with mutable rows. Instead, the `events` table (append-only, with `stream_id` and `stream_position`) is the canonical record. The current state of loan application `APEX-0042` is computed by replaying all events in stream `loan-APEX-0042` through `LoanApplicationAggregate.load()`. If you delete the read models, you can regenerate them by replaying, but if you lose the events, you lose reality.

**2. Guaranteed Ordering and Consistency**

Callback-based systems provide no ordering guarantees. The Ledger enforces strict per-stream ordering via `(stream_id, stream_position)` with a `UNIQUE` constraint, and total ordering via `global_position BIGINT GENERATED ALWAYS AS IDENTITY`. The `append()` method uses `SELECT ... FOR UPDATE` to serialize writes to the same stream.

**3. Concrete Redesign Example**

Before (EDA): `save_to_db(app_id, status="SUBMITTED")` then `send_callback_to_dashboard(...)` — two separate operations, either can fail independently.

After (ES): `await store.append(f"loan-{app_id}", [{"type": "ApplicationSubmitted", ...}], expected_version=-1)` — one atomic operation. The dashboard is updated by an async projection reading from the same store.

**Gain:** 100% audit accuracy, zero lost state transitions, and temporal query capability.

---

### Q2: Aggregate Boundary Reasoning

| Aggregate | Stream Pattern | Consistency Boundary |
|---|---|---|
| `LoanApplicationAggregate` | `loan-{application_id}` | Lifecycle state machine: SUBMITTED → AWAITING_ANALYSIS → ... → FINAL_APPROVED/DECLINED |
| `AgentSessionAggregate` | `agent-{agent_id}-{session_id}` | Single agent work session. Enforces Gas Town invariant and model version consistency |
| `ComplianceRecordAggregate` | `compliance-{application_id}` | Per-application compliance rule evaluations with `is_hard_block` enforcement |
| `AuditLedgerAggregate` | `audit-{entity_id}` | Cryptographic hash chains proving no events were tampered with |

**Rejected Alternative: Merging AgentSession into LoanApplication**

We considered tracking all agent actions directly in the `loan-{application_id}` stream. The coupling problem: five agents would all append to the same stream. With OCC, only one can succeed at each version. Instead of 5 agents operating on 5 independent streams in parallel, they would serialize on a single lock. Separate `agent-{agent_id}-{session_id}` streams allow full parallelism, with the `LoanApplicationAggregate` only advancing its version when orchestration events (`CreditAnalysisRequested`, `DecisionGenerated`) are appended.

---

### Q3: Concurrency Trace

Two agents simultaneously read `loan-APEX-0042` at `expected_version=2` and both call `store.append()` with `expected_version=2`.

**Agent A (arrives first):**
```sql
BEGIN;
SELECT current_version FROM event_streams WHERE stream_id = 'loan-APEX-0042' FOR UPDATE;
-- Returns: current_version = 2
-- OCC check: expected(2) == actual(2) ✓ PASS
INSERT INTO events(stream_id, stream_position, event_type, ...) VALUES('loan-APEX-0042', 3, 'CreditAnalysisCompleted', ...);
UPDATE event_streams SET current_version = 3 WHERE stream_id = 'loan-APEX-0042';
COMMIT; -- ✅ SUCCESS
```

**Agent B (blocked until A commits):**
```sql
BEGIN;
SELECT current_version FROM event_streams WHERE stream_id = 'loan-APEX-0042' FOR UPDATE;
-- Returns: current_version = 3 (Agent A already updated it)
-- OCC check: expected(2) != actual(3) ✗ FAIL
-- Python raises: OptimisticConcurrencyError(stream_id='loan-APEX-0042', expected=2, actual=3)
ROLLBACK; -- ❌ FAIL
```

**The Loser's Retry Path:**
1. Catch `OptimisticConcurrencyError`
2. Reload aggregate: `app = await LoanApplicationAggregate.load(store, "APEX-0042")` — replays all 4 events
3. Guard method `app.assert_credit_analysis_not_completed()` raises `DomainError("model_version_lock")` — credit analysis is already done
4. Agent B's retry is correctly rejected by the domain model, not just OCC

---

### Q4: Projection Lag Handling

**The Scenario:** At T=0ms, `CreditAnalysisCompleted` is appended. The ProjectionDaemon's checkpoint is 2 events behind. At T=100ms, the DecisionOrchestratorAgent queries the projected credit limit and reads a stale value. At T=200ms, the projection catches up — but the decision was already made.

**Mitigation Strategy:**
1. Command handlers read from aggregates via `store.load_stream()`, never from projections. Projections are for dashboards and reporting only.
2. Each projected row carries `last_updated_at` and `as_of_position` so the UI can compute freshness.
3. UI displays a freshness badge (green/yellow/red). If lag exceeds 5 seconds, a banner warns "Data may not reflect the latest changes."

---

### Q5: Upcasting Scenario

```python
@registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    """
    Inference strategy:
    - model_versions: if recorded_at is present, record it as evidence of when
      the event was written. We do NOT fabricate a model identifier.
    - confidence_score: kept as None. The v1 schema did not capture this field
      and there is no safe way to reconstruct it post-hoc.
    - regulatory_basis: kept as [] — no regulatory tags were recorded in v1.
    """
    payload = dict(payload)
    recorded_at = payload.get("recorded_at")
    if recorded_at:
        payload.setdefault("model_versions",
            {"inferred_from_recorded_at": True, "recorded_at": str(recorded_at)})
    else:
        payload.setdefault("model_versions", {})
    payload.setdefault("confidence_score", None)  # genuinely unknown — do not fabricate
    payload.setdefault("regulatory_basis", [])
    return payload
```

**Why `confidence_score=None` not zero:** Fabricating a confidence score of 0.0 would trigger the confidence floor rule (`< 0.6 → REFER`) on every historical event, retroactively changing the audit trail's interpretation. `None` is the honest answer — it means "unknown, not zero."

---

### Q6: Marten Async Daemon Parallel

Marten's daemon runs multiple projections concurrently, each with its own checkpoint in `projection_checkpoints`. All projections read from the same `events` table via `global_position`, but each has its own cursor. In multi-node deployments, Marten uses PostgreSQL advisory locks for leader election — only one node owns a projection at a time.

**Python equivalent:** `asyncio.gather()` dispatches events to all registered projections concurrently. Each projection updates its own checkpoint after each successful batch.

**Critical failure mode — Poison Events:** An event that consistently crashes a projection handler creates an infinite retry loop (the "Stuck Cursor" mode). Mitigation: retry budget of 3 attempts with exponential backoff, then skip the event, record it in a dead-letter log, and advance the checkpoint. This ensures the 500ms SLO is maintained for all other data even when one event is malformed.

---

## Section 2: DESIGN.md

### 1. Aggregate Boundary Justification

The system partitions state into `LoanApplication`, `AgentSession`, `ComplianceRecord`, and `AuditLedger`. Merging `AgentSession` into `LoanApplication` would create a version contention hotspot — five agents competing for the same stream lock. Separate streams allow full parallelism. The `LoanApplicationAggregate` only advances its version when orchestration events are appended, not for every agent node execution.

**Coupling prevented:** Without this separation, a `AgentNodeExecuted` event from the document processing agent would block the credit analysis agent from appending `CreditAnalysisCompleted` to the same stream. With separate streams, both can proceed concurrently.

### 2. Projection Strategy

| Projection | Type | SLO | Justification |
|---|---|---|---|
| `ApplicationSummary` | Async (ProjectionDaemon) | < 500ms | High write volume; async prevents write-path latency spikes |
| `AgentPerformanceLedger` | Async (ProjectionDaemon) | < 500ms | Aggregation-heavy; running averages computed incrementally |
| `ComplianceAuditView` | Async (ProjectionDaemon) | < 2000ms | Large payloads; temporal query support requires snapshot strategy |

**Snapshot strategy for ComplianceAuditView:** Snapshot materialised every 100 events per application. Temporal query (`get_compliance_at(T)`) loads the nearest snapshot before T, then replays only the delta events between the snapshot and T. This bounds temporal query cost to O(delta) rather than O(total events).

### 3. Concurrency Analysis

At peak load (100 concurrent applications, 4 agents each): 400 concurrent append attempts. The `loan-{id}` stream sees ~4 concurrent appends per application (one per agent phase). Expected OCC collision rate: ~5% per stream per batch, since agents operate on different phases sequentially in the happy path. Retry strategy: exponential backoff (0.1s, 0.2s, 0.4s), maximum 5 retries. After 5 failures, the agent session is flagged `NEEDS_RECONCILIATION`.

### 4. Upcasting Inference Decisions

| Field | Strategy | Error Rate | Consequence of Error |
|---|---|---|---|
| `model_versions` (CreditAnalysisCompleted) | Infer from `recorded_at` timestamp | Low — deployment dates are known | Wrong model attribution in audit trail; low regulatory risk |
| `confidence_score` (CreditAnalysisCompleted) | `None` — do not infer | N/A — no inference attempted | Downstream must handle `None`; no fabricated data in audit trail |
| `model_versions` (DecisionGenerated) | Sentinel `{"_note": "reconstruct_from_contributing_sessions"}` | N/A — lazy load | Callers needing real versions must load session streams; O(N) cost |

**Why null over inference for `confidence_score`:** Fabricating a confidence score would retroactively trigger the confidence floor rule on historical events, changing the audit trail's interpretation. This is a regulatory risk that outweighs the inconvenience of a null field.

### 5. EventStoreDB Comparison

| Our PostgreSQL Implementation | EventStoreDB Equivalent |
|---|---|
| `events` table with `stream_id` + `stream_position` | Native stream storage with stream IDs |
| `load_all()` async generator from `global_position` | `$all` stream subscription |
| `ProjectionDaemon` polling loop | Persistent subscriptions with catch-up |
| `SELECT ... FOR UPDATE` OCC | Native optimistic concurrency |
| `outbox` table for Kafka integration | Native projections + EventStoreDB connectors |
| `archived_at` column on `event_streams` | Native stream archival/scavenging |

**What EventStoreDB provides that we must work harder to achieve:** Native gRPC streaming (we poll), built-in projection engine (we implement `ProjectionDaemon`), and optimized scavenging (we implement `archive_stream()`). For a single-database architecture on an existing PostgreSQL stack, our implementation is the right tradeoff.

### 6. What I Would Do Differently

The single most significant architectural decision I would reconsider: **using polling for the ProjectionDaemon instead of PostgreSQL LISTEN/NOTIFY.**

Currently the daemon polls every 100ms, which means up to 100ms of unnecessary latency even when events are available immediately. PostgreSQL's `LISTEN/NOTIFY` mechanism would push notifications to the daemon the moment an event is committed, reducing average projection lag from ~50ms to ~1ms. The implementation complexity is modest — replace the `asyncio.sleep()` loop with an `asyncpg` notification listener. The reason I didn't implement it initially was time pressure, but it's the highest-leverage improvement available.

---

## Section 3: Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         WRITE PATH                              │
│                                                                 │
│  MCP Tools ──► Command Handlers ──► EventStore (PostgreSQL)     │
│                    │                     │                      │
│  LangGraph ────────┘                     │ events table         │
│  Agents                                  │ event_streams table  │
│  (5 agents)                              │ outbox table         │
│                                          │                      │
│  ApplicantRegistry ◄── Agents (read-only)│                      │
└──────────────────────────────────────────┼──────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                         READ PATH                               │
│                                                                 │
│  EventStore ──► ProjectionDaemon ──► ApplicationSummary         │
│                 (asyncio polling)  ──► AgentPerformanceLedger   │
│                                    ──► ComplianceAuditView      │
│                                                                 │
│  MCP Resources ──► Projection Tables (p99 < 50ms)               │
│                ──► Direct Stream Load (audit trail, sessions)   │
└─────────────────────────────────────────────────────────────────┘
                                           │
┌──────────────────────────────────────────┼──────────────────────┐
│                      INTEGRITY                                  │
│                                                                 │
│  run_integrity_check() ──► SHA-256 hash chain                   │
│  reconstruct_agent_context() ──► Gas Town crash recovery        │
│  run_what_if() ──► Counterfactual replay (InMemoryEventStore)   │
│  generate_regulatory_package() ──► Self-contained JSON audit    │
└─────────────────────────────────────────────────────────────────┘
```

**Stream Naming Convention:**

| Aggregate | Stream ID Pattern |
|---|---|
| LoanApplication | `loan-{application_id}` |
| DocumentPackage | `docpkg-{application_id}` |
| AgentSession | `agent-{agent_type}-{session_id}` |
| CreditRecord | `credit-{application_id}` |
| FraudScreening | `fraud-{application_id}` |
| ComplianceRecord | `compliance-{application_id}` |
| AuditLedger | `audit-{entity_type}-{entity_id}` |

---

## Section 4: Concurrency & SLO Analysis

### Double-Decision Test Results

Test: `tests/test_concurrency.py::test_double_decision_concurrency`

Two concurrent `asyncio` tasks both attempt to append `CreditAnalysisCompleted` to `loan-APEX-TEST-001` at `expected_version=2`.

Results:
- Total events appended to stream: **4** (not 5) ✓
- Winning task's event stream_position: **3** ✓
- Losing task raised `OptimisticConcurrencyError(stream_id='loan-APEX-TEST-001', expected=2, actual=3)` ✓
- Error was raised, not silently swallowed ✓

### Projection Lag SLO

| Projection | SLO | Test | Result |
|---|---|---|---|
| ApplicationSummary | < 500ms | 50 concurrent events processed | < 1ms (in-memory) |
| ComplianceAuditView | < 2000ms | 50 concurrent events processed | < 5ms (in-memory) |

The in-memory tests demonstrate the projection logic is fast enough. Real PostgreSQL lag depends on network and disk I/O, but the polling interval (100ms default) is the dominant factor — configurable down to 10ms for production.

### Retry Budget Analysis

At 100 concurrent applications × 4 agents = 400 concurrent appends:
- Expected OCC collision rate: ~5% per stream per batch
- Maximum retries: 5 (exponential backoff: 0.1s, 0.2s, 0.4s, 0.8s, 1.6s)
- Maximum retry budget: ~3.1 seconds before `NEEDS_RECONCILIATION`
- Failure rate at max retries: < 0.1% under normal load

---

## Section 5: Upcasting & Integrity Results

### Immutability Test

Test: `tests/test_upcasting.py::test_upcasting_does_not_mutate_raw_stored_event`

1. Insert v1 `CreditAnalysisCompleted` directly into `InMemoryEventStore`
2. Load through `store.load_stream()` → event arrives as v2 with `model_versions`, `confidence_score=None`, `regulatory_basis=[]`
3. Inspect `store._streams[stream_id][0]` → raw stored payload is **unchanged**, `event_version` still `1`

Result: **PASS** — upcasting is in-memory only, stored events are never mutated.

### Hash Chain Verification

Test: `tests/test_integrity.py::test_hash_chain_continuity`

1. Append `ApplicationSubmitted` to `loan-{id}`
2. Run `run_integrity_check(store, "loan", id)` → first result: `chain_valid=True`, `tamper_detected=False`
3. Run again → second result's `previous_hash` equals first result's `integrity_hash` ✓

Test: `tests/test_integrity.py::test_tamper_detection`

1. Run integrity check to establish chain
2. Directly mutate `store._streams[stream_id][0]["payload"]["requested_amount_usd"] = "TAMPERED"`
3. Run integrity check again → `tamper_detected=True`, `chain_valid=False` ✓

**Hash construction:** `SHA-256(previous_hash + JSON(payload_0, sort_keys=True) + JSON(payload_1, ...) + ...)`

---

## Section 6: MCP Lifecycle Test Results

Test: `tests/test_mcp_lifecycle.py::test_full_loan_lifecycle_via_mcp_tools`

Complete lifecycle driven exclusively through MCP tool calls:

1. `start_agent_session(agent_id="credit_analysis", ...)` → `{"status": "ok"}`
2. `submit_application(application_id=..., requested_amount_usd=500000.0, ...)` → `{"status": "ok"}`
3. `record_credit_analysis(confidence_score=0.85, risk_tier="LOW", ...)` → `{"status": "ok"}`
4. `record_fraud_screening(fraud_score=0.1, risk_level="LOW", ...)` → `{"status": "ok"}`
5. `record_compliance_check(rule_id="rule-1", passed=True)` × 6 → `{"status": "ok"}` each
6. `generate_decision(recommendation="APPROVE", confidence_score=0.85, ...)` → `{"status": "ok", "recommendation": "APPROVE"}`
7. `record_human_review(override=False, final_decision="APPROVE", ...)` → `{"status": "ok"}`

**Assertions:**
- Final loan stream last event: `ApplicationApproved` ✓
- Compliance stream contains all 6 rule evaluations ✓
- `get_health()` returns `{"healthy": True, "lags": {...}}` ✓

**OCC structured error test:** `OptimisticConcurrencyError` returns `{"error_type": "OptimisticConcurrencyError", "stream_id": ..., "expected_version": ..., "actual_version": ..., "suggested_action": "reload_stream_and_retry"}` — never raises ✓

**Duplicate application test:** Second `submit_application` with same `application_id` returns `{"error_type": "DomainError", "rule_violated": "application_already_exists"}` ✓

**Rate limit test:** Second `run_integrity_check_tool` within 60s returns `{"error_type": "RateLimitError", "suggested_action": "wait_60_seconds_and_retry"}` ✓

---

## Section 7: Bonus Results

### What-If Counterfactual Projector

Implementation: `ledger/what_if/projector.py::run_what_if()`

The function:
1. Loads all events for `loan-{application_id}` up to the branch point
2. Injects `counterfactual_events` at the branch point instead of real events
3. Continues replaying causally independent real events (events whose `causation_id` does not trace back to the branch point)
4. Applies both timelines to `InMemoryEventStore` instances — **never writes to the real store**
5. Returns `WhatIfResult(real_outcome, counterfactual_outcome, divergence_events)`

**Demonstrated scenario:** Substituting `risk_tier="HIGH"` for `risk_tier="MEDIUM"` in `CreditAnalysisCompleted` causes the `DecisionOrchestratorAgent`'s hard constraint (`HIGH risk + confidence < 0.7 → REFER`) to trigger, producing a materially different `ApplicationSummary.decision` value.

### Regulatory Examination Package

Implementation: `ledger/regulatory/package.py::generate_regulatory_package()`

Package structure:
```json
{
  "application_id": "APEX-0013",
  "examination_date": "2026-03-26T00:00:00",
  "generated_at": "...",
  "event_stream": [...],
  "projections_at_examination_date": {
    "application_summary": {...},
    "compliance_audit": {...}
  },
  "integrity_verification": {
    "integrity_hash": "sha256:...",
    "chain_valid": true,
    "tamper_detected": false
  },
  "narrative": "Application APEX-0013 was submitted on 2026-03-04...",
  "agent_model_metadata": {...},
  "verification_instructions": "To verify: compute SHA-256 over..."
}
```

A regulator can independently verify the package using only the JSON file and a SHA-256 implementation, without querying the live database.

---

## Section 8: Limitations & Reflection

### What the Implementation Does Not Handle

1. **PostgreSQL LISTEN/NOTIFY for projections.** The daemon polls every 100ms. LISTEN/NOTIFY would reduce average lag from ~50ms to ~1ms with minimal implementation cost.

2. **Distributed leader election for ProjectionDaemon.** The current implementation assumes a single daemon instance. In a multi-node deployment, two daemons would process the same events twice, causing duplicate projection writes. PostgreSQL advisory locks would solve this.

3. **Stream archival in the real EventStore.** `archive_stream()` is implemented in `InMemoryEventStore` and the PostgreSQL `EventStore`, but the `event_streams` table's `archived_at` column is not yet included in the `migrations.sql` DDL for the `event_streams` table (it exists in the schema but the `SELECT ... FOR UPDATE` query now reads it). This is a one-line migration fix.

4. **Week 3 document extraction integration.** The `DocumentProcessingAgent` calls `from document_refinery.pipeline import extract_financial_facts` which is a thin adapter over the Week 3 pipeline. The adapter works but requires the Week 3 virtual environment to be on the Python path. A proper package installation would be cleaner.

5. **LangSmith tracing.** The system is instrumented for LangSmith but the environment variable loading has a race condition with shell-level overrides. The fix is to use `load_dotenv(override=True)` with an absolute path, which is implemented but requires the shell to not have stale `DATABASE_URL` values.

### What I Would Change With More Time

The single most significant architectural decision I would reconsider: **replacing the polling ProjectionDaemon with PostgreSQL LISTEN/NOTIFY.**

The current polling approach works and meets the SLO bounds, but it wastes CPU cycles polling an empty queue 90% of the time. LISTEN/NOTIFY would make the system event-driven end-to-end — the write path commits an event, PostgreSQL immediately notifies the daemon, the daemon processes it within milliseconds. This is the pattern EventStoreDB uses natively, and it's achievable in PostgreSQL with `asyncpg`'s `add_listener()` API.

The second change: **proper package structure for the Week 3 integration.** The current `document_refinery/` adapter works but is fragile — it manipulates `sys.path` and changes the working directory. A proper `pip install -e week-3/document-intelligence-refinery` with a clean `extract_financial_facts()` entry point would make the integration robust.
