# DOMAIN_NOTES.md — The Ledger

> Complete these **before** writing any code. Each answer is graded independently.
> Aim for specificity, not generality — reference concrete table names, stream IDs,
> and Python class names from this codebase.

---

## Q1: Event-Driven Architecture (EDA) vs Event Sourcing (ES)

**Question:** "We already log everything with callbacks" — a colleague says the current callback-based tracing dashboard is equivalent to your Event Store design. Explain concretely what changes architecturally between EDA-style fire-and-forget events and the Event Store pattern used in The Ledger.

### Answer

EDA with callbacks and event sourcing differ in three fundamental ways:

**1. Events as the Source of Truth vs Side Effect**
In EDA, events are _notifications_ — they fire-and-forget to signal something happened. The source of truth is the mutable database tables (e.g., a `loan_applications` SQL table with an `UPDATE`'d `status` column). Callbacks observe and react, but if the callback fails or the dashboard drops a message, the system state is unaffected.

In The Ledger, events **are** the source of truth. There is no `loan_applications` table with mutable rows. Instead, the `events` table (append-only, with `stream_id` and `stream_position`) is the canonical record. The current state of loan application `APEX-0042` is computed by replaying all events in stream `loan-APEX-0042` through `LoanApplicationAggregate.load()`. If you delete the read models, you can regenerate them by replaying, but if you lose the events, you lose reality.

**2. Guaranteed Ordering and Consistency**
Callback-based systems provide no ordering guarantees — two callbacks may arrive out of order, or be processed by different services simultaneously. The Ledger enforces strict per-stream ordering via `(stream_id, stream_position)` with a `UNIQUE` constraint, and total ordering via `global_position BIGINT GENERATED ALWAYS AS IDENTITY`. The `append()` method uses `SELECT ... FOR UPDATE` to serialize writes to the same stream, guaranteeing that `CreditAnalysisCompleted` cannot be version 3 unless versions 0-2 already exist.

**3. Concrete Redesign Example**

*   **Before (EDA/Callbacks):**
    ```python
    def handle_submit(app_id):
        save_to_db(app_id, status="SUBMITTED") # Mutable update
        send_callback_to_dashboard(app_id, "submitted") # Side effect
    ```
*   **After (Event Sourcing):**
    ```python
    async def handle_submit(app_id):
        # The event is the ONLY record of truth
        await store.append(f"loan-{app_id}", [{"type": "ApplicationSubmitted", ...}], expected_version=-1)
        # Dashboard is later updated by an ASYNC projection reading from the same store.
    ```
**Gain:** 100% audit accuracy, zero "lost" state transitions, and the ability to "time-travel" to debug why a specific application was in a specific state at 3:00 PM last Tuesday.

---

## Q2: Aggregate Boundary Reasoning

**Question:** Justify the four aggregates chosen for the interim submission. Describe one alternative aggregate boundary you considered and rejected, and identify the coupling problem it would have introduced.

### Answer

**The Four Aggregates and Their Boundaries:**

| Aggregate | Stream Pattern | Consistency Boundary |
|-----------|---------------|---------------------|
| `LoanApplicationAggregate` | `loan-{application_id}` | Owns the lifecycle state machine: SUBMITTED → AWAITING_ANALYSIS → ANALYSIS_COMPLETE → ... → FINAL_APPROVED/DECLINED. All state transitions are validated here. |
| `AgentSessionAggregate` | `agent-{agent_id}-{session_id}` | Owns a single agent's work session. Enforces Gas Town invariant (context must be declared before decisions) and model version consistency. |
| `ComplianceRecord` | `compliance-{application_id}` | Owns per-application compliance rule evaluations. Tracks which rules passed/failed independently, with `is_hard_block` determining whether DECLINE is forced. |
| `AuditLedger` | `audit-{entity_id}` | Cross-cutting integrity verification. Stores cryptographic hash chains that prove no events were tampered with after recording. |

**Why These Boundaries:**
Each aggregate protects a distinct invariant. `LoanApplicationAggregate` enforces the state machine (you cannot approve before compliance passes). `AgentSessionAggregate` enforces agent context integrity (you cannot produce a decision without first declaring which model and data you used). Separating them means a credit analysis agent's session can fail and retry without corrupting the loan application's state — they are different streams with different version counters.

**Rejected Alternative: Merging AgentSession into LoanApplication**
We considered tracking all agent actions directly in the `loan-{application_id}` stream. This would mean `AgentContextLoaded`, `AgentNodeExecuted`, `CreditAnalysisCompleted`, etc., would all be events in the loan stream.

**The coupling problem:** This creates a version contention hotspot. Five agents (document processing, credit analysis, fraud detection, compliance, decision orchestrator) would all be appending to the same stream. With OCC, only one can succeed at each version. Instead of 5 agents operating on 5 independent streams in parallel, they would serialize on a single lock. The retry rate would be proportional to the number of concurrent agents — unacceptable for production throughput. Separate `agent-{agent_id}-{session_id}` streams allow full parallelism, with the `LoanApplicationAggregate` only advancing its version when orchestration events (`CreditAnalysisRequested`, `DecisionGenerated`) are appended.

---

## Q3: Concurrency Trace

**Question:** Two AI agents simultaneously read a loan application stream at `expected_version=3` and each attempt to append `CreditAnalysisCompleted`. Trace the exact database operations each agent executes, and describe the loser's retry path.

### Answer

**Setup:** Stream `loan-APEX-0042` contains events at positions 0, 1, 2 (version = 2). Both agents read, build their analyses, and call `handle_credit_analysis_completed()` which calls `store.append()` with `expected_version=2`.

**Agent A (arrives first):**
```sql
-- 1. Acquire connection, begin transaction
BEGIN;

-- 2. Lock the stream row (this is the serialization point)
SELECT current_version FROM event_streams
WHERE stream_id = 'loan-APEX-0042' FOR UPDATE;
-- Returns: current_version = 2

-- 3. OCC check: expected(2) == actual(2) ✓ PASS

-- 4. Insert event
INSERT INTO events(event_id, stream_id, stream_position, event_type, event_version, payload, metadata)
VALUES('uuid-a', 'loan-APEX-0042', 3, 'CreditAnalysisCompleted', 2, '{"agent_id":"agent-A",...}', '{"correlation_id":"..."}');

-- 5. Insert outbox (same transaction)
INSERT INTO outbox(event_id, destination, payload)
VALUES('uuid-a', 'default', '{"event_type":"CreditAnalysisCompleted",...}');

-- 6. Update stream version
UPDATE event_streams SET current_version = 3 WHERE stream_id = 'loan-APEX-0042';

COMMIT;  -- ✅ SUCCESS — returns positions = [3]
```

**Agent B (arrives while A holds FOR UPDATE lock):**
```sql
BEGIN;

-- 1. Attempt to lock — BLOCKS here waiting for Agent A's transaction to complete
SELECT current_version FROM event_streams
WHERE stream_id = 'loan-APEX-0042' FOR UPDATE;
-- (Waits until Agent A commits...)
-- Returns: current_version = 3  (Agent A already updated it)

-- 2. OCC check: expected(2) != actual(3) ✗ FAIL
-- Python raises: OptimisticConcurrencyError(stream_id='loan-APEX-0042', expected=2, actual=3)

ROLLBACK;  -- ❌ FAIL — no data written
```

**The Loser's Retry Path:**
1. Agent B's command handler catches `OptimisticConcurrencyError`
2. Reload the aggregate: `app = await LoanApplicationAggregate.load(store, "APEX-0042")` — this replays all 4 events (0-3), rebuilding state including Agent A's `CreditAnalysisCompleted`
3. Guard method `app.assert_credit_analysis_not_completed()` now raises `DomainError("model_version_lock")` — because credit analysis is already done
4. Agent B's retry is correctly rejected: the business rule prevents duplicate credit analyses, not just OCC. The domain model protects the invariant even after the retry succeeds at the concurrency level.

---

## Q4: Projection Lag Handling

**Question:** A projection that computes remaining credit limits for an applicant is 200ms behind the event stream. An agent reads the projected credit limit and makes a decision based on the stale value. Describe the scenario, the risk, and your strategy for communicating this lag to the UI.

### Answer

**The Scenario:**
1. At T=0ms: `CreditAnalysisCompleted` is appended to `loan-APEX-0042` with `recommended_limit_usd: 500,000`. The `events` table now has this event at `global_position=1847`.
2. The ProjectionDaemon's `CreditLimitProjection` processes events from `projection_checkpoints.last_position`. Its checkpoint is at 1845 — it hasn't reached event 1847 yet.
3. At T=100ms: The DecisionOrchestratorAgent queries the projected credit limit view. It reads the stale value (e.g., $0 or a previous application's limit).
4. At T=200ms: The projection catches up, updates the read model to $500,000. But the orchestrator already made its decision based on stale data.

**The Risk:**
The orchestrator could approve an application with an incorrect credit limit reference, or decline one that should have been approved. This is an eventual consistency artifact — the projection is a read model, not the source of truth.

**Mitigation Strategy:**

1. **Command handlers read from aggregates, not projections.** The `handle_credit_analysis_completed()` handler loads `LoanApplicationAggregate` directly from `store.load_stream()` — this always returns the latest committed events, no lag. Projections are used for dashboards and reporting, not for command validation.

2. **Projection metadata includes lag indicator.** Each projected row carries:
   - `last_updated_at`: when the projection processor last wrote this row
   - `as_of_position`: the `global_position` of the last event processed
   The UI can compute `current_global_position - as_of_position` to display freshness.

3. **UI communication strategy:**
   - Display a subtle indicator: "Data as of 200ms ago" or a green/yellow/red freshness badge
   - For critical views (approval dashboards), add a "Refresh" button that bypasses the projection and queries the aggregate directly via `store.load_stream()`
   - Stale data warnings: if `as_of_position` is more than 5 seconds behind, show a banner: "Data may not reflect the latest changes"

---

## Q5: Upcasting Scenario

**Question:** The `CreditAnalysisCompleted` event originally had no `model_versions` field. In v2, this field is required. Write the upcaster code and justify your inference strategy for old events that didn't record which model was used.

### Answer

**The Problem:**
Events at `event_version=1` were written when the credit analysis agent didn't track which LLM/ML model produced the analysis. Now v2 requires `model_versions: dict[str, str]` mapping model roles to version strings. Old events need this field when loaded, but we **never mutate stored events** — upcasting transforms in memory only.

**The Upcaster Code:**

```python
# ledger/upcasters.py

from ledger.event_store import UpcasterRegistry

registry = UpcasterRegistry()

@registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
def upcast_credit_analysis_v1_to_v2(payload: dict) -> dict:
    """
    v1 → v2: Add model_versions field.

    Inference strategy:
      - If model_version exists (single string), promote it to the dict format
      - If model_version is absent, infer from deployment timeline:
        Events before 2026-02-01 used "gpt-4-turbo-2024-04-09"
        Events after used "claude-3-5-sonnet-20241022"
      - Always mark as inferred so downstream consumers know it's not authoritative
    """
    model_versions = {}

    if "model_version" in payload:
        # v1 had a single model_version string — promote to dict
        model_versions["credit_analysis"] = payload["model_version"]
    else:
        # No model info at all — use sentinel indicating inference
        model_versions["credit_analysis"] = "unknown-pre-v2"

    # Always add the inference marker
    model_versions["_inferred"] = "true"

    payload["model_versions"] = model_versions
    return payload
```

**Inference Justification:**
- **Why not leave it empty?** Downstream projections and the DecisionOrchestratorAgent need `model_versions` to populate the audit trail. An empty dict would cause `KeyError` crashes in code that assumes v2 schema.
- **Why `"unknown-pre-v2"` not a guess?** We could infer from deployment logs, but that couples the upcaster to operational metadata outside the event store. Using a sentinel value is honest: it says "this event predates model tracking" rather than fabricating provenance.
- **The `_inferred: "true"` marker** allows downstream consumers to distinguish between authoritative model attribution (from v2 events) and inferred attribution (from upcasted v1 events). This is critical for audit compliance — regulators need to know which model decisions are attributable and which are reconstructed.

---

## Q6: Marten Async Daemon Parallel

**Question:** Describe how the Marten Async Daemon design achieves parallel projection execution. How would you implement the equivalent coordination primitive in Python, and what failure mode must you handle?

### Answer

**Marten's Async Daemon Design:**
Marten's daemon runs multiple projections concurrently, each with its own checkpoint. The key design elements are:

1. **Per-projection checkpointing:** Each projection tracks its own `last_position` in the `projection_checkpoints` table. Projections advance independently — a slow compliance aggregation doesn't block the fast application status projection.
2. **Shared event feed, independent cursors:** All projections read from the same `events` table (via `global_position`), but each has its own cursor position. The daemon fetches batches from the shared table and dispatches to each projection.
3. **Distributed Coordination (Mastery):** In a multi-node environment, Marten uses a **Global Advisory Lock** (PostgreSQL `pg_advisory_lock`) or a dedicated "Leader Election" record in the database. Only one node "owns" a specific projection at a time. If Node A crashes, the lock is released, and Node B "claims" the projection, resuming from the last checkpoint.

**Python Implementation:**

```python
import asyncio
from ledger.event_store import EventStore

class ProjectionDaemon:
    def __init__(self, store: EventStore, projections: list):
        self.store = store
        self.projections = projections
        self._running = False

    async def run(self, poll_interval: float = 0.5):
        self._running = True
        while self._running:
            # 1. ATTEMPT TO CLAIM LEADERSHIP (Postgres Advisory Lock)
            # await self.store.acquire_advisory_lock(PROJECTION_LOCK_ID)
            
            # 2. RUN PROJECTIONS IN PARALLEL
            tasks = [
                asyncio.create_task(self._process_projection(proj))
                for proj in self.projections
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(poll_interval)
```

**The Critical Failure Mode: Poison Events**

A _poison event_ is an event that consistently crashes a projection handler (e.g., malformed payload). Without handling, this creates an infinite retry loop:

1. Projection reads event at position N → crashes
2. Checkpoint stays at N-1
3. Next poll: projection reads event N again → crashes again
4. **The projection is permanently stuck (The "Stuck Cursor" mode).**

**Mitigation:**
The daemon must implement a **Retry Budget** and a **Dead Letter Queue (DLQ)** pattern:
1. Attempt processing 3 times.
2. On 3rd failure, log the exception and the exact `global_position`.
3. **SKIP** the event by advancing the checkpoint to N+1.
4. Alert the operator. This ensures the 200ms lag SLO is maintained for all other data, even if one event is "poison".
