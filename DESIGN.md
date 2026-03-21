# DESIGN.md — The Ledger

## 1. Aggregate Boundary Justification
The system partitions state into `LoanApplication`, `AgentSession`, `ComplianceRecord`, and `AuditLedger`.
- **Reasoning:** `LoanApplication` reflects the business lifecycle. `AgentSession` reflects the ephemeral work-memory of an AI.
- **Tradeoff:** Merging them would create a single, massive stream for each loan. Every agent action (log, reasoning, context load) would compete for a lock on the loan application stream. By separating them, multiple agents can work on the same loan application in parallel (writing to their own `AgentSession` streams) without causing `OptimisticConcurrencyError` on the main loan stream. The `DecisionOrchestratorAgent` then synthesizes these sessions into a single `DecisionGenerated` event on the loan stream.

## 2. Projection Strategy
- **ApplicationSummary:** Stored in a PostgreSQL table, updated **In-line** for local dev/Phase 2, but planned as **Async** (via `ProjectionDaemon`) for production.
- **ComplianceAuditView:** **Async** projection. Due to the high payload size of audit evidence, async processing prevents write-latency spikes.
- **Temporal Query Snapshot Strategy:** We use a **Point-in-Time Replay** strategy for compliance audits. Snapshots are triggered every 100 events. If a regulator asks for state at `T`, we load the nearest snapshot before `T` and replay events up to `T`.

## 3. Concurrency Analysis
- **Peak Load Estimates:** 100 concurrent apps x 4 agents = 400 events/batch. 
- **Expected Errors:** ~5% retry rate on the `LoanApplication` stream due to orchestrator collisions.
- **Retry Strategy:** Exponential backoff with a jitter. Maximum retry budget: 3 attempts. After 3 failures, the agent flags the session as `NEEDS_RECONCILIATION` and returns an error to the orchestrator.

## 4. Upcasting Inference Decisions
- **CreditAnalysisCompleted v1→v2:** Inferred `model_version` based on `recorded_at`.
- **DecisionGenerated v1→v2:** Inferred `contributing_sessions` by querying the store for `AgentSession` events matching the same `correlation_id`.
- **Error Rate & Consequences:** Inference errors on `model_version` are low-risk for historically processed applications. However, we choose **NULL** over inference for `confidence_score` if no data exists, as fabricating confidence scores is a severe regulatory risk.

## 5. EventStoreDB Comparison
- **PostgreSQL streams** map to EventStoreDB `StreamID`.
- Our `load_all()` async generator maps to EventStoreDB `$all` stream subscription.
- Our `ProjectionDaemon` maps to EventStoreDB **Persistent Subscriptions**.
- **What EventStoreDB provides:** Native gRPC streaming, built-in projection secondary storage, and optimized scavenging (deletion of old events) which we must implement manually in PostgreSQL via `archived_at` logic.

## 6. Reflections & Future Work
If I had another full day, I would implement **Inline Projections** as a secondary fallback for high-consistency requirements. Currently, our system leans heavily toward the "Async Daemon" (eventual consistency) model, which is superior for throughput but requires sophisticated UI handling for lag.
