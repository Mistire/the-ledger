# The Ledger

**Agentic Event Store & Enterprise Audit Infrastructure**

An append-only event sourcing system for Apex Financial Services' multi-agent AI platform. Provides immutable audit trails for commercial loan application processing with optimistic concurrency control, CQRS projections, and cryptographic integrity verification.

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 16+

### 1. Install Dependencies
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install core + dev dependencies
pip install -r requirements.txt
pip install pytest pytest-asyncio faker
```

### 2. Start PostgreSQL
```bash
docker run -d \
  -e POSTGRES_PASSWORD=apex \
  -e POSTGRES_DB=apex_ledger \
  -p 5432:5432 \
  --name apex-postgres \
  postgres:16
```

### 3. Run Migrations
```bash
psql -h localhost -U postgres -d apex_ledger -f schema.sql
```

### 4. Configure Environment
```bash
cp .env.example .env
# Edit .env with your API keys
```

### 5. Generate Seed Data
```bash
# Validate schema (no DB needed)
python datagen/generate_all.py --skip-db --skip-docs --validate-only

# Generate all data (requires PostgreSQL)
python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/apex_ledger
```

### 6. Run Tests
```bash
# Phase 0 (schema validation — must pass first)
pytest tests/test_schema_and_generator.py -v

# Phase 1 (EventStore core — InMemoryEventStore)
pytest tests/phase1/test_event_store.py -v

# Concurrency tests (OCC double-decision)
pytest tests/test_concurrency.py -v

# All tests
pytest tests/ -v
```

## Project Structure

```
the-ledger/
├── ledger/                         # Core application package
│   ├── event_store.py              # EventStore (6 async methods) + InMemoryEventStore
│   ├── domain/
│   │   └── aggregates/
│   │       ├── loan_application.py # LoanApplication aggregate (state machine)
│   │       └── agent_session.py    # AgentSession aggregate (Gas Town pattern)
│   ├── commands/
│   │   └── handlers.py            # Command handlers (4-step pattern)
│   ├── schema/
│   │   └── events.py              # 45 Pydantic event types + EVENT_REGISTRY
│   ├── agents/                    # AI agent framework (Phase 2-3)
│   ├── projections/               # CQRS projections (Phase 4)
│   ├── registry/                  # Applicant registry client
│   └── upcasters.py              # Schema evolution (Phase 4)
├── tests/
│   ├── phase1/test_event_store.py # Phase 1 gate tests (11 tests)
│   ├── test_concurrency.py        # OCC double-decision + suite (11 tests)
│   └── test_schema_and_generator.py
├── datagen/                       # Seed data generators
├── data/seed_events.jsonl        # 1,200+ pre-generated events
├── schema.sql                     # PostgreSQL DDL (4 tables)
├── DOMAIN_NOTES.md               # Domain analysis answers
├── pyproject.toml
└── README.md
```

## Architecture

### Event Store Core (Phase 1)
- **Append-only PostgreSQL store** with `SELECT ... FOR UPDATE` for DB-level OCC
- **Transactional outbox** — events + outbox written atomically in single transaction
- **6 async methods**: `stream_version`, `append`, `load_stream`, `load_all`, `get_event`, `get_stream_metadata`
- **UpcasterRegistry** for schema evolution without mutating stored events

### Domain Logic (Phase 2)
- **LoanApplicationAggregate** — 7+ lifecycle states, per-event dispatch handlers, business rule guards
- **AgentSessionAggregate** — Gas Town persistent ledger pattern, model version enforcement
- **Command handlers** — load → validate → determine → append with multi-aggregate support

### Database Schema
| Table | Purpose |
|-------|---------|
| `events` | Immutable event log with identity-based global ordering |
| `event_streams` | Stream metadata + version for OCC |
| `projection_checkpoints` | Async daemon position tracking |
| `outbox` | Guaranteed event delivery via transactional outbox |

### Aggregate Streams
| Aggregate | Stream ID | Purpose |
|-----------|-----------|---------|
| LoanApplication | `loan-{id}` | Full loan lifecycle |
| AgentSession | `agent-{agent_id}-{session_id}` | AI agent work sessions |
| ComplianceRecord | `compliance-{id}` | Regulatory checks |
| AuditLedger | `audit-{type}-{id}` | Cryptographic integrity |

## Business Rules (Enforced in Domain)
1. **State machine** — only valid transitions allowed (DomainError on violation)
2. **Gas Town pattern** — agents must declare context before decisions
3. **Model version locking** — no duplicate credit analyses
4. **Confidence floor** — score < 0.6 forces REFER recommendation
5. **Compliance dependency** — approval requires all checks passed
6. **Causal chain** — decisions must reference contributing sessions
