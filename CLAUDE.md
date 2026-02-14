# CLAUDE.md

Production-ready event sourcing library for Python (pydantic, sqlalchemy, async-first).

## Operating Mode: Orchestrator

**The primary Claude Code session operates as an orchestrator only.** Do not directly implement tasks -- dispatch work to specialized subagents and manage the beads backlog.

### Orchestrator Responsibilities

1. **Backlog Management**: Use `bd` commands to triage, prioritize, and track issues
2. **Task Dispatch**: Delegate implementation work to appropriate subagents via the Task tool
3. **Coordination**: Manage dependencies between tasks, unblock work, review agent outputs
4. **Session Management**: Run `bd sync --flush-only` before completing sessions

### Serialized Dispatching

**Dispatch tasks one at a time, not in parallel.** Wait for completion, review, then dispatch next.

---

## Quick Reference

```bash
# Install
uv sync --all-extras

# Unit tests (no Docker needed)
uv run pytest tests/unit/ -v

# Integration tests (requires Docker services)
docker-compose -f docker-compose.test.yml up -d
uv run pytest tests/integration/ -v

# Specific backend tests
uv run pytest tests/ -m postgres
uv run pytest tests/ -m sqlite
uv run pytest tests/ -m redis

# Lint and format
uv run ruff check src/ tests/ --fix
uv run ruff format src/ tests/

# Type check
uv run mypy src/eventsource/ --config-file=pyproject.toml

# Full pre-commit
pre-commit run --all-files
```

## Project Structure

```
src/eventsource/
  aggregates/       # AggregateRoot, DeclarativeAggregate, AggregateRepository
  bus/              # EventBus interface + InMemory, Redis, RabbitMQ, Kafka backends (implementations colocated)
  events/           # DomainEvent (pydantic BaseModel), EventRegistry
  handlers/         # @handles decorator for declarative event routing
  locks/            # Distributed locking (PostgreSQL advisory locks)
  migration/        # Live event store migration tooling (dual-write, cutover, sync tracking)
  migrations/       # SQL schema files (append-only)
  multitenancy/     # Tenant context (contextvars), scopes, TenantDomainEvent
  observability/    # OpenTelemetry tracing integration (optional dep)
  projections/      # Projection, DeclarativeProjection, DatabaseProjection
  readmodels/       # ReadModelProjection
  repositories/     # Checkpoint, DLQ, Outbox repos (postgres, sqlite, memory backends)
  serialization/    # JSON encoding (EventSourceJSONEncoder)
  snapshots/        # Snapshot store interface + InMemory, PostgreSQL, SQLite implementations
  stores/           # EventStore interface + PostgreSQL, SQLite, InMemory implementations
  subscriptions/    # Subscription lifecycle: manager, runners, retry, health, flow control
  sync/             # SyncEventStoreAdapter (wraps async store for sync callers)
  testing/          # Test helpers: assertions, BDD, builder, harness
  gdpr/             # GDPR compliance utilities
  _internal/        # Internal helpers (not public API)
  config.py         # Configuration utilities
  protocols.py      # Canonical type contracts (Protocols + ABCs, see note below)
  exceptions.py     # All exception types
  types.py          # Type aliases (AggregateId, EventId, TenantId, etc.)
```

## Architecture

- **Async-first**: All store/bus/projection interfaces are async. `SyncEventStoreAdapter` wraps async for sync callers.
- **Pydantic v2**: DomainEvent is a Pydantic BaseModel. Event data validated/serialized via pydantic. `model_config = ConfigDict(frozen=True)`.
- **Mixed Protocols + ABCs**: `protocols.py` has both Python Protocols (EventHandler, SyncEventHandler, FlexibleEventHandler) and ABCs (EventSubscriber, AsyncEventHandler). Protocols enable structural subtyping; ABCs are used where additional methods are needed.
- **Backend-agnostic**: EventStore, EventBus, repositories all have multiple backend implementations behind shared interfaces defined in `interface.py` or `base.py` files.
- **Optimistic locking**: Aggregates use `expected_version` for concurrency control via `OptimisticLockError`.

## Key Patterns

- Events auto-register via `DomainEvent.__init_subclass__` into the global `EventRegistry`
- `@handles(EventType)` decorator maps events to handler methods on `DeclarativeAggregate` / `DeclarativeProjection`
- Optional deps guarded by `try/except ImportError` with `*_AVAILABLE` boolean flags (e.g., `KAFKA_AVAILABLE`, `SQLITE_AVAILABLE`)
- Public API re-exported from top-level `__init__.py` -- all user-facing imports come from `eventsource`
- Core deps: pydantic, sqlalchemy. Optional: redis, asyncpg, aiosqlite, aio-pika, aiokafka, opentelemetry

## Key Conventions (details in .claude/rules/)

- See `.claude/rules/architecture.md` for layer boundaries and interface patterns
- See `.claude/rules/testing.md` for test structure, markers, and async conventions
- See `.claude/rules/definition-of-done.md` for feature/bugfix/refactor checklists
- See `.claude/rules/commits.md` for commit message format

## Do Not Modify

- `py.typed` marker file
- `migrations/` SQL schema files (append-only by design)
- Public API exports in `__init__.py` without considering backward compatibility
