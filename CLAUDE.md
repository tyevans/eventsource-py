# CLAUDE.md

Production-ready event sourcing library for Python (pydantic, sqlalchemy, async-first).

## Operating Mode: Orchestrator

**The primary Claude Code session operates as an orchestrator only.** Do not directly implement tasks -- dispatch work to specialized subagents and manage the backlog in `BACKLOG.md`.

### Orchestrator Responsibilities

1. **Backlog Management**: Triage, prioritize, and track work in `BACKLOG.md`
2. **Task Dispatch**: Delegate implementation work to appropriate subagents via the Task tool
3. **Coordination**: Manage dependencies between tasks, unblock work, review agent outputs

### Serialized Dispatching

**Dispatch tasks one at a time, not in parallel.** Wait for completion, review, then dispatch next.

---

## Quick Reference

Every command below also has a `make` target -- run `make help` for the list.
`make check` runs lint, mypy, import-linter, bandit/pip-audit and the unit
suite in one go, and is CI parity: CI installs the same locked environment
and runs the same commands, so green locally means green in CI.

**Running tests: use `make test`** (full unit suite) or `make test-changed`
(same suite, skipped when nothing under `src/` changed since the last green
run). Use raw `uv run pytest <path>` only for targeted single-test runs.

```bash
# Install
uv sync --all-extras

# Unit tests (no Docker needed)
make test              # full unit suite with coverage
make test-changed      # same, but skipped if src/ is unchanged since last green run
uv run pytest tests/unit/path/to/test_x.py -v   # targeted runs only

# Integration tests (requires Docker services)
docker compose -f docker-compose.test.yml up -d   # note: `docker compose`, not `docker-compose`
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
  domain/           # Entities ring: AggregateRoot, DeclarativeAggregate, StreamId (pure: stdlib + pydantic only);
                    #   exceptions.py (all exception types, incl. SnapshotError hierarchy), types.py (type
                    #   aliases: AggregateId, EventId, TenantId, etc.), command.py (DomainCommand)
  application/      # Use-case ring: AggregateRepository, snapshot policy/scheduler collaborators;
                    #   application/projections/: Projection, DeclarativeProjection, coordinator,
                    #   checkpoint/DLQ functions, retry policies (DatabaseProjection is an adapter, not here);
                    #   application/subscriptions/: subscription lifecycle -- manager, runners, retry,
                    #   health, flow control, coordination messages/WorkRedistributionCoordinator
  ports/            # Boundary interfaces: Snapshot/SnapshotStore, ProjectionCheckpoints/SubscriptionPositions/
                    #   CheckpointRepository, DLQRepository, OutboxRepository/outbox_event_data,
                    #   store/envelope/position ports, bus.py (EventBus, EventPublisher, EventHandlerFunc,
                    #   SubscribableEventBus), handlers.py (canonical handler/subscriber Protocols + ABCs,
                    #   see note below), subscribers.py (Subscriber/SyncSubscriber/BatchSubscriber
                    #   Protocols), coordination.py (LeaderElector Protocols)
  adapters/         # Interface adapters: memory/postgresql/sqlite snapshot + event store implementations;
                    #   adapters/sql/: dialect-parameterized checkpoint, DLQ, and DatabaseProjection (both
                    #   PostgreSQL and SQLite); adapters/memory/: in-memory checkpoint, DLQ, outbox, and
                    #   coordination (InMemoryLeaderElector, SharedLeaderState) repositories;
                    #   adapters/postgresql/ and adapters/sqlite/: per-technology outbox
                    #   repositories (not dialect-parameterized -- SQLite takes a raw aiosqlite.Connection);
                    #   adapters/sync/ (SyncEventStoreAdapter, wraps a FullEventStore for sync callers);
                    #   adapters/serialization/ (JSON encoding, EventSourceJSONEncoder); event bus backends --
                    #   adapters/memory/bus.py, adapters/redis/, adapters/kafka/, adapters/rabbitmq/ (InMemory/
                    #   Redis/Kafka/RabbitMQ EventBus), with shared collaborators (BaseEventBus,
                    #   SubscriptionRegistry) in adapters-internal adapters/_bus/
  events/           # DomainEvent (pydantic BaseModel), EventRegistry
  handlers/         # @handles decorator for declarative event routing
  migration/        # Live event store migration tooling (dual-write, cutover, sync tracking)
  migrations/       # SQL schema files (append-only)
  multitenancy/     # Tenant context (contextvars), scopes, TenantDomainEvent
  observability/    # OpenTelemetry tracing integration (optional dep)
  testing/          # Test helpers: assertions, BDD, builder, harness;
                    #   testing/conformance_ports/: backend conformance suites for the store/snapshot/
                    #   checkpoint/DLQ ports (EventStoreConformanceSuite's replacement)
  gdpr/             # GDPR compliance utilities
  _internal/        # Internal helpers (not public API)
```

`types.py`, `exceptions.py`, `protocols.py`, `commands/`, `sync/`, `serialization/`,
`locks/`, `readmodels/`, and `config.py` no longer exist at the top level (ADR 0030),
and `bus/` no longer exists at the top level (ADR 0031) -- no shims, clean breaks.
See `domain/`, `ports/`, and `adapters/` above for their homes.

## Architecture

- **Async-first**: All store/bus/projection interfaces are async. `SyncEventStoreAdapter` wraps async for sync callers.
- **Pydantic v2**: DomainEvent is a Pydantic BaseModel. Event data validated/serialized via pydantic. `model_config = ConfigDict(frozen=True)`.
- **Mixed Protocols + ABCs**: `ports/handlers.py` has both Python Protocols (EventHandler, SyncEventHandler, FlexibleEventHandler) and ABCs (EventSubscriber, AsyncEventHandler). Protocols enable structural subtyping; ABCs are used where additional methods are needed.
- **Backend-agnostic**: EventStore, EventBus, repositories all have multiple backend implementations behind shared interfaces defined in `ports/`.
- **Optimistic locking**: Aggregates use `expected_version` for concurrency control via `OptimisticLockError`.

## Key Patterns

- `DomainEvent.__init_subclass__` auto-derives `event_type` from the class name; registry registration is explicit via `@register_event`
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
