# Core Surface Boundary

The core surface is the set of modules that depend only on **stdlib + pydantic** -- no sqlalchemy, redis, or other infrastructure libraries. These modules define the contracts (protocols, ABCs, base classes, types) that the rest of the library implements. They are candidates for future extraction into a standalone `eventsource-core` package (Tier 0) that downstream libraries can depend on without pulling in database drivers.

## Why this matters

- **Lighter dependency tree**: Consumers who only need the event/aggregate contracts (e.g., shared domain libraries) should not need sqlalchemy.
- **Cleaner layering**: Making the boundary explicit prevents accidental infrastructure leakage into core contracts.
- **Future extraction**: When the time comes to split the package, this document defines exactly what moves.

## Tier 0 modules (no sqlalchemy/redis imports)

| Module | Dependencies (beyond stdlib) | Purpose |
|--------|------------------------------|---------|
| `events/base.py` | pydantic | `DomainEvent` base class |
| `events/registry.py` | (none beyond events/base) | `EventRegistry` auto-registration |
| `aggregates/base.py` | pydantic (via events, types) | `AggregateRoot`, `DeclarativeAggregate` |
| `protocols.py` | pydantic (via events/base) | `EventHandler`, `SyncEventHandler`, `EventSubscriber` protocols/ABCs |
| `stores/interface.py` | (none beyond events/base) | `EventStore` ABC, `StoredEvent`, `EventStream` |
| `bus/interface.py` | (none beyond events/base) | `EventBus` ABC |
| `snapshots/interface.py` | pydantic (via events/base) | `SnapshotStore` ABC |
| `snapshots/strategies.py` | (none beyond snapshots/interface) | Snapshot strategy definitions |
| `handlers/decorators.py` | pydantic (via events/base) | `@handles` decorator |
| `handlers/registry.py` | (none beyond handlers/decorators, events, exceptions) | `HandlerRegistry` |
| `handlers/adapter.py` | (none beyond protocols, events) | Sync/async handler adapter |
| `exceptions.py` | (none) | All exception types |
| `types.py` | pydantic | `AggregateId`, `TState`, `Version`, etc. |
| `serialization/` | (none) | `EventSourceJSONEncoder`, `json_dumps` |
| `observability/` | (none -- opentelemetry is optional, guarded) | `Tracer`, attribute constants |
| `stores/in_memory.py` | (none beyond stores/interface) | `InMemoryEventStore` |
| `bus/memory.py` | (none beyond bus/interface) | `InMemoryEventBus` |
| `snapshots/in_memory.py` | (none beyond snapshots/interface) | `InMemorySnapshotStore` |

## Modules NOT in Tier 0 (require sqlalchemy or redis)

| Module | Infrastructure dep | Reason |
|--------|-------------------|--------|
| `stores/postgresql.py` | sqlalchemy, asyncpg | PostgreSQL event store implementation |
| `stores/sqlite.py` | sqlalchemy (aiosqlite) | SQLite event store implementation |
| `snapshots/postgresql.py` | sqlalchemy, asyncpg | PostgreSQL snapshot store |
| `snapshots/sqlite.py` | sqlalchemy (aiosqlite) | SQLite snapshot store |
| `bus/redis.py` | redis | Redis event bus |
| `bus/kafka.py` | aiokafka | Kafka event bus |
| `bus/rabbitmq.py` | aio-pika | RabbitMQ event bus |
| `repositories/checkpoint.py` | sqlalchemy | Checkpoint repository (includes InMemory variant) |
| `repositories/dlq.py` | sqlalchemy | DLQ repository (includes InMemory variant) |
| `projections/base.py` | sqlalchemy (transitive via repositories) | Projection base classes |
| `subscriptions/` | sqlalchemy (transitive via stores, repositories) | Subscription lifecycle management |
| `migration/` | sqlalchemy | Live migration tooling |
| `locks/` | sqlalchemy | Distributed locking |
| `multitenancy/` | (check -- may be Tier 0 candidate) | Tenant context |

## Boundary findings

1. **`projections/base.py` is NOT Tier 0.** It imports `repositories/checkpoint.py` and `repositories/dlq.py`, both of which import sqlalchemy at module level. The projection *protocols* (`projections/protocols.py`) are clean -- they only re-export from `protocols.py`.

2. **`repositories/checkpoint.py` and `repositories/dlq.py` mix interface and implementation.** Both files define their Protocol/ABC, dataclass, in-memory implementation, AND sqlalchemy implementation in the same file. This means importing `InMemoryCheckpointRepository` forces a sqlalchemy import. Splitting interface from implementation would make the in-memory variants Tier 0 eligible.

3. **`testing/` is almost Tier 0** but is blocked by the repository files above. `testing/harness.py` imports `InMemoryCheckpointRepository` and `InMemoryDLQRepository`, which transitively pull in sqlalchemy. If the repositories are split, `testing/` becomes fully Tier 0.

4. **`observability/` is Tier 0.** It guards opentelemetry behind optional imports with no-op fallbacks. No sqlalchemy or redis.

## Boundary rules for Tier 0

1. **Allowed dependencies**: stdlib, pydantic, typing-extensions.
2. **No sqlalchemy imports** -- not even behind `TYPE_CHECKING`. Tier 0 modules must be importable without sqlalchemy installed.
3. **Optional deps must be guarded**: opentelemetry is acceptable if behind `try/except ImportError` with no-op fallback.
4. **In-memory implementations belong in Tier 0**: They implement the interface contracts using only stdlib. They are essential for testing without infrastructure.
5. **Interface + implementation separation**: Files that define both a Protocol/ABC and a sqlalchemy-backed implementation must be split before the interface can move to Tier 0.

## Recommended pre-extraction cleanup

Before extracting Tier 0, these changes would clean up the boundary:

1. **Split `repositories/checkpoint.py`** into `checkpoint/interface.py` (Protocol + dataclass + InMemory) and `checkpoint/postgresql.py` (sqlalchemy impl).
2. **Split `repositories/dlq.py`** similarly.
3. After the split, `projections/base.py` can import only the interface, and `testing/harness.py` can import only in-memory variants -- both become Tier 0 eligible.
