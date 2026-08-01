# Core Surface Boundary

The core surface is the set of modules that depend only on **stdlib + pydantic** -- no sqlalchemy, redis, or other infrastructure libraries. These modules define the contracts (protocols, ABCs, base classes, types) that the rest of the library implements. They are candidates for future extraction into a standalone `eventsource-core` package (Tier 0) that downstream libraries can depend on without pulling in database drivers.

## Why this matters

- **Lighter dependency tree**: Consumers who only need the event/aggregate contracts (e.g., shared domain libraries) should not need sqlalchemy.
- **Cleaner layering**: Making the boundary explicit prevents accidental infrastructure leakage into core contracts.
- **Future extraction**: When the time comes to split the package, this document defines exactly what moves.

## What a Tier 0 module looks like

> **Correction (2026-07-28):** this section previously used `serialization/`
> as the reference case for the strictest form of Tier 0 -- "imports cleanly
> with nothing but the standard library present." That stopped being true
> the moment `orjson` became a core dependency of `serialization/json.py`
> (a user decision: performance over Tier 0 purity for that module). orjson
> is a compiled third-party extension, not stdlib and not pydantic, so
> `serialization/` no longer qualifies as Tier 0 under this document's own
> definition -- see its new entry in "Modules NOT in Tier 0" below.
> `exceptions.py` replaces it as the reference case; the rest of this
> section is otherwise unchanged in substance.

`exceptions.py` is the cleanest example in the codebase, and the easiest one to reason about.

Its entire import block, at `src/eventsource/exceptions.py`, is:

```python
from uuid import UUID
```

That is the whole dependency surface. No pydantic. No sqlalchemy. Not even a transitive `eventsource` import -- `exceptions.py` never reaches back into the rest of the library, so there is no chain of internal modules that could quietly drag infrastructure in behind it.

The module is just as small: one file, defining `EventSourceError` and its subclasses. Everything you need to understand it fits in one screen, which is what makes it a useful yardstick: if a candidate Tier 0 module is harder to audit than this, the boundary is doing work you should look at closely.

The practical test for extraction-readiness follows directly from that import block. In a bare virtualenv with no third-party packages installed, this succeeds:

```
python -c 'import eventsource.exceptions'
```

Any module that passes that test -- imports cleanly with nothing but the standard library present -- is unambiguously extractable. Most Tier 0 modules will not pass it, because pydantic is an allowed Tier 0 dependency (see the rules below); they pass the weaker "stdlib + pydantic only" version of the same check. `exceptions.py` passing the strict form is what makes it the reference case.

## Tier 0 modules

| Module | Dependencies (beyond stdlib) | Purpose |
|--------|------------------------------|---------|
| `domain/event.py` | pydantic | `DomainEvent` base class |
| `domain/event_registry.py` | (none beyond domain/event) | `EventRegistry` auto-registration |
| `domain/aggregate.py` | pydantic (via domain/event, types) | `AggregateRoot`, `DeclarativeAggregate` |
| `domain/stream_id.py` | (none -- stdlib only) | `StreamId` value object |
| `protocols.py` | `from eventsource.domain.event import DomainEvent` (module level, so pydantic transitively) -- not standalone | `EventHandler`, `SyncEventHandler`, `EventSubscriber` protocols/ABCs |
| `bus/interface.py` | (none beyond domain/event, protocols) | `EventBus` ABC |
| `ports/snapshots.py` | (none -- stdlib only) | `SnapshotStore` ABC, `Snapshot` dataclass |
| `ports/positions.py` | (none beyond exceptions) | `Position`, `ExpectedVersion` value objects |
| `ports/envelopes.py` | pydantic (via domain, events) | `EventEnvelope`, read-option value objects, `AppendResult`, `ReadDirection` |
| `ports/store.py` | (none beyond domain, events, ports siblings) | `EventAppender`, `StreamReader`, `CategoryQuery`, `EventLookup`, `GlobalEventFeed`, `FullEventStore` port Protocols |
| `ports/bus.py` | (none beyond events/base) | `EventPublisher` publishing port |
| `ports/checkpoints.py` | (none -- stdlib only) | `ProjectionCheckpoints`, `SubscriptionPositions`, composed `CheckpointRepository` Protocols; `CheckpointData`, `LagMetrics` dataclasses. Replaced the Protocol/dataclass half of the old `repositories/checkpoint.py` (see boundary finding 2 and ADR 0024). |
| `ports/dlq.py` | (none -- stdlib only) | `DLQRepository` Protocol; `DLQEntry`, `DLQStats`, `ProjectionFailureCount` dataclasses. Replaced the Protocol/dataclass half of the old `repositories/dlq.py` (see boundary finding 2 and ADR 0024). |
| `application/projections/base.py`, `checkpoints.py`, `dlq.py`, `coordinator.py`, `retry.py` | (none beyond `ports/checkpoints`, `ports/dlq`, `events/base`, `handlers/registry`, `observability`, `protocols`, `application/subscriptions/retry`) | Replaces the old `projections/` package (dissolved by ADR 0024). `Projection`, `EventHandlerBase`, `DeclarativeProjection`, checkpoint/DLQ recording helpers, `RetryConfig`. This is the module the rest of this document now describes as Tier-0-clean where `projections/base.py`, `projections/checkpoint_manager.py`, and `projections/dlq_manager.py` used to be blocked (see boundary finding 1). |
| `application/aggregates/snapshotting.py` | `_internal/background_tasks`, `domain/aggregate`, `ports/snapshots` | `SnapshotPolicy`, `SnapshotScheduler`, `EveryNEvents`, `Never`, `ImmediateScheduler`, `BackgroundScheduler`, `take_snapshot()`, `read_valid_snapshot()` |
| `handlers/decorators.py` | pydantic (via events/base) | `@handles` decorator |
| `handlers/registry.py` | (none beyond handlers/decorators, events, exceptions) | `HandlerRegistry` |
| `handlers/adapter.py` | (none beyond protocols, events) | Sync/async handler adapter |
| `exceptions.py` | (none) | All exception types |
| `config.py` | (none -- empty placeholder module, defines zero configuration classes) | Nothing yet; see "Empty and placeholder modules" below |
| `types.py` | pydantic | `AggregateId`, `TState`, `Version`, etc. |
| `observability/` | (none -- opentelemetry is optional, guarded) | `Tracer`, attribute constants |
| `bus/memory.py` | (none beyond bus/interface, bus/base, events, handlers/adapter, observability) | `InMemoryEventBus` |
| `adapters/memory/store.py` | (none beyond domain, events, exceptions, ports) | `InMemoryEventStore` (ring in-memory store) |
| `adapters/memory/snapshots.py` | `observability`, `ports/snapshots` | `InMemorySnapshotStore` |
| `adapters/memory/checkpoints.py` | `observability`, `ports/checkpoints` | `InMemoryCheckpointRepository` -- the in-memory half of the old `repositories/checkpoint.py` split (ADR 0024). Tier 0 only when imported by its own submodule path (`eventsource.adapters.memory.checkpoints`); importing the `eventsource.adapters.memory` package pulls its sibling `dlq.py`, which is not Tier 0 -- see the next table and boundary finding 2. |
| `adapters/_sql/positions.py` | (none beyond exceptions, ports/positions) | `IntPositionCodec` |
| `testing/builder.py` | pydantic (via events/base) | `EventBuilder` fluent test-event builder |
| `testing/assertions.py` | pydantic (via events/base) | `EventAssertions` domain-specific test assertions |
| `testing/conformance.py` | (none beyond events/base, bus/interface) | `EventBusConformanceSuite`; store conformance now lives in `testing/conformance_ports/` (see below) |
| `testing/recording.py` | (none beyond bus/interface, events/base, protocols) | `RecordingEventBus` |
| `testing/partitioned_memory.py` | (none beyond domain, events, exceptions, ports) | Partitioned in-memory store for feed/ordering tests |
| `testing/sync_facade.py` | (none beyond domain, events, ports) | `SyncStoreFacade` synchronous wrapper over the ports surface |
| `application/aggregates/repository.py` | `application/aggregates/snapshotting`, `domain/aggregate`, `exceptions`, `observability`, `ports/bus`, `ports/envelopes`, `ports/positions`, `ports/store` (`ports/snapshots` types imported under `TYPE_CHECKING`) | `AggregateRepository` load/save orchestration, composing a `SnapshotPolicy` and `SnapshotScheduler` |
| `sync/adapter.py` | stdlib `threading` + `concurrent.futures.ThreadPoolExecutor`, plus `domain`, `events/base`, and `ports` | `SyncEventStoreAdapter` wrapping an async store for sync callers |
| `multitenancy/context.py` | stdlib `contextvars` only, plus `multitenancy/exceptions` | Tenant context get/set, `get_required_tenant` |
| `multitenancy/events.py` | pydantic (`Field`), plus `events/base` and `multitenancy/context` | `TenantDomainEvent` |
| `multitenancy/exceptions.py` | (none beyond `eventsource.exceptions`) | `TenantContextNotSetError`, `TenantMismatchError` |
| `multitenancy/repository.py` | (none beyond application/aggregates/repository, domain/aggregate, multitenancy context + exceptions) | Tenant-scoped aggregate repository |
| `readmodels/base.py` | pydantic (`BaseModel`, `ConfigDict`, `Field`) | `ReadModel` base class |
| `readmodels/query.py` | (none -- stdlib only) | `Query`, `Filter`, operators |
| `readmodels/schema.py` | pydantic (`FieldInfo`), stdlib `types`/`decimal`, plus `readmodels/base` | Read model schema derivation from pydantic fields |
| `readmodels/repository.py` | (none beyond `readmodels/base`; `Query` imported under `TYPE_CHECKING`) | `ReadModelRepository` interface |
| `readmodels/in_memory.py` | (none beyond readmodels base/query/exceptions and `observability`) | `InMemoryReadModelRepository` |
| `readmodels/exceptions.py` | (none) | `ReadModelNotFoundError`, `OptimisticLockError` |

`domain/`, `ports/`, and `application/` are Tier 0 in full, package `__init__`s included: every module under them resolves to stdlib, pydantic, and other Tier 0 in-library modules. This now includes `ports/checkpoints.py` and `ports/dlq.py`, and all five modules of `application/projections/` -- the pure and application-ring halves of the old checkpoint/DLQ Tier-0 blocker (see boundary finding 2, resolved by ADR 0024).

`adapters/memory/` is *not* Tier 0 in full, unlike the three ring packages above -- it is tiered per module, the same way `testing/` and `readmodels/` are. `store.py`, `snapshots.py`, and `checkpoints.py` are individually Tier 0; `dlq.py` is not, because it imports `eventsource.serialization` for `json_dumps`, and `serialization/` carries `orjson` as a core dependency (see "Modules NOT in Tier 0"). Because `adapters/memory/__init__.py` eagerly re-exports all four submodules, importing the *package* -- `from eventsource.adapters.memory import ...` -- always executes `dlq.py` and therefore always pulls orjson, even for callers who only want `InMemoryCheckpointRepository`. Its sibling backend directories (`adapters/postgresql/`, `adapters/sqlite/`, `adapters/sql/`, `adapters/_sql/dialect.py`) are sqlalchemy-backed and not Tier 0 by design; see the next table. `ports/bus.py` owns the `EventPublisher` definition outright; there is no longer any `stores/` module for it to have been transitionally coupled to (see boundary finding 12).

`testing/` is tiered per module, not as a package. The modules above import only stdlib plus in-library Tier 0 contracts -- `builder.py` and `assertions.py` reach for `eventsource.domain.event` and nothing else; `conformance.py` (now bus-only) adds `eventsource.ports.bus`; `recording.py`, `partitioned_memory.py`, and `sync_facade.py` build on `ports.bus`, `ports.handlers`, `domain`, and `ports`, all of which are themselves Tier 0. None of them touch a repository, a harness, or a backend implementation.

Their sibling modules in the same package are not Tier 0, and the package `__init__.py` is not either -- see the next table. `testing/conformance_ports/` is a third category: it carries no driver dependency, but every suite module in it imports `pytest` at module level (and `stateful.py` imports `hypothesis`), so it does not meet the "stdlib + pydantic only" bar either -- see boundary finding 6.

`multitenancy/` is Tier 0 in full: all five modules resolve to stdlib (`contextvars`), pydantic, and other Tier 0 in-library modules -- `repository.py` now sits on `application/aggregates/repository.py`, which is itself Tier 0. Nothing under it reaches a store or repository implementation.

`application/subscriptions/` is Tier 0 in full as well, which is a change from earlier revisions of this document: no module under it imports a repository, a store backend, or a driver. Its only out-of-package imports are `ports` and `observability`. See boundary finding 13.

`readmodels/` is tiered per module in the same way `testing/` is. The contract and in-memory half -- `base.py`, `query.py`, `schema.py`, `repository.py`, `in_memory.py`, `exceptions.py` -- is Tier 0; the backend half (`postgresql.py`, `sqlite.py`, `projection.py`) is not. See the next table.

`config.py` remains a seven-line placeholder with no imports and no configuration classes defined.

## Module-level import coupling within Tier 0

Tier 0 membership is a statement about *which* third-party packages a module pulls in. It is not a statement about independence. A module can be perfectly Tier 0 and still be impossible to ship on its own, because it imports other `eventsource` modules at import time.

`protocols.py` is the clearest case. It advertises itself as the canonical type-contract module, but `src/eventsource/protocols.py:35` is:

```python
from eventsource.domain.event import DomainEvent
```

That import is at module level -- not inside `if TYPE_CHECKING:`, not deferred into a function body -- so importing `eventsource.protocols` executes `eventsource.domain.event`, which in turn imports pydantic. `domain/event.py` is therefore an **extraction floor**: it sits beneath the protocols module and has to move with it. Nothing above the floor can be published without it.

The same shape repeats across the other contract modules:

| Module | Module-level `eventsource` import | Floor it sits on |
|--------|-----------------------------------|------------------|
| `protocols.py` | `events.base` | `events/base.py` |
| `bus/interface.py` | `events.base`, `protocols` | `events/base.py` (+ `protocols.py`) |
| `handlers/decorators.py` | `events.base` | `events/base.py` |
| `handlers/registry.py` | `events.base`, `exceptions`, `handlers.decorators` | `events/base.py`, `exceptions.py` |
| `handlers/adapter.py` | `events.base`, `protocols` | `events/base.py` (+ `protocols.py`) |
| `domain/aggregate.py` | `events.base`, `exceptions`, `types` | `events/base.py`, `exceptions.py`, `types.py` |
| `ports/bus.py` | `events.base` | `events/base.py` |

Two modules are worth calling out as exceptions, because they behave differently from what the table's shape suggests:

- **`ports/snapshots.py` has no `eventsource` imports at all.** Its import block is stdlib only (`abc`, `dataclasses`, `datetime`, `typing`, `uuid`). It does not sit downstream of `events/base.py`, and unlike the modules above it could in principle be lifted on its own.
- **`events/registry.py` imports `events.base` only under `TYPE_CHECKING`** (`src/eventsource/events/registry.py:43-44`). At runtime it is stdlib only. It still ships with `events/` in practice, but the coupling is a typing-time one, not an import-time one.

`types.py` is a floor too, but for `domain/aggregate.py`, not for `protocols.py` -- `protocols.py` does not import it. Both floors are stdlib + pydantic, so neither drags infrastructure in; the cost is purely that the package boundary has to be drawn wide enough to contain them.

**Consequence for extraction.** An `eventsource-core` package cannot ship `protocols.py` without `events/`. The two are a single unit: `events/base.py`, `events/registry.py`, and `events/__init__.py` (which re-exports both) move together with `protocols.py`, and `exceptions.py` and `types.py` come along for the aggregate and handler contracts. Splitting "protocols" and "events" into separate distributions would produce a protocols package that fails to import. `ports/bus.py` defines `EventPublisher` outright now; there is no `stores/` module left for a reverse binding to have pointed at.

## Empty and placeholder modules on the advertised surface

One module qualifies as Tier 0 for a reason worth stating out loud: it contains no code.

`src/eventsource/config.py` is seven lines long in full -- a five-line module docstring, a blank line, and a single trailing comment:

```python
"""Configuration dataclasses for the eventsource library.

This module will contain configuration classes for various components.
Placeholder for future tasks.
"""

# Configuration dataclasses will be added as needed by various tasks
```

There are no imports, no classes, no functions, and no `__all__`. It defines zero configuration classes. Its Tier 0 status is therefore vacuous: a module with no import statements cannot import sqlalchemy, so it passes the boundary test without asserting anything about the library's layering.

Nothing imports it. There is no `from eventsource.config import ...` or `from .config import ...` anywhere under `src/`, and nothing under `tests/` reaches for it either. The only places it appears are two lines of documentation -- `docs/api/index.md:69` lists `eventsource.config` among the public subsystems that ship without top-level re-export, and `docs/api/index.md:797` describes it in the module table as "Configuration helpers." Both descriptions overstate what is there. (`eventsource/application/subscriptions/config.py` is a *different* file, is fully populated, and is unrelated to this one; do not conflate them when grepping.)

**Why this matters for extraction.** A module listed on the advertised API surface and described as providing "configuration helpers" reads, to anyone drawing the `eventsource-core` boundary, like a contract that core consumers depend on. It is not one. Moving it into a core package would ship an empty file; leaving it behind would break nothing.

There are three ways to resolve it, and the choice should be made deliberately rather than by default:

1. **Populate it.** If the library wants a real configuration surface -- store connection settings, retry defaults, serialization options -- this is the module for it. That decision also determines its tier: pydantic-based settings keep it Tier 0, anything reaching for a driver does not.
2. **Drop it.** Delete the file and remove both `docs/api/index.md` references. Nothing imports it, so removal is a no-op for behavior. This is the lowest-cost option if no configuration surface is planned.
3. **Leave it and flag it here.** Keep the placeholder as a marker of intent, but treat this section as the authoritative statement that it is empty, so the extraction boundary is drawn with that knowledge rather than around a name.

Until one of the first two happens, option three is what is in effect, and this section is the flag.

## Modules NOT in Tier 0 (require sqlalchemy, redis, or another core-but-non-stdlib dependency)

| Module | Infrastructure dep | Reason |
|--------|-------------------|--------|
| `serialization/` (`src/eventsource/serialization/README.md`) | `orjson` (core dependency, not stdlib/pydantic) | Moved out of Tier 0 on 2026-07-28: `orjson` became a core dependency of `json.py` (performance over Tier 0 purity, a user decision). It still has no `eventsource`-internal dependency and no sqlalchemy/redis -- it is the lightest module in this table by far, and would be Tier 0 again the moment `orjson` were made optional -- but it no longer meets this document's "stdlib + pydantic only" bar as written. See `docs/reference/serialization-limits.md` for what the dependency buys (native UUID/datetime serialization, non-finite-float and integer-range rejection, non-`\u`-escaped output). |
| `adapters/_sql/engine.py` | sqlalchemy | Shared async engine factory (`create_async_engine`). Module-level `from sqlalchemy import event` and `sqlalchemy.ext.asyncio` imports; exists to apply the SQLite transaction-control and PRAGMA setup the library's guarantees depend on. |
| `adapters/postgresql/store.py` | sqlalchemy (lines 29-31); asyncpg availability-checked in a guard (line 52); orjson via `serialization` | PostgreSQL event store adapter |
| `adapters/postgresql/snapshots.py` | sqlalchemy (lines 15-16) | PostgreSQL snapshot store |
| `adapters/sqlite/store.py` | orjson via `serialization` (line 46); aiosqlite guarded (line 49) | SQLite event store adapter. Per boundary rule 2's spirit, the guard does not exempt it -- the runtime contract requires aiosqlite -- and the unguarded serialization import disqualifies it on its own. |
| `adapters/sqlite/snapshots.py` | aiosqlite (guarded, line 33) | SQLite snapshot store. Clean import block otherwise, but a backend module by runtime contract (boundary rule 2's worked example, `readmodels/sqlite.py`, applies verbatim). |
| `adapters/_sql/dialect.py` | sqlalchemy (line 19), orjson via `serialization` (line 21) | Shared SQL dialect helpers (`Dialect`, `json_param`, `uuid_param`, ...) used by `adapters/sql/checkpoints.py` and `adapters/sql/dlq.py`. The `repositories/_dialect.py` transition shim that used to re-export this module has been removed; nothing under `repositories/` imports it any more. |
| `adapters/sql/checkpoints.py` | sqlalchemy (`text`, `AsyncConnection`, `AsyncEngine`); `adapters/_sql/dialect` (sqlalchemy + orjson) | `SQLCheckpointRepository`, dialect-parameterized for PostgreSQL and SQLite. The SQL-backed half of the old `repositories/checkpoint.py` split (ADR 0024, resolving boundary finding 2). |
| `adapters/sql/dlq.py` | sqlalchemy (`text`, `AsyncConnection`, `AsyncEngine`); `adapters/_sql/dialect` (sqlalchemy + orjson) | `SQLDLQRepository`, dialect-parameterized for PostgreSQL and SQLite. The SQL-backed half of the old `repositories/dlq.py` split (ADR 0024, resolving boundary finding 2). |
| `adapters/memory/dlq.py` | orjson via `eventsource.serialization` (`json_dumps`) | `InMemoryDLQRepository`. No sqlalchemy of its own, unlike its pre-ADR-0024 predecessor -- but it is not stdlib-only either, so it stays out of Tier 0 and is the reason importing the `eventsource.adapters.memory` package (rather than `adapters.memory.checkpoints` directly) still costs a non-stdlib dependency. Its sibling `adapters/memory/checkpoints.py` has no such import and is Tier 0; see the previous table. |
| `bus/redis.py` | redis (guarded, lines 64-67) | Redis event bus -- backend module by runtime contract |
| `bus/kafka/` | aiokafka (guarded, across `bus.py`, `connection.py`, `dlq.py`) | Kafka event bus package |
| `bus/rabbitmq/` | aio-pika (guarded, across its modules) | RabbitMQ event bus package |
| *(`repositories/checkpoint.py` and `repositories/dlq.py` -- deleted)* | -- | These two files, which used to mix a Protocol, dataclasses, an in-memory implementation, and a sqlalchemy-backed SQL implementation in one module each, no longer exist. ADR 0024 split each into a Tier 0 `ports/` module (`ports/checkpoints.py`, `ports/dlq.py`), a Tier 0 in-memory adapter (`adapters/memory/checkpoints.py`; `adapters/memory/dlq.py` is not Tier 0 for an unrelated orjson reason, see above), and a non-Tier-0 SQL adapter (`adapters/sql/checkpoints.py`, `adapters/sql/dlq.py`). See boundary finding 2. |
| *(`repositories/outbox.py` and `repositories/_connection.py` -- deleted)* | -- | ADR 0026 completed the split ADR 0024 made for checkpoints and DLQ: the Tier 0 half (`OutboxRepository` Protocol, `OutboxEntry`/`OutboxStats`, `outbox_event_data()`) now lives in `ports/outbox.py`; the backend half is three per-technology modules, not one dialect-parameterized module (see `adapters/postgresql/outbox.py` below and its sibling `adapters/sqlite/outbox.py`, which is aiosqlite-guarded and appears in the `TYPE_CHECKING` table). `execute_with_connection` is gone with `_connection.py`; every former caller now uses `sql_connection` from `adapters/_sql/connection.py`. The whole `repositories/` package, including its eager JSON-utility re-exports, is deleted. See boundary finding 2. |
| `adapters/postgresql/outbox.py` | sqlalchemy (`sql_connection`, `text`) | `PostgreSQLOutboxRepository`. Same shape as `adapters/postgresql/store.py` and `adapters/postgresql/snapshots.py` -- a genuine SQL adapter, not Tier 0 by design. |
| `adapters/postgresql/readmodels.py` | sqlalchemy, asyncpg | PostgreSQL read model repository. Module-level `from sqlalchemy import text` and `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine`, plus `adapters/_sql/connection` (`sql_connection`), also sqlalchemy-backed. The accepted debt ADR 0026 §4 recorded against `readmodels/postgresql.py` -- a non-`adapters/` module reaching into `adapters/` for a connection helper -- is now resolved: the module itself moved under `adapters/` (ADR 0029 §2.3), so there is no longer a boundary crossing to record here. |
| `adapters/sqlite/readmodels.py` | aiosqlite | SQLite read model repository. No sqlalchemy: it drives `aiosqlite` connections directly, and the `import aiosqlite` sits under `TYPE_CHECKING`. Per boundary rule 2, a `TYPE_CHECKING` guard does not exempt a module -- the runtime contract is an `aiosqlite.Connection`, so this is a backend module, not a contract module. |
| `adapters/sql/readmodel_projection.py` | sqlalchemy (transitive via `adapters/sql/projection.py`) | `ReadModelProjection`. Its own imports are otherwise Tier 0 -- `application/projections/base.py`, `ports/checkpoints.py`, and `ports/dlq.py` are gone from this file's blocker list, since ADR 0024 split them clean. The remaining, different cause is module-level `from eventsource.adapters.sql.projection import DatabaseProjection`, and `adapters/sql/projection.py` is itself a sqlalchemy-backed SQL adapter. `AsyncConnection`/`AsyncSession` imports are `TYPE_CHECKING`-only and are not the cause. |
| *(`projections/base.py`, `projections/checkpoint_manager.py`, `projections/dlq_manager.py` -- deleted)* | -- | The whole `projections/` package no longer exists. ADR 0024 dissolved it into `application/projections/` -- `base.py`, `checkpoints.py`, `dlq.py`, `coordinator.py`, `retry.py` -- which imports only `ports/checkpoints.py` and `ports/dlq.py` for the checkpoint/DLQ contracts, and is Tier 0 in full (see the Tier 0 table and boundary finding 1). |
| `testing/harness.py` | orjson (transitive via `adapters/memory/dlq.py`, reached through the `eventsource.adapters.memory` package `__init__`) | `InMemoryTestHarness`. The sqlalchemy blocker is gone -- ADR 0024's split means `from eventsource.adapters.memory import InMemoryCheckpointRepository, InMemoryDLQRepository` (line 28) no longer touches a repository file that imports sqlalchemy. It still is not Tier 0, for an unrelated reason: importing the `adapters.memory` package (rather than `adapters.memory.checkpoints` directly) runs `adapters/memory/dlq.py`, which imports `eventsource.serialization` for orjson. Every other import in the file (`bus/memory`, `events/base`, `adapters/memory/store.py`) is Tier 0. |
| `testing/bdd.py` | sqlalchemy (transitive via `testing/harness.py`) | Given-When-Then helpers. Module-level `from eventsource.testing.harness import InMemoryTestHarness` (line 49); its other imports (`domain/aggregate`, `events/base`) are Tier 0. It carries no infrastructure dependency of its own -- it inherits the harness's. |
| `testing/__init__.py` | sqlalchemy (transitively) | Eagerly re-exports `InMemoryTestHarness` from `harness.py` and the BDD helpers from `bdd.py` alongside the Tier 0 names, so the whole package namespace is tainted: `from eventsource.testing import EventBuilder` imports sqlalchemy even though `builder.py` itself is Tier 0. |
| `testing/conformance_ports/` | pytest (module level in every suite module), hypothesis (`stateful.py`) | Port-level conformance suites. No driver dependency anywhere in the package -- but it is not importable with stdlib + pydantic alone, so it does not meet this document's Tier 0 bar. See finding 6. |
| `migration/` | sqlalchemy | Live migration tooling. Its `repositories/` submodules import sqlalchemy and `adapters/_sql/connection` (`sql_connection`) directly, since ADR 0026 retired `eventsource.repositories._connection`; `cutover.py` and `coordinator.py` type-hint `PostgreSQLLockManager` (`eventsource.adapters.postgresql.locks`, `TYPE_CHECKING`-only) rather than importing the port. Several leaf modules (`models.py`, `exceptions.py`, `metrics.py`, `write_pause.py`, and others) are individually clean, but the package `__init__` is not. |
| `adapters/postgresql/locks.py` | sqlalchemy | `PostgreSQLLockManager` -- the only production lock manager. `eventsource.ports.locks` (the `DistributedLock`/`LockRegistry` Protocols and `LockInfo`/`migration_lock_key`) and `eventsource.adapters.memory.locks` (`InMemoryLockManager`) are Tier 0 -- see the Tier 0 table. ADR 0029. |

The `readmodels/` split this paragraph used to describe -- "splits down the middle," contract and in-memory halves Tier 0, backend halves not -- **has been performed** (ADR 0029 §2.2-§2.3). There is no longer a `readmodels/` package to split: the contract half (`ReadModel`, `Query`/`Filter`, the `ReadModelRepository` Protocol, the exception family) is `eventsource.ports.readmodels/`, a genuine Tier 0 subpackage; the in-memory adapter is `adapters/memory/readmodels.py`, also Tier 0; the PostgreSQL and SQLite adapters and `ReadModelProjection` are the three rows above, still not Tier 0 for the reasons stated there. `eventsource.readmodels` is now a deprecated re-export shim, lazily resolving each name with a warning, removed in 0.8.0.

`testing/` still splits down the middle as described: the contract and in-memory halves are Tier 0 (see the previous table), while the backend halves land here. `adapters/` splits along directory lines instead: `adapters/memory/store.py`, `snapshots.py`, `checkpoints.py`, `locks.py`, and `readmodels.py` are Tier 0, `adapters/memory/dlq.py` is not (orjson, not sqlalchemy), and the technology directories (`adapters/postgresql/`, `adapters/sqlite/`, `adapters/sql/`) are not.

## Boundary findings

1. **Resolved by ADR 0024: three of the four core `projections/` modules were NOT Tier 0, not just `base.py`.** *(Historical.)* `projections/base.py` used to import `repositories/checkpoint.py` and `repositories/dlq.py`, both of which imported sqlalchemy at module level, and the same pull happened twice more independently through `projections/checkpoint_manager.py` and `projections/dlq_manager.py` -- `base.py` imported both managers as well, so the dependency was redundant rather than layered, and removing the repository imports from `base.py` alone would not have made it Tier 0. ADR 0024 resolved this by dissolving all three modules: `projections/` no longer exists. Its replacement, `application/projections/` (`base.py`, `checkpoints.py`, `dlq.py`, `coordinator.py`, `retry.py`), imports only `ports/checkpoints.py` and `ports/dlq.py` for the checkpoint/DLQ contracts, both of which are Tier 0, and the package is Tier 0 in full -- see the Tier 0 table. The old projection *protocols* module (`projections/protocols.py`) was already clean before the split; its re-export of `AsyncEventHandler` now lives directly on `protocols.py`, which callers should use instead.

2. **Resolved by ADR 0024 for checkpoint/DLQ, and by ADR 0026 for outbox.** *(Historical.)* All three repository modules used to mix a Protocol, dataclasses, an in-memory implementation, and a sqlalchemy-backed SQL implementation in one file each -- the uniform shape of `repositories/`, and the Tier 0 blocker this document flagged as a known violation in the import-linter section of `pyproject.toml`. `checkpoint.py` and `dlq.py` were split first; ADR 0024 moved each into a pure `ports/` module (`ports/checkpoints.py`, `ports/dlq.py` -- the Protocol and dataclasses), an in-memory adapter (`adapters/memory/checkpoints.py`, `adapters/memory/dlq.py`), and a dialect-parameterized SQL adapter (`adapters/sql/checkpoints.py`, `adapters/sql/dlq.py`). `adapters/memory/dlq.py` is not itself Tier 0 -- it imports `eventsource.serialization` for orjson, an unrelated and much smaller problem than the sqlalchemy one it replaced (see "Modules NOT in Tier 0").

   `repositories/outbox.py` completed the set: ADR 0026 moved the `OutboxRepository` Protocol, the `OutboxEntry`/`OutboxStats` dataclasses, and `outbox_event_data()` to `ports/outbox.py`, and split the backend half into three per-technology modules -- `adapters/memory/outbox.py`, `adapters/postgresql/outbox.py`, `adapters/sqlite/outbox.py` -- rather than one dialect-parameterized module like checkpoints and DLQ, because `SQLiteOutboxRepository` is written against a raw `aiosqlite.Connection` and unifying it onto sqlalchemy would have rewritten a working adapter's driver layer for directory-naming symmetry alone. The whole `repositories/` package is now deleted; the import-linter comment block that used to record this as a known violation now records it as resolved for all three modules.

   The projection side and the testing side used to reach into `checkpoint.py` and `dlq.py` for *different* names -- the managers wanted the Protocols and dataclasses, `testing/harness.py` wanted only the in-memory classes -- and both got sqlalchemy anyway, because all four names lived in files whose first two non-stdlib imports were the sqlalchemy pair above. Splitting `checkpoint.py` and `dlq.py` cleared the blocker for `projections/` (now `application/projections/`), for `testing/harness.py` and `testing/bdd.py` and the `testing/__init__.py` namespace behind them, all at once. `readmodels/projection.py` also lost that blocker, but it picked up a different, narrower one in its place: it now imports `adapters/sql/projection.py` directly, which is itself a SQL adapter (see finding 5 and "Modules NOT in Tier 0").

3. **`testing/` is not one tier -- it splits three ways, and the package `__init__` sits on the wrong side of the split.** Read per module rather than per package:

   - `testing/harness.py` no longer has the sqlalchemy blocker described in earlier revisions of this finding: `from eventsource.repositories.checkpoint import InMemoryCheckpointRepository` and `from eventsource.repositories.dlq import InMemoryDLQRepository` are gone, replaced by `from eventsource.adapters.memory import InMemoryCheckpointRepository, InMemoryDLQRepository` (line 28). It is still not Tier 0, for a different and smaller reason: that import touches the `eventsource.adapters.memory` *package*, whose `__init__.py` eagerly re-exports `adapters/memory/dlq.py`, which imports `eventsource.serialization` for orjson. Everything else it imports -- `bus/memory`, `domain/event`, `adapters/memory/store.py` -- is Tier 0.
   - `testing/bdd.py` has no infrastructure import at all. Its single non-Tier-0 line is `from eventsource.testing.harness import InMemoryTestHarness`; its other in-library imports are `domain/aggregate` and `domain/event`. It is out of Tier 0 purely by inheritance from the harness.
   - `testing/builder.py`, `testing/assertions.py`, `testing/conformance.py`, `testing/recording.py`, `testing/partitioned_memory.py`, and `testing/sync_facade.py` are Tier 0 as they stand today, with no pending change. `builder.py` and `assertions.py` each import exactly one in-library name, `eventsource.domain.event.DomainEvent`. `conformance.py` (bus-only now) imports two, both contracts: `bus/interface`, `domain/event`. The three newer modules reach only `bus/interface`, `protocols`, `domain`, `exceptions`, and `ports`. None of the six names a repository, a harness, or a backend.

   The distinction matters because the fix is not uniform. The harness needs its checkpoint/DLQ import re-pointed at the submodule paths (`adapters.memory.checkpoints`, and an orjson-free DLQ source once one exists) rather than the package; `bdd.py` needs nothing; the other six already comply. Treating `testing/` as a single "almost Tier 0" unit hides the fact that most of it is already clean.

### Why this matters to users specifically

This is the one boundary finding whose cost lands on library consumers rather than on maintainers. The shipped test toolkit is precisely the surface people want without a database: `EventBuilder` constructs event instances, `EventAssertions` inspects them, and the conformance suites exercise an `EventStore` or `EventBus` implementation through its interface. All of these are pure-Python and all are Tier 0 on their own. A downstream team writing domain tests against in-memory infrastructure has a legitimate expectation that importing them costs nothing beyond pydantic.

It does not. `testing/__init__.py` eagerly imports every re-exported submodule -- `assertions`, `bdd`, `builder`, `conformance`, `harness`, and `recording` -- so `from eventsource.testing import EventBuilder` executes `harness.py`. Before ADR 0024 that meant executing `repositories/checkpoint.py` and `repositories/dlq.py`, which imported sqlalchemy; today it means executing `eventsource.adapters.memory` (for `InMemoryCheckpointRepository`/`InMemoryDLQRepository`), whose package `__init__` in turn executes `adapters/memory/dlq.py` and pulls orjson. The dependency changed from sqlalchemy to orjson, and shrank considerably, but the shape of the defect did not: the name being imported is Tier 0; the import statement is not. Nothing in the `EventBuilder` call path touches a repository or a driver, and the user has no way to tell from the import line that it will cost anything at all.

That is why finding 2 was worth prioritizing on user-facing grounds and not only architectural ones: the harness taint was a two-line defect in one file, but its blast radius was every import of the public testing namespace. The sqlalchemy instance of it is fixed; the smaller orjson instance described above is what is left.

4. **`observability/` is Tier 0.** It guards opentelemetry behind optional imports with no-op fallbacks. No sqlalchemy or redis.

5. **Resolved by ADR 0029: `readmodels/` split along the seam this finding predicted.** *(Historical.)* This finding used to observe that `readmodels/` already had its Tier 0 and non-Tier-0 halves in separate files (`base.py`, `query.py`, `schema.py`, `repository.py`, `in_memory.py`, `exceptions.py` Tier 0; `postgresql.py`, `sqlite.py`, `projection.py` not) and needed only a package split, not a file-level refactor, to extract. ADR 0029 §2.2-§2.3 performed exactly that: the Tier 0 half moved to `eventsource.ports.readmodels/` (a genuine subpackage, not a flat module -- `model.py`, `query.py`, `repository.py`, `exceptions.py`), the in-memory adapter to `adapters/memory/readmodels.py` (also Tier 0), and the two SQL/aiosqlite-backed pieces to `adapters/{postgresql,sqlite}/readmodels.py` and `adapters/sql/{readmodel_schema,readmodel_projection}.py` (not Tier 0, per the table above). `readmodels/projection.py`'s blocker -- its own module-level import of `adapters/sql/projection.py` -- travelled with it unchanged to `adapters/sql/readmodel_projection.py`. `eventsource.readmodels` is now a deprecated lazy re-export shim over all of the above, removed in 0.8.0.

6. **`testing/conformance.py` is Tier 0 today, and it is bus-only -- the store conformance ABC that used to live here is gone.** Its import block is `abc`, `typing`, `uuid`, and two Tier 0 in-library modules -- `bus/interface` and `events/base`. It reaches no repository, no harness, and no backend. The retired `EventStoreConformanceSuite` has no replacement in this module; store implementers now use the per-port suites in `testing/conformance_ports/` (`AppenderConformance`, `StreamReaderConformance`, `EventLookupConformance`, `GlobalFeedConformance`, `CategoryQueryConformance`, `TypeQueryConformance`), which are not Tier 0 themselves (see below). `EventBusConformanceSuite` is the ABC an out-of-tree bus subclasses to prove it satisfies the `EventBus` contract, so it must travel with the contract it tests. Keeping this module Tier 0 is a constraint on future edits to it, not just an observation: any import added here that pulls sqlalchemy would break the extraction story for every downstream bus. Note that the module is Tier 0 only when imported directly -- `from eventsource.testing import ...` still pulls sqlalchemy through the package `__init__` (see the previous table).

   The newer port-level suites in `testing/conformance_ports/` are deliberately *not* in the same category. They carry no driver imports, but every suite module imports `pytest` at module level (e.g., `appender.py:13`) and `stateful.py` imports `hypothesis` -- both dev-tier dependencies, neither stdlib nor pydantic. If the ports conformance suites are meant to travel with an extracted core the way `conformance.py` is, either the extraction accepts pytest as a dependency of a `testing` extra, or the suites need restructuring so the pytest integration is separable. That is a decision to make at extraction time; this document only records that the bar is currently not met.

7. **Resolved by ADR 0026: `repositories/_connection.py` is deleted, and its consumers now share `adapters/_sql/connection.py:sql_connection`.** *(Historical.)* The module used to be 65 lines hosting `execute_with_connection`, an `@asynccontextmanager` that normalized an `AsyncConnection | AsyncEngine` argument down to a connection, with an unguarded module-level `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine`. It taints nothing now, because it no longer exists: `PostgreSQLOutboxRepository`'s seven call sites, and the five importers outside `repositories/` this finding used to flag as the complication (`readmodels/postgresql.py`, `migration/repositories/audit_log.py`, `position_mapping.py`, `migration.py`, and `routing.py`), all now call `sql_connection(conn, *, write=...)` from `adapters/_sql/connection.py` -- the same helper ADR 0024 already introduced for checkpoints and DLQ, so there is now exactly one connection-normalization helper in the codebase instead of two.

   ADR 0026 §4 recorded this as accepted debt -- five non-`adapters/` modules reaching into `adapters/` for a connection helper -- resolved when those modules themselves moved under `adapters/`. **One of the five is now resolved: `readmodels/postgresql.py`'s 16 `sql_connection` call sites now sit under `adapters/postgresql/readmodels.py` (ADR 0029 §2.3), so that import no longer crosses a ring boundary.** The remaining four -- `migration/repositories/{audit_log,position_mapping,migration,routing}.py` -- are **still open**: they remain non-`adapters/` modules importing from `adapters/_sql/connection.py`, tracked in `BACKLOG.md`'s "Move migration/repositories onto the adapters ring" entry.

   `tests/unit/test_connection_helper.py`, which used to pin `execute_with_connection`'s isinstance-patch behavior at eight call sites, is deleted rather than moved -- the helper it existed to test no longer exists.

8. **`exceptions.py` is the only Tier 0 *module* with no eventsource-internal dependency at all, which is why the rules are calibrated against it.** *(Updated 2026-07-28: this finding previously named `serialization/` as the package meeting this bar. `orjson` becoming a core dependency of `serialization/json.py` moved that package out of Tier 0 entirely -- see "Modules NOT in Tier 0." `exceptions.py` is the replacement reference case, and it is a single module rather than a package, since `serialization/` no longer has a Tier 0 sibling to stand in for it.)* Every other Tier 0 entry in the table above reaches somewhere -- most commonly to `domain/event`, and through it to pydantic. `exceptions.py` does not: it imports `uuid` and nothing else. A few other individual modules match that on their own -- `config.py` (no imports at all, still a placeholder), `readmodels/query.py` (`dataclasses`, `typing`), `domain/stream_id.py` (`re`, `dataclasses`, `uuid`), and `ports/snapshots.py` (stdlib only) -- but each is one file inside a top-level namespace or a package whose extraction unit is larger, so none of them is an extractable unit by itself in the same clean sense. `exceptions.py` is, and that is what makes it the reference case: when a rule below has to say what "Tier 0" means at its strictest, this is the module it is describing, and the pydantic allowance in rule 1 exists precisely because most Tier 0 modules cannot meet the bar `exceptions.py` sets.

9. **`protocols.py` is Tier 0 but not standalone -- `domain/event.py` is a hard floor beneath it.** The module advertises itself as the canonical type-contract home, which invites the assumption that the protocols can be lifted on their own. They cannot. `src/eventsource/protocols.py:35` is a plain module-level `from eventsource.domain.event import DomainEvent` -- not under `if TYPE_CHECKING:`, not deferred into a function -- so importing `eventsource.protocols` executes `eventsource.domain.event` and, through it, pydantic. The same shape holds for `domain/decorators.py:20` and `bus/interface.py:16-17` (which imports `domain.event` *and* `protocols`). Every one of these is Tier 0 by the dependency test and non-extractable by itself. The consequence for an `eventsource-core` split is concrete: `domain/event.py` and `protocols.py` are a single shipping unit, and `exceptions.py` and `types.py` come with them for the aggregate and handler contracts. A distribution containing `protocols.py` without `domain/event.py` would fail to import. See "Module-level import coupling within Tier 0" above for the full floor-by-module table, including the two modules that break the pattern (`ports/snapshots.py`, which has no `eventsource` imports at all, and `domain/event_registry.py`, whose coupling is `TYPE_CHECKING`-only).

10. **`config.py` is an empty placeholder that the documentation describes as a real subsystem.** The file is seven lines total: a five-line docstring saying "This module will contain configuration classes for various components. Placeholder for future tasks.", a blank line, and one trailing comment. Zero imports, zero classes, zero functions, no `__all__`. Nothing under `src/` or `tests/` imports it. Its Tier 0 status is vacuous -- a file with no import statements passes the boundary test without asserting anything about layering -- so it should not be counted as evidence that the core surface is in good shape. What makes it a finding rather than a curiosity is the gap between the file and its documentation: `docs/api/index.md:69` lists `eventsource.config` among the public subsystems that ship without top-level re-export, and `docs/api/index.md:797` describes it in the module table as "Configuration helpers." Anyone drawing the extraction boundary from the API docs would treat it as a contract core consumers depend on. It is not one, and moving it into a core package would ship an empty file. (Do not confuse it with `eventsource/application/subscriptions/config.py`, which is a real, populated module.) Resolve it -- populate or delete -- before extraction; see "Empty and placeholder modules on the advertised surface" above for the options and the trade-offs.

11. **Resolved by ADR 0026: `repositories/__init__.py` no longer exists.** *(Historical.)* The package `__init__` used to be 97 lines, and every line of executable code in it was an eager `from ... import` at module scope: `checkpoint`, `dlq`, `outbox`, and `eventsource.serialization`, followed by a flat 23-name `__all__` -- no `__getattr__`, no `TYPE_CHECKING` guard, no deferred import anywhere. Splitting `checkpoint.py`, `dlq.py`, and `outbox.py` into interface + in-memory and SQL halves (finding 2) did not by itself unblock this file; the `__init__` itself had to stop eagerly re-exporting the SQL-backed names. It never got narrowed or made lazy, because the whole package was deleted instead once outbox landed, which is a stronger fix than either option this finding used to weigh: there is no longer a namespace whose mere touch runs three module bodies and imports sqlalchemy, and the JSON-utility re-export (`EventSourceJSONEncoder`, `json_dumps`, `json_loads`) is gone with it -- callers reach `eventsource.serialization` directly, at the cost of orjson alone.

    The top-level package's own cost, which this finding used to trace up from `repositories/__init__.py`, is unaffected by the deletion and is worth restating rather than leaving implied: `src/eventsource/__init__.py` no longer imports from `eventsource.repositories` at all, but it still imports `eventsource.adapters._sql.engine` and `eventsource.adapters.postgresql` at module level, each of which is sqlalchemy at import time in its own right. `import eventsource` still requires sqlalchemy regardless of what the caller wants -- deleting `repositories/` removed one route to that cost, not the cost itself. See cleanup step 5.

12. **The ring packages -- `domain/`, `ports/`, `application/`, `adapters/memory/` -- are Tier 0 in full, and `ports/` no longer has any import path of its own to sqlalchemy.** Per module, the new hexagonal core is exactly what this document asks for: every file under those four packages, `__init__`s included, resolves to stdlib, pydantic, and Tier 0 in-library modules. The legacy `stores/` package is gone entirely, so there is no longer any transitional re-export seam to describe: `EventPublisher` is defined in `ports/bus.py` and nothing else claims it. The complete import closure of the `ports` subpackage, package `__init__`s included, is `domain/`, `events/`, `exceptions.py`, and `types.py`; verified by importing every `ports` submodule under a stubbed top-level package and asserting sqlalchemy absent from `sys.modules`. The Tier 0 import-linter contract now lists `eventsource.ports` as a whole (not just `ports/snapshots`), so a reintroduced outward import would break the build. The remaining caveat is not specific to `ports/`: any `import eventsource.<anything>` executes the top-level `eventsource/__init__.py` first, and that front-door initializer eagerly reaches `adapters/postgresql/snapshots.py` (see the import chain in the verification section below), so a bare runtime `import eventsource.ports` still loads sqlalchemy for the same reason importing anything in the library does. Clearing that requires making the top-level `__init__` lazy about its sqlalchemy-backed exports -- a property of the delivery mechanism's front door, not of the ring packages.

13. **`application/subscriptions/` no longer belongs in the non-Tier-0 table.** Earlier revisions of this document listed it as sqlalchemy-tainted "via stores, repositories." That is no longer true: no module under `application/subscriptions/` imports a repository, a store backend, or a driver, at module level or otherwise. The package's only out-of-package imports are `ports` and `observability` -- all Tier 0 -- and its runtime contracts are the store/bus ports, satisfiable by the in-memory adapters. The package `__init__` eagerly re-exports its own submodules only, so the namespace is as clean as the modules. It is a large surface (25+ modules) rather than a small contract set, so whether it belongs in an extracted core is a scoping question, not a dependency one.

## Boundary rules for Tier 0

1. **Allowed dependencies**: stdlib, pydantic, typing-extensions. (Pydantic is *permitted*, not *required* -- a Tier 0 module that needs none of it should import none of it. `exceptions.py` is the pydantic-free case: its whole import surface is `uuid`, and it stays that way. `serialization/` was this document's previous example of the pydantic-free case; it no longer qualifies, having taken on `orjson` as a core dependency -- see "Modules NOT in Tier 0.")
2. **No sqlalchemy imports** -- not even behind `TYPE_CHECKING`. Tier 0 modules must be importable without sqlalchemy installed.
3. **Optional deps must be guarded**: opentelemetry is acceptable if behind `try/except ImportError` with no-op fallback.
4. **In-memory implementations belong in Tier 0**: They implement the interface contracts using only stdlib. They are essential for testing without infrastructure.
5. **Interface + implementation separation**: Files that define both a Protocol/ABC and a sqlalchemy-backed implementation must be split before the interface can move to Tier 0.
6. **Tier 0 membership travels with the module's own transitive Tier 0 imports.** Passing the dependency test is not the same as being extractable. If a module imports another `eventsource` module at import time, that module is part of its extraction unit and must move with it -- there is no partial ship. `protocols.py` is the worked example: `src/eventsource/protocols.py:35` is a plain module-level `from eventsource.domain.event import DomainEvent`, so a distribution containing `protocols.py` without `domain/event.py` fails to import. When claiming a module for Tier 0, close its module-level `eventsource` import set transitively and confirm every member is also Tier 0 and also in scope for the move. Imports under `if TYPE_CHECKING:` do not create this obligation (they are typing-time only, as in `domain/event_registry.py:43-44`) -- but note this is the opposite of rule 2, where a `TYPE_CHECKING` guard does *not* excuse a sqlalchemy import. The two rules ask different questions: rule 2 is about what must not be installed, this rule is about what must ship together.
7. **Placeholder and empty modules do not count as satisfied surface.** A module qualifies for Tier 0 only by *asserting* something about layering, not by containing too little code to violate it. `src/eventsource/config.py` is seven lines of docstring and comment with zero imports, zero classes, and no importer anywhere in `src/` or `tests/`; it passes rules 1-3 vacuously. Do not treat such a module as a satisfied contract, do not cite it as evidence the core surface is healthy, and do not carry it into an extracted package on the strength of its name or its API-doc description. Resolve it first -- populate it, or delete it and its documentation references -- so the boundary is drawn around code that exists.

## Recommended pre-extraction cleanup

These are tracked items; steps 1 and 2 have landed since the last revision of this document, the rest have not. Steps 1-3 were originally the same operation applied three times to the three repository modules; steps 4 and 5 are the parts that are easy to miss, and skipping either of them leaves the boundary where it started.

1. **Split `repositories/checkpoint.py` into an interface + in-memory module and a SQL backend module.** **Done (ADR 0024).** The Tier 0 half -- the `CheckpointRepository` Protocol and its composed pieces, the `CheckpointData`/`LagMetrics` dataclasses -- now lives in `ports/checkpoints.py`, plus `InMemoryCheckpointRepository` in `adapters/memory/checkpoints.py`. The backend half, `SQLCheckpointRepository` (dialect-parameterized for PostgreSQL and SQLite), now lives in `adapters/sql/checkpoints.py`. `repositories/checkpoint.py` itself is deleted. Importing the Protocol or the in-memory class no longer requires sqlalchemy, which is what boundary rule 5 asked for.

2. **Split `repositories/dlq.py` the same way.** **Done (ADR 0024).** Interface + dataclasses (`DLQRepository`, `DLQEntry`, `DLQStats`, `ProjectionFailureCount`) now live in `ports/dlq.py`; the in-memory implementation is `adapters/memory/dlq.py`; the SQL-backed `SQLDLQRepository` is `adapters/sql/dlq.py`. `repositories/dlq.py` itself is deleted. One caveat carried over rather than fully resolved: `adapters/memory/dlq.py` imports `eventsource.serialization` for orjson, so it is not itself Tier 0 -- a smaller, unrelated problem than the sqlalchemy one this step removed (see boundary finding 2 and "Modules NOT in Tier 0").

3. **Split `repositories/outbox.py` the same way.** **Done (ADR 0026).** The Tier 0 half -- the `OutboxRepository` Protocol, the `OutboxEntry`/`OutboxStats` dataclasses, and `outbox_event_data()` -- now lives in `ports/outbox.py`. The backend half is three per-technology modules rather than one dialect-parameterized module like checkpoints and DLQ: `adapters/memory/outbox.py`, `adapters/postgresql/outbox.py`, and `adapters/sqlite/outbox.py`. Unlike checkpoints and DLQ, the outbox could not land dialect-parameterized -- `SQLiteOutboxRepository` is written against a raw `aiosqlite.Connection`, not a sqlalchemy engine or session, and unifying it would mean rewriting a working adapter's driver layer for no functional gain (ADR 0026 §2, rejected alternative). `repositories/outbox.py` itself is deleted along with the rest of the `repositories/` package.

4. **Confine `_connection.py` to the backend side.** **Done (ADR 0026), by deletion rather than relocation.** `repositories/_connection.py` and its `execute_with_connection` helper are gone. Every call site -- the seven inside `PostgreSQLOutboxRepository`, plus the five importers outside `repositories/` this step originally flagged as the constraint (`readmodels/postgresql.py`, and `migration/repositories/audit_log.py`, `migration/repositories/position_mapping.py`, `migration/repositories/migration.py`, `migration/repositories/routing.py`) -- now calls `sql_connection(conn, *, write=...)` from `adapters/_sql/connection.py`, the same helper ADR 0024 introduced for checkpoints and DLQ. `tests/unit/test_connection_helper.py` is deleted rather than moved: it existed to pin `execute_with_connection`'s isinstance-patch behavior, and that helper no longer exists to pin.

   **Accepted debt, recorded rather than resolved:** the five migration/readmodels importers above are non-Tier-0 backend modules importing a name from `adapters/`, which is permitted by the dependency rule (no ring boundary is crossed) but is a naming smell -- five modules outside `adapters/` reaching into it for a connection helper. ADR 0026 §4 records this as intentional debt, closed when those five modules themselves move into `adapters/` in a future slice.

5. **Reduce the eager re-exports in `repositories/__init__.py`.** **Moot -- the package is deleted.** `repositories/__init__.py` no longer exists, so there is nothing left to narrow or make lazy; the "two viable shapes" this step used to weigh (narrow the eager surface, or make it lazy via `__getattr__`) were never built, because the package that would have hosted either shape was removed outright in the same slice that would have applied them.

   The re-export cost this step traced up to the top-level package is **not** resolved by the deletion, and that is worth stating plainly rather than letting the step's closure imply otherwise. `src/eventsource/__init__.py` no longer imports from `eventsource.repositories` -- that route to sqlalchemy is gone -- but it still imports `eventsource.adapters._sql.engine` and `eventsource.adapters.postgresql` at module level, both sqlalchemy at import time in their own right (see the front-door import chain in the verification section below). `import eventsource` still requires sqlalchemy regardless of what the caller wants; deleting `repositories/` removed one route to that cost without removing the cost itself. Making the top-level `__init__` lazy about its sqlalchemy-backed exports is unchanged in scope from before this step's closure and stays out of scope for this slice.

6. **The payoff: with the split, `application/projections/` (the successor to `projections/base.py`) is Tier 0, and `testing/harness.py` has shrunk to a single, smaller open item.** **Realized for `application/projections/`.** The table below records what the old `projections/` modules imported and where those names live now -- `projections/base.py` itself no longer exists, dissolved into `application/projections/base.py` and its siblings.

   | Old module (deleted) | Old import to re-point | Names it wanted | Where they live now |
   |--------|--------------------|-------------------------|----------------------|
   | `projections/base.py` | `repositories.checkpoint`, `repositories.dlq` | `CheckpointRepository`, `InMemoryCheckpointRepository`, `DLQRepository`, `InMemoryDLQRepository` | `application/projections/base.py` imports the Protocols from `ports/checkpoints.py` and `ports/dlq.py`; the in-memory classes are reached via `adapters/memory` where needed, not by `application/projections/` itself |
   | `projections/checkpoint_manager.py` | `repositories.checkpoint` | `CheckpointRepository`, `InMemoryCheckpointRepository` | Dissolved into `application/projections/checkpoints.py`, which imports `ProjectionCheckpoints` from `ports/checkpoints.py` |
   | `projections/dlq_manager.py` | `repositories.dlq` | `DLQEntry`, `DLQRepository`, `InMemoryDLQRepository` | Dissolved into `application/projections/dlq.py`, which imports `DLQEntry`, `DLQRepository` from `ports/dlq.py` |

   **Not fully realized for `testing/harness.py`.** The re-point happened -- it now imports `InMemoryCheckpointRepository`, `InMemoryDLQRepository` from `eventsource.adapters.memory` (lines 28) -- but that import targets the package rather than `adapters.memory.checkpoints` directly, and the package `__init__` eagerly pulls in `adapters/memory/dlq.py`, which is not itself Tier 0 (orjson via `eventsource.serialization`). Pointing `testing/harness.py` at `eventsource.adapters.memory.checkpoints` directly, and at an orjson-free source for `InMemoryDLQRepository` (or accepting orjson as the harness's one remaining non-stdlib cost), is what closes this out; see boundary finding 3. Because both names now target the package rather than a single defining module, step 5 is not a prerequisite for this one either -- the constraint is the package `__init__`'s own eagerness, not `repositories/__init__.py`. Everything else `testing/harness.py` imports -- `bus/memory`, `events/base`, `adapters/memory/store.py` -- is already Tier 0. Closing this out also unblocks `testing/bdd.py` and the `testing/__init__.py` namespace, both of which inherit the harness's dependency.

## Verifying the boundary

The tables above are a snapshot. Every claim in them was produced by the checks below, and those checks are how you re-derive a module's tier after any change -- do not trust the table over a fresh grep.

### The check: module-level infrastructure imports

A module is disqualified from Tier 0 by an import that runs at import time. The direct check is a grep anchored at column zero, which is what "module level" means in an import block:

```bash
grep -rlE '^(from|import) (sqlalchemy|redis|asyncpg|aiosqlite|aiokafka|aio_pika)' \
  src/eventsource/ --include='*.py' | sort
```

Run against the tree today, this returns fourteen files -- down from fifteen after ADR 0026 deleted `repositories/_connection.py` and `repositories/outbox.py` and added `adapters/postgresql/outbox.py` in their place (net one file fewer). The legacy `stores/` package (`stores/postgresql.py`, `stores/sqlite.py`) is gone entirely -- deleted along with the rest of `stores/` in favor of `ports/` + `adapters/` -- which drops two further rows a prior revision of this document carried:

```
src/eventsource/adapters/_sql/connection.py
src/eventsource/adapters/_sql/dialect.py
src/eventsource/adapters/postgresql/outbox.py
src/eventsource/adapters/postgresql/snapshots.py
src/eventsource/adapters/postgresql/store.py
src/eventsource/adapters/sql/checkpoints.py
src/eventsource/adapters/sql/dlq.py
src/eventsource/adapters/_sql/engine.py
src/eventsource/locks/postgresql.py
src/eventsource/migration/repositories/audit_log.py
src/eventsource/migration/repositories/migration.py
src/eventsource/migration/repositories/position_mapping.py
src/eventsource/migration/repositories/routing.py
src/eventsource/readmodels/postgresql.py
```

Every other non-Tier-0 module in this document is disqualified *transitively* -- it imports one of these fifteen, or something that does (or, in the `serialization/` and `conformance_ports/` cases, a non-driver dependency the grep does not cover: `orjson`, `pytest`, `hypothesis`). So the grep is a first pass, not the whole answer: a clean result means "not directly disqualified," and you still have to walk the module's own in-library imports and confirm each one is Tier 0.

### TYPE_CHECKING guards do not exempt a module

Boundary rule 2 says no sqlalchemy imports, *not even behind `TYPE_CHECKING`*. This is the rule people get wrong most often, because a guarded import genuinely does not execute at runtime and the module will import fine with the dependency absent. It is still not Tier 0.

The reason is that the guard hides the dependency without removing it. If a method signature is annotated `connection: AsyncConnection`, the runtime contract is a sqlalchemy object regardless of when the name is resolved -- the module is a backend adapter that happens to defer a symbol lookup. A Tier 0 module has to be usable, not merely importable, without the infrastructure library.

Two consequences for how you run the check:

- **Do not relax the anchor to `^\s*`.** A leading-whitespace regex sweeps in guarded and function-local imports, and its output mixes genuine module-level violations with indented ones. Run the anchored grep for disqualification, then run the indented variant separately as a review list, not as a verdict:

  ```bash
  grep -rnE '^[ \t]+(from|import) (sqlalchemy|redis|asyncpg|aiosqlite|aiokafka|aio_pika)' \
    src/eventsource/ --include='*.py'
  ```

  Today that surfaces the guarded driver imports across `bus/redis.py` (lines 64-67), the `bus/kafka/` and `bus/rabbitmq/` package modules, `adapters/postgresql/store.py:52` (asyncpg), `adapters/sqlite/store.py:46` and `adapters/sqlite/snapshots.py:33` (aiosqlite), `adapters/sqlite/outbox.py:31` (aiosqlite -- ADR 0026's replacement for the old `repositories/outbox.py:38` guard), `adapters/sql/projection.py:355,359` (sqlalchemy), `readmodels/sqlite.py:37`, and `readmodels/projection.py:26` and `:231`. None of those lines runs at import time; none of those modules is Tier 0 either.

- **`readmodels/sqlite.py` is the worked example.** It does not appear in the anchored grep at all -- it uses no sqlalchemy and imports `aiosqlite` only under `TYPE_CHECKING`. It is still a backend module, because its public methods take an `aiosqlite.Connection`. Judge by what the module requires of its caller, not by what its import block executes.

### Why an import-time test does not work today

The tempting automated check -- install a `sys.meta_path` finder that raises on `sqlalchemy`, then `importlib.import_module` the target -- currently fails for *every* module in the package, including ones this document lists as Tier 0. The chain is in the package initializers, not the modules. Deleting `stores/` shortened the chain further: it no longer routes through a `stores/__init__` that pulls in a PostgreSQL backend as a side effect of touching a Tier 0 interface module. The front door now reaches sqlalchemy directly through the top-level `__init__`'s own eager import of `adapters/postgresql/`:

```
eventsource/__init__.py:37       (from eventsource.adapters.postgresql import ASYNCPG_AVAILABLE, PostgreSQLEventStore)
  -> adapters/postgresql/__init__.py:3   (from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore)
  -> adapters/postgresql/snapshots.py:15 (from sqlalchemy import text)
```

That is a three-hop chain, down from the five-hop one this document recorded before `stores/` was deleted. The sqlalchemy enters entirely through the top-level `eventsource/__init__.py` eagerly importing the `adapters.postgresql` package -- fired unconditionally, before any caller-specific import is ever reached. Importing `eventsource.testing.conformance` -- or anything else -- runs `eventsource/__init__.py` first, so the blocker fires before the target module is ever reached. `application/aggregates/repository.py` itself stays clean in the static graph: it imports only `ports.bus`, `ports.envelopes`, `ports.positions`, and `ports.store` at module level, all Tier 0, with its snapshot names coming from `ports.snapshots` only under `TYPE_CHECKING`; the runtime taint above belongs to the top-level `__init__.py`'s own import list, not to it. `PostgreSQLSnapshotStore` itself is still not re-exported from the top-level package (see the comment at the top of `eventsource/__init__.py`) and must be imported path-only; it is `PostgreSQLEventStore`, the sibling name in the same `adapters.postgresql` import statement, that the top-level `__init__` asks for at line 37 -- and importing that package `__init__` runs `snapshots.py` regardless of which of the two names the caller wanted. This is the same package-taint pattern already described for `repositories/__init__.py`, `testing/__init__.py`, and `readmodels/` -- an eager `__init__` reaching a backend before the caller asked for one.

So: **tier is a property of a module, not of the package that contains it**, and until the package initializers are made lazy, static grep plus manual import-walking is the only reliable verification. An import-time test becomes the authoritative check the moment those initializers stop eagerly importing backends -- and adding one is a reasonable acceptance criterion for that work.

### Checklist for a single module

1. Run the anchored grep on the file. Any hit: not Tier 0, stop.
2. Read its import block. For each `from eventsource.X import ...` at module level, confirm `X` is Tier 0 per the tables above -- recursing if you do not already know. Watch for package imports (`from eventsource.adapters.postgresql import ...`), which pull that package's `__init__.py` and everything it re-exports, not just the name you asked for.
3. Check the `TYPE_CHECKING` block and the method signatures. If a public signature's runtime contract is an infrastructure object, the module is a backend adapter regardless of step 1.
4. Optional deps are acceptable only in the `try/except ImportError` with no-op fallback shape that `observability/` uses (boundary rule 3). A bare guarded import is not that shape.
