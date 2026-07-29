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
| `events/base.py` | pydantic | `DomainEvent` base class |
| `events/registry.py` | (none beyond events/base) | `EventRegistry` auto-registration |
| `aggregates/base.py` | pydantic (via events, types) | `AggregateRoot`, `DeclarativeAggregate` |
| `protocols.py` | `from eventsource.events.base import DomainEvent` (module level, so pydantic transitively) -- not standalone | `EventHandler`, `SyncEventHandler`, `EventSubscriber` protocols/ABCs |
| `stores/interface.py` | (none beyond events/base) | `EventStore` ABC, `StoredEvent`, `EventStream` |
| `bus/interface.py` | (none beyond events/base) | `EventBus` ABC |
| `snapshots/interface.py` | pydantic (via events/base) | `SnapshotStore` ABC |
| `snapshots/strategies.py` | (none beyond snapshots/interface) | Snapshot strategy definitions |
| `handlers/decorators.py` | pydantic (via events/base) | `@handles` decorator |
| `handlers/registry.py` | (none beyond handlers/decorators, events, exceptions) | `HandlerRegistry` |
| `handlers/adapter.py` | (none beyond protocols, events) | Sync/async handler adapter |
| `exceptions.py` | (none) | All exception types |
| `config.py` | (none -- empty placeholder module, defines zero configuration classes) | Nothing yet; see "Empty and placeholder modules" below |
| `types.py` | pydantic | `AggregateId`, `TState`, `Version`, etc. |
| `observability/` | (none -- opentelemetry is optional, guarded) | `Tracer`, attribute constants |
| `stores/in_memory.py` | (none beyond stores/interface) | `InMemoryEventStore` |
| `bus/memory.py` | (none beyond bus/interface) | `InMemoryEventBus` |
| `snapshots/in_memory.py` | (none beyond snapshots/interface) | `InMemorySnapshotStore` |
| `projections/protocols.py` | (none beyond `eventsource.protocols`) | Re-exports `AsyncEventHandler` from the canonical `protocols.py` -- the only Tier 0 module under `projections/` |
| `testing/builder.py` | pydantic (via events/base) | `EventBuilder` fluent test-event builder |
| `testing/assertions.py` | pydantic (via events/base) | `EventAssertions` domain-specific test assertions |
| `testing/conformance.py` | (none beyond events/base, exceptions, stores/interface, bus/interface) | `EventStoreConformanceSuite`, `EventBusConformanceSuite` |
| `aggregates/repository.py` | (none beyond aggregates/base, snapshots, stores/interface, observability) | `AggregateRepository` load/save orchestration |
| `aggregates/snapshot_manager.py` | (none beyond observability; snapshot types imported under `TYPE_CHECKING` or lazily inside methods) | `AggregateSnapshotManager` |
| `sync/adapter.py` | stdlib `threading` + `concurrent.futures.ThreadPoolExecutor`, plus `stores/interface` | `SyncEventStoreAdapter` wrapping an async store for sync callers |
| `multitenancy/context.py` | stdlib `contextvars` only, plus `multitenancy/exceptions` | Tenant context get/set, `get_required_tenant` |
| `multitenancy/events.py` | pydantic (`Field`), plus `events/base` and `multitenancy/context` | `TenantDomainEvent` |
| `multitenancy/exceptions.py` | (none beyond `eventsource.exceptions`) | `TenantContextNotSetError`, `TenantMismatchError` |
| `multitenancy/repository.py` | (none beyond aggregates/base, aggregates/repository, multitenancy context + exceptions) | Tenant-scoped aggregate repository |
| `readmodels/base.py` | pydantic (`BaseModel`, `ConfigDict`, `Field`) | `ReadModel` base class |
| `readmodels/query.py` | (none -- stdlib only) | `Query`, `Filter`, operators |
| `readmodels/schema.py` | pydantic (`FieldInfo`), stdlib `types`/`decimal`, plus `readmodels/base` | Read model schema derivation from pydantic fields |
| `readmodels/repository.py` | (none beyond `readmodels/base`; `Query` imported under `TYPE_CHECKING`) | `ReadModelRepository` interface |
| `readmodels/in_memory.py` | (none beyond readmodels base/query/exceptions and `observability`) | `InMemoryReadModelRepository` |
| `readmodels/exceptions.py` | (none) | `ReadModelNotFoundError`, `OptimisticLockError` |

`testing/` is tiered per module, not as a package. The three modules above import only stdlib plus in-library Tier 0 contracts -- `builder.py` and `assertions.py` reach for `eventsource.events.base` and nothing else; `conformance.py` adds `eventsource.exceptions`, `eventsource.stores.interface`, and `eventsource.bus.interface`, all of which are themselves Tier 0. None of them touch a repository, a harness, or a backend implementation.

Their sibling modules in the same package are not Tier 0, and the package `__init__.py` is not either -- see the next table.

`multitenancy/` is Tier 0 in full: all four modules resolve to stdlib (`contextvars`), pydantic, and other Tier 0 in-library modules. Nothing under it reaches a store or repository implementation.

`readmodels/` is tiered per module in the same way `testing/` is. The contract and in-memory half -- `base.py`, `query.py`, `schema.py`, `repository.py`, `in_memory.py`, `exceptions.py` -- is Tier 0; the backend half (`postgresql.py`, `sqlite.py`, `projection.py`) is not. See the next table.

`config.py` remains a seven-line placeholder with no imports and no configuration classes defined.

## Module-level import coupling within Tier 0

Tier 0 membership is a statement about *which* third-party packages a module pulls in. It is not a statement about independence. A module can be perfectly Tier 0 and still be impossible to ship on its own, because it imports other `eventsource` modules at import time.

`protocols.py` is the clearest case. It advertises itself as the canonical type-contract module, but `src/eventsource/protocols.py:35` is:

```python
from eventsource.events.base import DomainEvent
```

That import is at module level -- not inside `if TYPE_CHECKING:`, not deferred into a function body -- so importing `eventsource.protocols` executes `eventsource.events.base`, which in turn imports pydantic. `events/base.py` is therefore an **extraction floor**: it sits beneath the protocols module and has to move with it. Nothing above the floor can be published without it.

The same shape repeats across the other contract modules:

| Module | Module-level `eventsource` import | Floor it sits on |
|--------|-----------------------------------|------------------|
| `protocols.py` | `events.base` | `events/base.py` |
| `stores/interface.py` | `events.base` | `events/base.py` |
| `bus/interface.py` | `events.base`, `protocols` | `events/base.py` (+ `protocols.py`) |
| `handlers/decorators.py` | `events.base` | `events/base.py` |
| `handlers/registry.py` | `events.base`, `exceptions`, `handlers.decorators` | `events/base.py`, `exceptions.py` |
| `handlers/adapter.py` | `events.base`, `protocols` | `events/base.py` (+ `protocols.py`) |
| `aggregates/base.py` | `events.base`, `exceptions`, `types` | `events/base.py`, `exceptions.py`, `types.py` |

Two modules are worth calling out as exceptions, because they behave differently from what the table's shape suggests:

- **`snapshots/interface.py` has no `eventsource` imports at all.** Its import block is stdlib only (`abc`, `dataclasses`, `datetime`, `typing`, `uuid`). It does not sit downstream of `events/base.py`, and unlike the modules above it could in principle be lifted on its own.
- **`events/registry.py` imports `events.base` only under `TYPE_CHECKING`** (`src/eventsource/events/registry.py:43-44`). At runtime it is stdlib only. It still ships with `events/` in practice, but the coupling is a typing-time one, not an import-time one.

`types.py` is a floor too, but for `aggregates/base.py`, not for `protocols.py` -- `protocols.py` does not import it. Both floors are stdlib + pydantic, so neither drags infrastructure in; the cost is purely that the package boundary has to be drawn wide enough to contain them.

**Consequence for extraction.** An `eventsource-core` package cannot ship `protocols.py` without `events/`. The two are a single unit: `events/base.py`, `events/registry.py`, and `events/__init__.py` (which re-exports both) move together with `protocols.py`, and `exceptions.py` and `types.py` come along for the aggregate and handler contracts. Splitting "protocols" and "events" into separate distributions would produce a protocols package that fails to import.

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

Nothing imports it. There is no `from eventsource.config import ...` or `from .config import ...` anywhere under `src/`, and nothing under `tests/` reaches for it either. The only places it appears are two lines of documentation -- `docs/api/index.md:67` lists `eventsource.config` among the public subsystems that ship without top-level re-export, and `docs/api/index.md:780` describes it in the module table as "Configuration helpers." Both descriptions overstate what is there. (`eventsource/subscriptions/config.py` is a *different* file, is fully populated, and is unrelated to this one; do not conflate them when grepping.)

**Why this matters for extraction.** A module listed on the advertised API surface and described as providing "configuration helpers" reads, to anyone drawing the `eventsource-core` boundary, like a contract that core consumers depend on. It is not one. Moving it into a core package would ship an empty file; leaving it behind would break nothing.

There are three ways to resolve it, and the choice should be made deliberately rather than by default:

1. **Populate it.** If the library wants a real configuration surface -- store connection settings, retry defaults, serialization options -- this is the module for it. That decision also determines its tier: pydantic-based settings keep it Tier 0, anything reaching for a driver does not.
2. **Drop it.** Delete the file and remove both `docs/api/index.md` references. Nothing imports it, so removal is a no-op for behavior. This is the lowest-cost option if no configuration surface is planned.
3. **Leave it and flag it here.** Keep the placeholder as a marker of intent, but treat this section as the authoritative statement that it is empty, so the extraction boundary is drawn with that knowledge rather than around a name.

Until one of the first two happens, option three is what is in effect, and this section is the flag.

## Modules NOT in Tier 0 (require sqlalchemy, redis, or another core-but-non-stdlib dependency)

| Module | Infrastructure dep | Reason |
|--------|-------------------|--------|
| [`serialization/`](../src/eventsource/serialization/README.md) | `orjson` (core dependency, not stdlib/pydantic) | Moved out of Tier 0 on 2026-07-28: `orjson` became a core dependency of `json.py` (performance over Tier 0 purity, a user decision). It still has no `eventsource`-internal dependency and no sqlalchemy/redis -- it is the lightest module in this table by far, and would be Tier 0 again the moment `orjson` were made optional -- but it no longer meets this document's "stdlib + pydantic only" bar as written. See `docs/reference/serialization-limits.md` for what the dependency buys (native UUID/datetime serialization, non-finite-float and integer-range rejection, non-`\u`-escaped output). |
| `stores/postgresql.py` | sqlalchemy, asyncpg | PostgreSQL event store implementation |
| `stores/sqlite.py` | sqlalchemy (aiosqlite) | SQLite event store implementation |
| `snapshots/postgresql.py` | sqlalchemy, asyncpg | PostgreSQL snapshot store |
| `snapshots/sqlite.py` | sqlalchemy (aiosqlite) | SQLite snapshot store |
| `bus/redis.py` | redis | Redis event bus |
| `bus/kafka.py` | aiokafka | Kafka event bus |
| `bus/rabbitmq.py` | aio-pika | RabbitMQ event bus |
| `repositories/checkpoint.py` | sqlalchemy | Checkpoint repository -- one module carrying the `CheckpointRepository` Protocol, the `CheckpointData`/`LagMetrics` dataclasses, `InMemoryCheckpointRepository`, and both `PostgreSQLCheckpointRepository` and `SQLiteCheckpointRepository` |
| `repositories/dlq.py` | sqlalchemy | DLQ repository -- one module carrying the `DLQRepository` Protocol, the `DLQEntry`/`DLQStats`/`ProjectionFailureCount` dataclasses, `InMemoryDLQRepository`, and both `PostgreSQLDLQRepository` and `SQLiteDLQRepository` |
| `repositories/outbox.py` | sqlalchemy | Outbox repository -- one module carrying the `OutboxRepository` Protocol, the `OutboxEntry`/`OutboxStats` dataclasses, `InMemoryOutboxRepository`, and both `PostgreSQLOutboxRepository` and `SQLiteOutboxRepository` |
| `repositories/_connection.py` | sqlalchemy.ext.asyncio | Unconditional module-level `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine` for the `execute_with_connection` helper, imported by all three repository modules |
| `repositories/_json.py` | sqlalchemy (only by package association) | Deprecated shim re-exporting `EventSourceJSONEncoder`/`json_dumps`/`json_loads` from `eventsource.serialization` via a lazy module `__getattr__`. Its own imports are stdlib (`warnings`, `typing`) with the serialization names under `TYPE_CHECKING`, so the file carries no infrastructure dependency -- but it is only reachable through `eventsource.repositories`, whose `__init__.py` pulls sqlalchemy. Slated for removal; do not count it as a Tier 0 module. |
| `readmodels/postgresql.py` | sqlalchemy, asyncpg | PostgreSQL read model repository. Module-level `from sqlalchemy import text` and `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine` (lines 12-13), plus `repositories/_connection` (line 29), which is itself sqlalchemy-backed. |
| `readmodels/sqlite.py` | aiosqlite | SQLite read model repository. No sqlalchemy: it drives `aiosqlite` connections directly, and the `import aiosqlite` sits under `TYPE_CHECKING` (line 37). Per boundary rule 2, a `TYPE_CHECKING` guard does not exempt a module -- the runtime contract is an `aiosqlite.Connection`, so this is a backend module, not a contract module. |
| `readmodels/projection.py` | sqlalchemy (transitive via `projections/base.py`, `repositories/checkpoint.py`, `repositories/dlq.py`) | `ReadModelProjection`. Its own imports are otherwise Tier 0, but lines 20-23 are module-level `from eventsource.projections.base import DatabaseProjection`, `from eventsource.repositories.checkpoint import CheckpointRepository`, and `from eventsource.repositories.dlq import DLQRepository` -- three separate paths to the same sqlalchemy pull. The `AsyncConnection`/`AsyncSession` imports at line 26 are `TYPE_CHECKING`-only and are not the cause. |
| `repositories/__init__.py` | sqlalchemy (transitively) | Eagerly re-exports all three repository modules, so `import eventsource.repositories` pulls sqlalchemy even when the caller only wants the InMemory names |
| `projections/base.py` | sqlalchemy (transitive via repositories) | Projection base classes. Imports `InMemoryCheckpointRepository` from `repositories/checkpoint.py` and `InMemoryDLQRepository` from `repositories/dlq.py` at module level (lines 43-47) -- both files import sqlalchemy at their own module level. It also imports `checkpoint_manager` and `dlq_manager`, which pull the same two repositories independently. |
| `projections/checkpoint_manager.py` | sqlalchemy (transitive via `repositories/checkpoint.py`) | `ProjectionCheckpointManager`. Module-level `from eventsource.repositories.checkpoint import CheckpointRepository, InMemoryCheckpointRepository` (line 35) -- the in-memory name alone is enough to require sqlalchemy. |
| `projections/dlq_manager.py` | sqlalchemy (transitive via `repositories/dlq.py`) | `ProjectionDLQManager`. Module-level `from eventsource.repositories.dlq import DLQEntry, DLQRepository, InMemoryDLQRepository` (line 33) -- same shape as the checkpoint manager. |
| `testing/harness.py` | sqlalchemy (transitive via `repositories/checkpoint.py` + `repositories/dlq.py`) | `InMemoryTestHarness`. Module-level `from eventsource.repositories.checkpoint import InMemoryCheckpointRepository` (line 29) and `from eventsource.repositories.dlq import InMemoryDLQRepository` (line 30). Every other import in the file is Tier 0 (`bus/memory`, `events/base`, `stores/in_memory`) -- these two lines are the sole cause. |
| `testing/bdd.py` | sqlalchemy (transitive via `testing/harness.py`) | Given-When-Then helpers. Module-level `from eventsource.testing.harness import InMemoryTestHarness` (line 49); its other imports (`aggregates/base`, `events/base`) are Tier 0. It carries no infrastructure dependency of its own -- it inherits the harness's. |
| `testing/__init__.py` | sqlalchemy (transitively) | Eagerly re-exports `InMemoryTestHarness` from `harness.py` and the BDD helpers from `bdd.py` alongside the Tier 0 names, so the whole package namespace is tainted: `from eventsource.testing import EventBuilder` imports sqlalchemy even though `builder.py` itself is Tier 0. |
| `subscriptions/` | sqlalchemy (transitive via stores, repositories) | Subscription lifecycle management |
| `migration/` | sqlalchemy | Live migration tooling |
| `locks/` | sqlalchemy | Distributed locking |

Two of these rows are entire packages tainted by a single seam rather than by their own code. `readmodels/` and `testing/` each split down the middle: the contract and in-memory halves are Tier 0 (see the previous table), while the backend halves land here. `readmodels/projection.py` is the only one of the three that is *not* blocked by a driver import of its own -- it is blocked purely by the unsplit repository modules, so it moves the moment those are split.

## Boundary findings

1. **Three of the four `projections/` modules are NOT Tier 0, not just `base.py`.** `projections/base.py` imports `repositories/checkpoint.py` (lines 43-46) and `repositories/dlq.py` (line 47), both of which import sqlalchemy at module level. But the same pull happens twice more, independently: `projections/checkpoint_manager.py:35` imports `CheckpointRepository` and `InMemoryCheckpointRepository`, and `projections/dlq_manager.py:33` imports `DLQEntry`, `DLQRepository`, and `InMemoryDLQRepository`. `base.py` imports both managers (lines 36-37) as well, so the dependency is redundant rather than layered -- removing the repository imports from `base.py` alone would not make it Tier 0, because the managers would still drag sqlalchemy in behind it. The three modules have to be fixed as a set. The projection *protocols* (`projections/protocols.py`) are clean -- they only re-export from `protocols.py`.

2. **All three repository modules still mix interface and implementation in one file, and this remains open.** This is not a checkpoint-and-DLQ quirk -- it is the uniform shape of `repositories/`, and no split has landed yet (`repositories/checkpoint.py:18-19` and `repositories/dlq.py:18-19` still carry the sqlalchemy imports today; see "Open cleanup before extraction"). `checkpoint.py`, `dlq.py`, and `outbox.py` each pack four layers into a single module: the `@runtime_checkable` Protocol (`CheckpointRepository` at line 77, `DLQRepository` at line 107, `OutboxRepository` at line 96), the data-transfer dataclasses (`CheckpointData`/`LagMetrics`; `DLQEntry`/`DLQStats`/`ProjectionFailureCount`; `OutboxEntry`/`OutboxStats`), the stdlib-only in-memory implementation (`InMemoryCheckpointRepository` line 511, `InMemoryDLQRepository` line 642, `InMemoryOutboxRepository` line 497), and both backend implementations (PostgreSQL and SQLite). Every one of the three opens with the same two module-level lines -- `from sqlalchemy import text` and `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine` -- followed by `from eventsource.repositories._connection import execute_with_connection`. The consequence is identical in all three cases: importing the Protocol, a dataclass, or the in-memory class forces sqlalchemy, because the import is at the top of the file the name lives in. Splitting interface + in-memory from the backend halves is a three-module job, not a two-module one, and it is what makes the contracts and in-memory variants Tier 0 eligible.

   This finding is the shared root cause behind both finding 1 and finding 3, which is why it is worth reading as a structural fact rather than a list of two files. The projection side and the testing side reach into `checkpoint.py` and `dlq.py` for *different* names -- `projections/checkpoint_manager.py:35` and `projections/dlq_manager.py:33` want the Protocols (`CheckpointRepository`, `DLQRepository`) and their dataclasses, while `testing/harness.py:29-30` wants only the in-memory classes (`InMemoryCheckpointRepository`, `InMemoryDLQRepository`). Neither side wants anything sqlalchemy-backed. Both get sqlalchemy anyway, because all four names live in files whose first two non-stdlib imports are `from sqlalchemy import text` and `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine` (`checkpoint.py:18-19`, `dlq.py:18-19`).

   The practical consequence is that these are not three independent cleanups. Splitting `checkpoint.py` and `dlq.py` clears the blocker for `projections/` (three modules), for `testing/harness.py` and `testing/bdd.py` and the `testing/__init__.py` namespace behind them, and for `readmodels/projection.py` -- all at once, and none of those modules needs an edit of its own beyond re-pointing an import. Conversely, no amount of work inside `projections/` or `testing/` fixes any of them while the two repository files stay merged: there is no import either package could write that reaches a Protocol or an in-memory class without loading the sqlalchemy at the top of the same file.

3. **`testing/` is not one tier -- it splits three ways, and the package `__init__` sits on the wrong side of the split.** Read per module rather than per package:

   - `testing/harness.py` is the only file in the package with a non-Tier-0 import of its own, and it has exactly two: `from eventsource.repositories.checkpoint import InMemoryCheckpointRepository` (line 29) and `from eventsource.repositories.dlq import InMemoryDLQRepository` (line 30). Both are module level, both target the defining module rather than the `repositories` package `__init__`, and both pull sqlalchemy from those files' own import blocks. Everything else it imports -- `bus/memory`, `events/base`, `stores/in_memory` (lines 27, 28, 31) -- is Tier 0.
   - `testing/bdd.py` has no infrastructure import at all. Its single non-Tier-0 line is `from eventsource.testing.harness import InMemoryTestHarness` (line 49); its other in-library imports are `aggregates/base` and `events/base` (lines 47-48). It is out of Tier 0 purely by inheritance from the harness.
   - `testing/builder.py`, `testing/assertions.py`, and `testing/conformance.py` are Tier 0 as they stand today, with no pending change. `builder.py` (line 23) and `assertions.py` (line 27) each import exactly one in-library name, `eventsource.events.base.DomainEvent`. `conformance.py` imports four, all of them contracts: `bus/interface`, `events/base`, `exceptions`, `stores/interface` (lines 27-30). None of the three names a repository, a harness, or a backend.

   The distinction matters because the fix is not uniform. The harness needs its two imports re-pointed once `checkpoint.py` and `dlq.py` are split (finding 2); `bdd.py` needs nothing; the other three already comply. Treating `testing/` as a single "almost Tier 0" unit hides the fact that three fifths of it is already clean.

### Why this matters to users specifically

This is the one boundary finding whose cost lands on library consumers rather than on maintainers. The shipped test toolkit is precisely the surface people want without a database: `EventBuilder` constructs event instances, `EventAssertions` inspects them, and the conformance suites exercise an `EventStore` or `EventBus` implementation through its interface. All three are pure-Python and all three are Tier 0 on their own. A downstream team writing domain tests against in-memory infrastructure has a legitimate expectation that importing them costs nothing beyond pydantic.

It does not. `testing/__init__.py` eagerly imports every submodule to build its re-export list -- `assertions` (line 33), `bdd` (line 34), `builder` (line 42), `conformance` (line 43), and `harness` (line 47) -- so `from eventsource.testing import EventBuilder` executes `harness.py`, which executes `repositories/checkpoint.py` and `repositories/dlq.py`, which import sqlalchemy. The name being imported is Tier 0; the import statement is not. Nothing in the `EventBuilder` call path touches a repository, and the user has no way to tell from the import line that it will.

That is why finding 2 is worth prioritizing on user-facing grounds and not only architectural ones: the harness taint is a two-line defect in one file, but its blast radius is every import of the public testing namespace.

4. **`observability/` is Tier 0.** It guards opentelemetry behind optional imports with no-op fallbacks. No sqlalchemy or redis.

5. **`readmodels/` splits cleanly along the same seam as `repositories/` -- but the split is already made for it.** Where `repositories/` packs contract, in-memory, and backend into one file per concern, `readmodels/` already has them in separate files, and the tier line falls exactly on that existing seam. `base.py`, `query.py`, `schema.py`, `repository.py`, `in_memory.py`, and `exceptions.py` are Tier 0; `postgresql.py`, `sqlite.py`, and `projection.py` are not. That means `readmodels/` needs no refactor to extract -- it needs only the package `__init__` to stop eagerly re-exporting the backend half, the same taint described for `testing/__init__.py`. The one exception is `readmodels/projection.py`, which is blocked not by a driver import of its own but by `projections/base.py`, `repositories/checkpoint.py`, and `repositories/dlq.py` -- so it is gated on finding 2, not on anything in `readmodels/`.

6. **`testing/conformance.py` is Tier 0 today, and it is the reference contract-test surface for third-party backends.** Its entire import block is `abc`, `uuid`, and four Tier 0 in-library modules -- `bus/interface`, `events/base`, `exceptions`, and `stores/interface` (lines 24-30). It reaches no repository, no harness, and no backend. This matters more than one row in the table: `EventStoreConformanceSuite` and `EventBusConformanceSuite` are the ABCs an out-of-tree backend subclasses to prove it satisfies the `EventStore` / `EventBus` contract, so they must travel with the contracts they test. A `eventsource-core` that shipped the interfaces without the conformance suites would leave third-party implementers with a contract they cannot verify against. Keeping this module Tier 0 is a constraint on future edits to it, not just an observation: any import added here that pulls sqlalchemy would break the extraction story for every downstream backend. Note that the module is Tier 0 only when imported directly -- `from eventsource.testing import ...` still pulls sqlalchemy through the package `__init__` (see the previous table).

7. **`repositories/_connection.py` is a shared unconditional sqlalchemy import, and it has consumers outside `repositories/`.** The module is 65 lines and exists only to host `execute_with_connection`, an `@asynccontextmanager` that normalizes an `AsyncConnection | AsyncEngine` argument down to a connection. Its import block is `collections.abc`, `contextlib`, and -- unguarded, at module level -- `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine` (line 16). There is no `TYPE_CHECKING` guard and no fallback, so the module is unimportable without sqlalchemy installed and taints every importer.

   This is why splitting the three repository modules is necessary but not sufficient. The split only works if the *backend* halves are the sole consumers of the helper -- and they nearly are, but not quite. Inside `repositories/`, the helper is used exclusively by the PostgreSQL implementations: all 25 call sites across `checkpoint.py`, `dlq.py`, and `outbox.py` fall between the `PostgreSQL*Repository` class and the `InMemory*Repository` class that follows it (e.g. `checkpoint.py` lines 246-494, with `InMemoryCheckpointRepository` starting at line 511). Neither the in-memory nor the SQLite classes call it. So for the three modules the seam is clean: move the helper import with the PostgreSQL half and the interface + in-memory half loses its last sqlalchemy edge.

   The complication is the five importers outside `repositories/`: `readmodels/postgresql.py:29`, and all four migration repositories -- `migration/repositories/migration.py:66`, `position_mapping.py:65`, `routing.py:72`, and `audit_log.py:64`. Every one is already a non-Tier-0 backend module, so none of them *blocks* extraction. But they mean the helper cannot simply be inlined into a single `repositories/postgresql.py` and forgotten. It has to land somewhere the migration and read-model backends can still reach it -- either a backend-only subpackage under `repositories/`, or a shared infrastructure module outside Tier 0 that all eight importers point at. Choosing the destination is part of the split, not a follow-up to it.

   Note also that `tests/unit/test_connection_helper.py` imports the helper directly (line 16) and is 316 lines of coverage over it; whichever path the helper ends up at, that import moves with it.

8. **`exceptions.py` is the only Tier 0 *module* with no eventsource-internal dependency at all, which is why the rules are calibrated against it.** *(Updated 2026-07-28: this finding previously named `serialization/` as the package meeting this bar. `orjson` becoming a core dependency of `serialization/json.py` moved that package out of Tier 0 entirely -- see "Modules NOT in Tier 0." `exceptions.py` is the replacement reference case, and it is a single module rather than a package, since `serialization/` no longer has a Tier 0 sibling to stand in for it.)* Every other Tier 0 entry in the table above reaches somewhere -- most commonly to `events/base`, and through it to pydantic. `exceptions.py` does not: it imports `uuid` and nothing else. A couple of other individual modules match that on their own -- `config.py` (no imports at all, still a placeholder) and `readmodels/query.py` (`dataclasses`, `typing`) -- but each is one file inside a top-level namespace or a package whose siblings are not Tier 0, so none of them is an extractable unit by itself in the same clean sense. `exceptions.py` is, and that is what makes it the reference case: when a rule below has to say what "Tier 0" means at its strictest, this is the module it is describing, and the pydantic allowance in rule 1 exists precisely because most Tier 0 modules cannot meet the bar `exceptions.py` sets.

9. **`protocols.py` is Tier 0 but not standalone -- `events/base.py` is a hard floor beneath it.** The module advertises itself as the canonical type-contract home, and `docs/api/index.md` names it as one of only two modules that are both re-exported and importable directly "because they are canonical homes." That framing invites the assumption that the protocols can be lifted on their own. They cannot. `src/eventsource/protocols.py:35` is a plain module-level `from eventsource.events.base import DomainEvent` -- not under `if TYPE_CHECKING:`, not deferred into a function -- so importing `eventsource.protocols` executes `eventsource.events.base` and, through it, pydantic. The same shape holds for `stores/interface.py:24`, `handlers/decorators.py:20`, and `bus/interface.py:16-17` (which imports `events.base` *and* `protocols`). Every one of these is Tier 0 by the dependency test and non-extractable by itself. The consequence for an `eventsource-core` split is concrete: `events/` and `protocols.py` are a single shipping unit, and `exceptions.py` and `types.py` come with them for the aggregate and handler contracts. A distribution containing `protocols.py` without `events/` would fail to import. See "Module-level import coupling within Tier 0" above for the full floor-by-module table, including the two modules that break the pattern (`snapshots/interface.py`, which has no `eventsource` imports at all, and `events/registry.py`, whose coupling is `TYPE_CHECKING`-only).

10. **`config.py` is an empty placeholder that the documentation describes as a real subsystem.** The file is seven lines total: a five-line docstring saying "This module will contain configuration classes for various components. Placeholder for future tasks.", a blank line, and one trailing comment. Zero imports, zero classes, zero functions, no `__all__`. Nothing under `src/` or `tests/` imports it. Its Tier 0 status is vacuous -- a file with no import statements passes the boundary test without asserting anything about layering -- so it should not be counted as evidence that the core surface is in good shape. What makes it a finding rather than a curiosity is the gap between the file and its documentation: `docs/api/index.md:67` lists `eventsource.config` among the public subsystems that ship without top-level re-export, and `docs/api/index.md:780` describes it in the module table as "Configuration helpers." Anyone drawing the extraction boundary from the API docs would treat it as a contract core consumers depend on. It is not one, and moving it into a core package would ship an empty file. (Do not confuse it with `eventsource/subscriptions/config.py`, which is a real, populated module.) Resolve it -- populate or delete -- before extraction; see "Empty and placeholder modules on the advertised surface" above for the options and the trade-offs.

11. **`repositories/__init__.py` is a boundary blocker in its own right, independent of the three modules it re-exports.** The package `__init__` is 101 lines, and every line of executable code in it is an eager `from ... import` at module scope: `checkpoint` (lines 33-41), `dlq` (lines 44-53), `outbox` (lines 56-64), and `eventsource.serialization` (lines 65-69), followed by a flat 30-name `__all__`. There is no `__getattr__`, no `TYPE_CHECKING` guard, no deferred import anywhere in the file. Python executes the package `__init__` before it can bind any submodule, so *any* touch of the `eventsource.repositories` namespace runs all three module bodies and therefore imports sqlalchemy.

    Two consequences follow, and they are the reason this finding is separate from finding 2. First, the split described there is necessary but does not by itself unblock the package: even after `checkpoint.py`, `dlq.py`, and `outbox.py` are cut into interface + in-memory and backend halves, an `__init__` that still eagerly re-exports `PostgreSQLCheckpointRepository`, `SQLiteDLQRepository`, and their six siblings re-imports sqlalchemy on the next line. The `__init__` has to change too -- either narrowed to the interface + in-memory names with the backends reachable only via their own submodule paths, or made lazy with a module-level `__getattr__`. The repository already contains a working precedent for the lazy shape: `repositories/_json.py` is a 44-line deprecation shim whose entire implementation is a `__getattr__` (line 26) forwarding to `eventsource.serialization`.

    Second, the eager re-export already taints names that are otherwise cheaper to reach directly. Lines 65-69 pull `EventSourceJSONEncoder`, `json_dumps`, and `json_loads` out of `serialization/` and re-export them at lines 98-100. *(Updated 2026-07-28: `serialization/` is no longer Tier 0 -- see "Modules NOT in Tier 0" -- but it still has no `eventsource`-internal dependency and no sqlalchemy/redis, so it remains true that reaching these names via `eventsource.serialization` costs strictly less than via `eventsource.repositories`, just no longer "zero" in the stdlib-only sense.)* Reaching them through `eventsource.repositories` costs sqlalchemy on top of orjson; reaching them through `eventsource.serialization` costs only orjson. The same trap catches the `_json` shim, since `from eventsource.repositories import _json` (exercised at `tests/unit/serialization/test_json.py:184` and `:194`) must execute the parent `__init__` first.

    Note that the top-level package does the same thing one level up: `src/eventsource/__init__.py:123-138` imports 17 names from `eventsource.repositories`, including the three PostgreSQL classes. So `import eventsource` requires sqlalchemy regardless of what the caller wants -- narrowing `repositories/__init__.py` without also revisiting the top-level re-export list leaves the practical import cost unchanged for anyone using the documented `from eventsource import ...` entry point.

## Boundary rules for Tier 0

1. **Allowed dependencies**: stdlib, pydantic, typing-extensions. (Pydantic is *permitted*, not *required* -- a Tier 0 module that needs none of it should import none of it. `exceptions.py` is the pydantic-free case: its whole import surface is `uuid`, and it stays that way. `serialization/` was this document's previous example of the pydantic-free case; it no longer qualifies, having taken on `orjson` as a core dependency -- see "Modules NOT in Tier 0.")
2. **No sqlalchemy imports** -- not even behind `TYPE_CHECKING`. Tier 0 modules must be importable without sqlalchemy installed.
3. **Optional deps must be guarded**: opentelemetry is acceptable if behind `try/except ImportError` with no-op fallback.
4. **In-memory implementations belong in Tier 0**: They implement the interface contracts using only stdlib. They are essential for testing without infrastructure.
5. **Interface + implementation separation**: Files that define both a Protocol/ABC and a sqlalchemy-backed implementation must be split before the interface can move to Tier 0.
6. **Tier 0 membership travels with the module's own transitive Tier 0 imports.** Passing the dependency test is not the same as being extractable. If a module imports another `eventsource` module at import time, that module is part of its extraction unit and must move with it -- there is no partial ship. `protocols.py` is the worked example: `src/eventsource/protocols.py:35` is a plain module-level `from eventsource.events.base import DomainEvent`, so a distribution containing `protocols.py` without `events/` fails to import. When claiming a module for Tier 0, close its module-level `eventsource` import set transitively and confirm every member is also Tier 0 and also in scope for the move. Imports under `if TYPE_CHECKING:` do not create this obligation (they are typing-time only, as in `events/registry.py:43-44`) -- but note this is the opposite of rule 2, where a `TYPE_CHECKING` guard does *not* excuse a sqlalchemy import. The two rules ask different questions: rule 2 is about what must not be installed, this rule is about what must ship together.
7. **Placeholder and empty modules do not count as satisfied surface.** A module qualifies for Tier 0 only by *asserting* something about layering, not by containing too little code to violate it. `src/eventsource/config.py` is seven lines of docstring and comment with zero imports, zero classes, and no importer anywhere in `src/` or `tests/`; it passes rules 1-3 vacuously. Do not treat such a module as a satisfied contract, do not cite it as evidence the core surface is healthy, and do not carry it into an extracted package on the strength of its name or its API-doc description. Resolve it first -- populate it, or delete it and its documentation references -- so the boundary is drawn around code that exists.

## Recommended pre-extraction cleanup

These are tracked, still-open items. Nothing below has landed yet -- each claim is stated against the file as it exists today. Steps 1-3 are the same operation applied three times; steps 4 and 5 are the parts that are easy to miss, and skipping either of them leaves the boundary where it started.

1. **Split `repositories/checkpoint.py` into an interface + in-memory module and backend modules for postgresql and sqlite.** **Still open.** The Tier 0 half is the `CheckpointRepository` Protocol (line 77), the `CheckpointData` (line 33) and `LagMetrics` (line 55) dataclasses, and `InMemoryCheckpointRepository` (line 511) -- none of which touches a database. The backend half is `PostgreSQLCheckpointRepository` (line 191) and `SQLiteCheckpointRepository` (line 735). Today they share one 1102-line file whose lines 18-19 are:

   ```python
   from sqlalchemy import text
   from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
   ```

   followed by `from eventsource.repositories._connection import execute_with_connection` (line 26). Because these sit at module level, importing the Protocol or the in-memory class -- neither of which needs a database -- requires sqlalchemy. This is exactly what boundary rule 5 forbids.

2. **Split `repositories/dlq.py` the same way.** **Still open.** Interface + in-memory half: the `DLQRepository` Protocol (line 107), the `DLQEntry` (line 35), `DLQStats` (line 71) and `ProjectionFailureCount` (line 89) dataclasses, and `InMemoryDLQRepository` (line 642). Backend half: `PostgreSQLDLQRepository` (line 259) and `SQLiteDLQRepository` (line 997). Lines 18-19 are identical to the checkpoint module's, with `execute_with_connection` imported at line 30.

3. **Split `repositories/outbox.py` the same way.** **Still open.** Interface + in-memory half: the `OutboxRepository` Protocol (line 96), the `OutboxEntry` (line 42) and `OutboxStats` (line 76) dataclasses, and `InMemoryOutboxRepository` (line 497). Backend half: `PostgreSQLOutboxRepository` (line 198) and `SQLiteOutboxRepository` (line 754). The import block matches its two siblings -- `from sqlalchemy import text` and `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine` at lines 21-22, `execute_with_connection` at line 34.

   Outbox differs from the other two only in who is downstream of it: nothing in `projections/`, `testing/`, or `readmodels/` imports it, and the sole in-tree mention outside `repositories/` is a docstring line at `bus/kafka.py:28`. So it does not block the payoff in step 6. Do it in the same pass anyway -- three modules with one shape are one refactor, and leaving the third behind is how the shape grows back.

4. **Confine `_connection.py` to the backend-only subpackage.** **Still open.** The helper module is 65 lines and exists only to host `execute_with_connection`, but line 16 is an unguarded module-level `from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine`. Steps 1-3 strip sqlalchemy from the interface halves only if those halves stop importing this module -- which they can, because inside `repositories/` the helper is called exclusively from the PostgreSQL classes, never from the in-memory or SQLite ones.

   The constraint is the five importers outside `repositories/`: `readmodels/postgresql.py:29`, `migration/repositories/migration.py:66`, `migration/repositories/position_mapping.py:65`, `migration/repositories/routing.py:72`, and `migration/repositories/audit_log.py:64`. All five are already non-Tier-0 backend modules, so none of them blocks extraction -- but they do mean the helper cannot simply be inlined into one `repositories/postgresql.py` and deleted. Pick a destination all eight importers can still reach: either a backend-only subpackage under `repositories/` or a shared non-Tier-0 infrastructure module. Whichever path is chosen, `tests/unit/test_connection_helper.py` moves with it -- it imports the helper at line 16 and patches `"eventsource.repositories._connection.isinstance"` by dotted string at eight further call sites, so a bare move breaks those patches unless the strings are updated too.

5. **Reduce the eager re-exports in `repositories/__init__.py`.** **Still open.** All executable code in the 101-line package `__init__` is eager module-scope imports -- `checkpoint` (lines 33-41), `dlq` (lines 44-53), `outbox` (lines 56-64), `eventsource.serialization` (lines 65-69) -- followed by a flat 30-name `__all__`. Python runs this file before it can bind any submodule, so it re-imports sqlalchemy even after steps 1-4 land, for as long as it keeps naming `PostgreSQLCheckpointRepository`, `SQLiteDLQRepository`, and their four siblings.

   Two viable shapes. Narrow it: keep only the Protocol, dataclass, and in-memory names eager, and let callers reach the six backend classes through their own submodule paths. Or make it lazy: a module-level `__getattr__` that resolves backend names on first access, following the precedent already in the tree at `repositories/_json.py:26`. Either way, also drop the `serialization` re-export at lines 65-69 -- `EventSourceJSONEncoder`, `json_dumps`, and `json_loads` are reachable from `eventsource.serialization` at the cost of `orjson` alone (no longer stdlib-only, per the correction above, but still far lighter than pulling sqlalchemy through `eventsource.repositories`).

   Do not stop at the subpackage. `src/eventsource/__init__.py:123-138` imports 17 names from `eventsource.repositories`, three of them PostgreSQL classes, so `import eventsource` pulls sqlalchemy regardless of what the caller wants. Narrowing the subpackage `__init__` without revisiting the top-level list leaves the practical import cost unchanged for anyone using the documented `from eventsource import ...` entry point.

6. **The payoff: after the split, `projections/base.py` and `testing/` become Tier 0 eligible.** Neither is blocked by anything except the steps above, and neither needs an edit beyond the import lines themselves -- every name they reach for is a Protocol, a dataclass, or an in-memory class, and all of those land on the Tier 0 side of the split.

   | Module | Import to re-point | Names it actually wants |
   |--------|--------------------|-------------------------|
   | `projections/base.py` | lines 43-47 | `CheckpointRepository`, `InMemoryCheckpointRepository`, `DLQRepository`, `InMemoryDLQRepository` |
   | `projections/checkpoint_manager.py` | line 35 | `CheckpointRepository`, `InMemoryCheckpointRepository` |
   | `projections/dlq_manager.py` | line 33 | `DLQEntry`, `DLQRepository`, `InMemoryDLQRepository` |
   | `testing/harness.py` | lines 29-30 | `InMemoryCheckpointRepository`, `InMemoryDLQRepository` |

   `testing/harness.py` is the cleanest case: its only non-Tier-0 imports are `from eventsource.repositories.checkpoint import InMemoryCheckpointRepository` (line 29) and `from eventsource.repositories.dlq import InMemoryDLQRepository` (line 30). Because both target the defining module rather than the package `__init__`, step 5 is not even a prerequisite for it. Everything else it imports -- `bus/memory`, `events/base`, `stores/in_memory` -- is already Tier 0. Fixing those two lines also unblocks `testing/bdd.py` and the `testing/__init__.py` namespace, both of which inherit the harness's dependency.

## Verifying the boundary

The tables above are a snapshot. Every claim in them was produced by the checks below, and those checks are how you re-derive a module's tier after any change -- do not trust the table over a fresh grep.

### The check: module-level infrastructure imports

A module is disqualified from Tier 0 by an import that runs at import time. The direct check is a grep anchored at column zero, which is what "module level" means in an import block:

```bash
grep -rlE '^(from|import) (sqlalchemy|redis|asyncpg|aiosqlite|aiokafka|aio_pika)' \
  src/eventsource/ --include='*.py' | sort
```

Run against the tree today, this returns exactly thirteen files:

```
src/eventsource/locks/postgresql.py
src/eventsource/migration/repositories/audit_log.py
src/eventsource/migration/repositories/migration.py
src/eventsource/migration/repositories/position_mapping.py
src/eventsource/migration/repositories/routing.py
src/eventsource/readmodels/postgresql.py
src/eventsource/repositories/_connection.py
src/eventsource/repositories/checkpoint.py
src/eventsource/repositories/dlq.py
src/eventsource/repositories/outbox.py
src/eventsource/snapshots/postgresql.py
src/eventsource/stores/postgresql.py
src/eventsource/stores/sqlite.py
```

Every other non-Tier-0 module in this document is disqualified *transitively* -- it imports one of these thirteen, or something that does. So the grep is a first pass, not the whole answer: a clean result means "not directly disqualified," and you still have to walk the module's own in-library imports and confirm each one is Tier 0.

### TYPE_CHECKING guards do not exempt a module

Boundary rule 2 says no sqlalchemy imports, *not even behind `TYPE_CHECKING`*. This is the rule people get wrong most often, because a guarded import genuinely does not execute at runtime and the module will import fine with the dependency absent. It is still not Tier 0.

The reason is that the guard hides the dependency without removing it. If a method signature is annotated `connection: AsyncConnection`, the runtime contract is a sqlalchemy object regardless of when the name is resolved -- the module is a backend adapter that happens to defer a symbol lookup. A Tier 0 module has to be usable, not merely importable, without the infrastructure library.

Two consequences for how you run the check:

- **Do not relax the anchor to `^\s*`.** A leading-whitespace regex sweeps in guarded and function-local imports, and its output mixes genuine module-level violations with indented ones. Run the anchored grep for disqualification, then run the indented variant separately as a review list, not as a verdict:

  ```bash
  grep -rnE '^[ \t]+(from|import) (sqlalchemy|redis|asyncpg|aiosqlite|aiokafka|aio_pika)' \
    src/eventsource/ --include='*.py'
  ```

  Today that surfaces `readmodels/sqlite.py:37` (`import aiosqlite`), `readmodels/projection.py:26`, `snapshots/sqlite.py:33`, `bus/redis.py:71-74`, `bus/kafka.py:123-125`, `bus/rabbitmq.py:73-75`, `repositories/checkpoint.py:29`, `repositories/outbox.py:38`, and `projections/base.py:973,979`. None of those lines runs at import time; none of those modules is Tier 0 either.

- **`readmodels/sqlite.py` is the worked example.** It does not appear in the anchored grep at all -- it uses no sqlalchemy and imports `aiosqlite` only under `TYPE_CHECKING`. It is still a backend module, because its public methods take an `aiosqlite.Connection`. Judge by what the module requires of its caller, not by what its import block executes.

### Why an import-time test does not work today

The tempting automated check -- install a `sys.meta_path` finder that raises on `sqlalchemy`, then `importlib.import_module` the target -- currently fails for *every* module in the package, including ones this document lists as Tier 0. The chain is in the package initializers, not the modules:

```
eventsource/__init__.py:24
  -> eventsource/aggregates/__init__.py:7
     -> eventsource/aggregates/repository.py:22
        -> eventsource/snapshots/__init__.py:87
           -> eventsource/snapshots/postgresql.py:15  (import sqlalchemy)
```

Importing `eventsource.testing.conformance` runs `eventsource/__init__.py` first, so the blocker fires before the target module is ever reached. Note the middle of that chain: `aggregates/repository.py` imports `eventsource.snapshots.strategies` (Tier 0) and pulls the concrete `Snapshot`/`SnapshotStore` names only under `TYPE_CHECKING` -- it is the *package* `snapshots/__init__.py` eagerly re-exporting `PostgreSQLSnapshotStore` that drags sqlalchemy in. This is the same package-taint pattern already described for `repositories/__init__.py`, `testing/__init__.py`, and `readmodels/`.

So: **tier is a property of a module, not of the package that contains it**, and until the package initializers are made lazy, static grep plus manual import-walking is the only reliable verification. An import-time test becomes the authoritative check the moment those initializers stop eagerly importing backends -- and adding one is a reasonable acceptance criterion for that work.

### Checklist for a single module

1. Run the anchored grep on the file. Any hit: not Tier 0, stop.
2. Read its import block. For each `from eventsource.X import ...` at module level, confirm `X` is Tier 0 per the tables above -- recursing if you do not already know. Watch for package imports (`from eventsource.snapshots import ...`), which pull that package's `__init__.py` and everything it re-exports, not just the name you asked for.
3. Check the `TYPE_CHECKING` block and the method signatures. If a public signature's runtime contract is an infrastructure object, the module is a backend adapter regardless of step 1.
4. Optional deps are acceptable only in the `try/except ImportError` with no-op fallback shape that `observability/` uses (boundary rule 3). A bare guarded import is not that shape.
