# API Reference

Reference documentation for the public API of `eventsource-py`: the names exported from
the top-level `eventsource` package, the modules behind them, the optional backends and
their availability flags, and the shared exception and type-alias vocabulary.

The distribution is named `eventsource-py` and is imported as `eventsource`:

```python
import eventsource

print(eventsource.__version__)  # "0.5.0" for the current release
```

`__version__` is resolved at import time from installed distribution metadata via
`importlib.metadata.version("eventsource-py")`, falling back to `"0.0.0.dev0"` when the
package is used from a source checkout that has not been installed.

The core dependencies are `pydantic` and `sqlalchemy`. Everything else — PostgreSQL,
SQLite, Redis, RabbitMQ, Kafka, OpenTelemetry — is an optional extra, and the modules
wrapping those drivers remain importable whether or not the driver is present.

This page is descriptive, not instructional: it states what exists and how it behaves.
For a guided introduction start with the tutorials; for task-oriented recipes see the
how-to guides.

## Scope and Conventions

This page documents the **top-level barrel** — the names bound by
`src/eventsource/__init__.py` and listed in its `__all__` — plus the conventions that
apply uniformly across the reference pages: how optional backends are exposed, and what
shape the public methods take.

### Public Import Surface (`from eventsource import ...`)

`eventsource.__all__` is the supported public surface. It contains 130 names when the
SQLite extra is absent and 133 when it is present (`SQLITE_AVAILABLE`, `SQLiteEventStore`,
and `SQLiteOutboxRepository` are the only names conditionally appended). The barrel is
deliberately flat —
`from eventsource import DomainEvent, AggregateRoot, InMemoryEventStore` is the idiomatic
import style, and the module paths underneath (`eventsource.events.base`,
`eventsource.domain.aggregate`, `eventsource.stores.in_memory`) are where those names are
defined rather than where callers are expected to reach.

The exported names fall into these groups:

| Group | Representative names |
| --- | --- |
| Version metadata | `__version__` |
| Type aliases | `TState`, `AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId` |
| Events and registry | `DomainEvent`, `EventRegistry`, `default_registry`, `register_event`, `get_event_class`, `get_event_class_or_none`, `is_event_registered`, `list_registered_events`, `EventTypeNotFoundError`, `DuplicateEventTypeError` |
| Event store interface | `EventStore`, `EventPublisher`, `EventStream`, `AppendResult`, `StoredEvent`, `ReadOptions`, `ReadDirection`, `ExpectedVersion` |
| Event store implementations | `InMemoryEventStore`, `PostgreSQLEventStore`, plus `SQLiteEventStore` when available |
| Aggregates | `AggregateRoot`, `DeclarativeAggregate`, `AggregateRepository`, `handles` |
| Event bus | `EventBus`, `EventHandlerFunc`, `InMemoryEventBus`, and the Redis, RabbitMQ, and Kafka bus classes with their `*Config` and `*Stats` types |
| Handler protocols | `EventHandler`, `AsyncEventHandler`, `SyncEventHandler`, `FlexibleEventHandler`, `EventSubscriber`, `FlexibleEventSubscriber` |
| Projections and read models | `Projection`, `CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection`, `ReadModelProjection` |
| Repository infrastructure | checkpoint, DLQ, and outbox protocols with their in-memory and PostgreSQL implementations and data types |
| Snapshots | `Snapshot`, `SnapshotStore`, `InMemorySnapshotStore`, and the four snapshot exceptions (`SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError`) |
| Multi-tenancy | `TenantDomainEvent`, `tenant_context`, `tenant_scope`, `tenant_scope_sync`, the context accessors, and the tenant exceptions |
| Sync adapters | `SyncEventStoreAdapter` |
| Serialization | `EventSourceJSONEncoder` |
| Exceptions | `EventSourceError` and its subclasses |
| Availability flags | `REDIS_AVAILABLE`, `KAFKA_AVAILABLE`, `RABBITMQ_AVAILABLE`, and `SQLITE_AVAILABLE` when the extra is installed |

Several public subsystems ship in the package but are **not** re-exported at the top
level. They are imported from their own modules: `eventsource.testing`,
`eventsource.subscriptions`, `eventsource.observability`, `eventsource.migration`,
`eventsource.locks`, `eventsource.gdpr`, `eventsource.config`, and the raw SQL under
`eventsource.migrations`.

A few names are also narrower at the barrel than in their defining adapter package.
Snapshots are the clearest case: `PostgreSQLSnapshotStore` (`eventsource.adapters.postgresql`)
and `SQLiteSnapshotStore` plus its own `SQLiteNotAvailableError`
(`eventsource.adapters.sqlite`) are not re-exported at the top level, though the
top-level `SQLITE_AVAILABLE` flag does cover the SQLite snapshot store along with the
SQLite event store. Persistent snapshot stores are therefore imported path-only, e.g.
`from eventsource.adapters.postgresql import PostgreSQLSnapshotStore`.

`eventsource.protocols`, `eventsource.exceptions`, and `eventsource.types` are partly
re-exported: the barrel carries the commonly used names from each, while the modules
themselves remain the canonical definition sites and contain additional members.

`eventsource._internal` is private. Names under it are not part of the public API and
may change in any release without notice.

### Optional Backends and `*_AVAILABLE` Flags

Every driver beyond `pydantic` and `sqlalchemy` is an extra declared in
`pyproject.toml`: `postgresql` (`asyncpg`), `sqlite` (`aiosqlite`), `redis`, `rabbitmq`
(`aio-pika`), `kafka` (`aiokafka`), `kafka-schema-registry` (adds `confluent-kafka`), and
`telemetry` (`opentelemetry-api` and `-sdk`). `all` pulls in all six backend/telemetry
extras; `all-backends` is the narrower `postgresql,sqlite` pair.

```bash
pip install "eventsource-py[postgresql,redis]"
pip install "eventsource-py[all]"
```

Optional drivers are guarded at import time, and the library uses two distinct patterns.

**Always importable, fails at construction.** The Redis, RabbitMQ, and Kafka bus modules
import their driver inside a `try/except ImportError`, set a module-level flag, and bind
the driver names to `None` so the module still imports. The bus classes themselves are
imported unconditionally by the barrel, so `RedisEventBus`, `RabbitMQEventBus`, and
`KafkaEventBus` — and their flags `REDIS_AVAILABLE`, `RABBITMQ_AVAILABLE`,
`KAFKA_AVAILABLE` — are always present in `eventsource.__all__` even with no driver
installed. The check moves to `__init__`, which raises `RedisNotAvailableError`,
`RabbitMQNotAvailableError`, or `KafkaNotAvailableError`. Each is a subclass of
`ImportError` carrying the install hint for its extra:

```python
from eventsource import REDIS_AVAILABLE, RedisEventBus

if REDIS_AVAILABLE:
    bus = RedisEventBus()  # otherwise raises RedisNotAvailableError
```

`SQLiteSnapshotStore` in `eventsource.adapters.sqlite` follows this same pattern, with
`SQLiteNotAvailableError` raised from its constructor.

**Conditionally importable, absent when missing.** The SQLite store and repositories
follow the other pattern: `SQLiteEventStore`, `SQLiteCheckpointRepository`,
`SQLiteDLQRepository`, and `SQLiteOutboxRepository` are imported by the barrel inside a
`try/except ImportError`. When `aiosqlite` is not installed they are not bound at all,
and `from eventsource import SQLiteEventStore` raises `ImportError`. `SQLITE_AVAILABLE`
is always defined as a module attribute, but it is appended to `__all__` — along with the
four SQLite names — only when the import succeeded. That conditional extension accounts
for the 102-versus-107 difference in the export count.

```python
from eventsource import SQLITE_AVAILABLE

if SQLITE_AVAILABLE:
    from eventsource import SQLiteEventStore
```

PostgreSQL has no flag. `PostgreSQLEventStore`, `PostgreSQLSnapshotStore`, and the
PostgreSQL repositories are built on SQLAlchemy async and take an `AsyncEngine` supplied
by the caller, so the `asyncpg` requirement surfaces where the engine is created
(`create_async_engine("postgresql+asyncpg://...")`) rather than at import.

Two further flags live outside the barrel. `eventsource.observability` exposes
`OTEL_AVAILABLE` for the OpenTelemetry tracing integration, and
`eventsource.subscriptions` exposes `OTEL_METRICS_AVAILABLE` (re-exported from its
shutdown module, and also available as `SHUTDOWN_OTEL_METRICS_AVAILABLE`) for the metrics
half; `eventsource.migration.metrics` defines its own `OTEL_METRICS_AVAILABLE`. When
OpenTelemetry is absent, tracing and metrics degrade to no-ops rather than raising.

The rule to rely on: for the bus backends and the SQLite snapshot store, **check the flag
rather than guarding the import**; for the SQLite store and repositories, **check the
flag before importing**.

### Async-First Signatures

Everything that performs I/O is async. `await` is the default calling convention
throughout the reference pages, and a signature shown without `async` is genuinely
synchronous — a property, a classmethod constructor, an in-process registration call, or
a pure-computation helper.

Three shapes recur across the async surface.

**Coroutines for bounded results.** `EventStore.append_events`, `get_events`,
`get_events_by_type`, `event_exists`, `get_stream_version`, and `get_global_position` are
`async def` methods returning a single value: an `AppendResult`, an `EventStream`, a
`bool`, an `int`. The same holds for `SnapshotStore.save_snapshot`, `get_snapshot`,
`delete_snapshot`, `snapshot_exists`, and `delete_snapshots_by_type`; for
`AggregateRepository.load`, `load_or_create`, `save`, `exists`, `get_version`,
`get_or_raise`, `create_snapshot`, and `await_pending_snapshots`; and for
`Projection.handle` and `reset`.

**Async iterators for unbounded reads.** `EventStore.read_stream` and
`EventStore.read_all` are declared as `AsyncIterator[StoredEvent]`, so large streams are
consumed incrementally with `async for` rather than materialized in memory:

```python
async for stored_event in store.read_all():
    await projection.handle(stored_event.event)
```

`read_stream` has a working default on the base class, implemented on top of
`get_events`; because that default cannot know global ordering, it yields events with
`global_position=0`. `read_all` is optional — the base implementation raises
`NotImplementedError`, and a backend without global ordering may leave it that way.

**Sync registration, async delivery.** `EventBus` splits along this line:
`publish(events, background=False)` is `async def`, while `subscribe`, `unsubscribe`,
`subscribe_all`, `subscribe_to_all_events`, and `unsubscribe_from_all_events` are plain
`def` — wiring up subscribers mutates in-process state and does not need to be awaited.
Passing `background=True` to `publish` makes delivery fire-and-forget, trading
synchronous error propagation for eventual consistency.

Handlers themselves may be either. `EventHandler` and `AsyncEventHandler` declare
`async def handle`; `SyncEventHandler` declares a plain `def handle`; and
`FlexibleEventHandler` / `FlexibleEventSubscriber` type `handle` as returning
`Awaitable[None] | None`, which is what lets the bus and subscription machinery accept
both kinds and adapt at call time.

Synchronous members that appear alongside all of this are not oversights. The frozen
dataclasses expose properties (`StoredEvent.event_id`, `EventStream.is_empty`,
`EventStream.latest_event`) and classmethod constructors (`EventStream.empty`,
`AppendResult.successful`, `AppendResult.conflicted`); `AggregateRepository` exposes
configuration properties (`aggregate_type`, `snapshot_mode`, `has_snapshot_support`,
`pending_snapshot_count`) and the purely local `create_new`.

For callers that cannot `await` at all — Celery tasks, Django management commands, RQ
workers — `SyncEventStoreAdapter` wraps any `EventStore` and exposes `*_sync`
counterparts: `append_events_sync`, `get_events_sync`, `get_events_by_type_sync`,
`get_stream_version_sync`, `event_exists_sync`, `read_all_sync`, and
`get_global_position_sync`, plus a `wrapped_store` property.

```python
from eventsource.sync import SyncEventStoreAdapter

sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
stream = sync_store.get_events_sync(order_id, "Order")
```

The adapter's constructor rejects anything that is not an `EventStore` with `TypeError`.
When no event loop is running it drives each coroutine with `asyncio.run` wrapped in
`asyncio.wait_for`; when called from inside a running loop it logs a warning and
dispatches via `asyncio.run_coroutine_threadsafe` against a shared four-worker thread
pool. Either path is bounded by the adapter's `timeout` (30 seconds by default,
overridable per call via the keyword-only `timeout` argument) and raises `TimeoutError`
when exceeded. `SyncEventStoreAdapter.shutdown_executor()` tears down the shared pool at
application shutdown.

One asymmetry to note: `read_all_sync` collects the async iterator into a `list`, so
unlike `read_all` it is not streaming. Bound it with `ReadOptions(limit=...)` on large
stores.

## Reference Pages

Six subsystems have dedicated reference pages, ordered below the way an event flows
through the library: an event is defined, appended to a store, produced by an aggregate,
short-circuited by a snapshot, consumed by a projection, and distributed over a bus.

| Page | Package(s) documented | Top-level exports covered |
| --- | --- | --- |
| [Events](events.md) | `eventsource.events` | 10 |
| Event Stores | `eventsource.stores` | 9 (10 with the SQLite extra) |
| [Aggregates](aggregates.md) | `eventsource.domain.aggregate`, `eventsource.application.aggregates`, `eventsource.handlers` | 4 |
| [Snapshots](snapshots.md) | `eventsource.ports.snapshots`, `eventsource.application.aggregates.snapshotting`, `eventsource.adapters.{memory,postgresql,sqlite}` | 7 of 11 |
| [Projections](projections.md) | `eventsource.application.projections`, `eventsource.readmodels` | 5 of 21 |
| [Event Bus](bus.md) | `eventsource.bus` | 20 |

Where a page documents more names than the barrel exports — snapshots, projections and
read models — the extra names are imported from their own module. Each page states which.

### Events — `api/events.md` (`eventsource.events.base`, `eventsource.events.registry`)

Covers `DomainEvent`, the frozen Pydantic base class every domain event derives from, and
the `EventRegistry` that maps `event_type` strings back to classes during
deserialization. `eventsource.events.__all__` and the corresponding slice of the barrel
agree exactly: `DomainEvent`, `EventRegistry`, `default_registry`, `register_event`, the
four convenience functions that operate on `default_registry` (`get_event_class`,
`get_event_class_or_none`, `is_event_registered`, `list_registered_events`), and the two
registry exceptions `EventTypeNotFoundError` and `DuplicateEventTypeError`.

The page explains the `event_type` auto-derivation that `DomainEvent.__init_subclass__`
performs — subclassing sets the `event_type` field default to the class name, and
declaring a different literal logs a warning unless the class sets
`suppress_event_type_warning = True` — together with the `_ensure_event_type` validator
that applies the same rule when an event is built from a dict. Note that this hook derives
the *name* only; it does **not** add the class to any registry.

It also documents `model_config = ConfigDict(frozen=True)` and the copy-on-write helpers
that immutability implies (`with_causation`, `with_metadata`, `with_aggregate_version`,
each returning a new instance via `model_copy`), the serialization pair `to_dict` /
`from_dict`, the relationship predicates `is_caused_by` and `is_correlated_with`, and the
standard fields: `event_id`, `event_type`, `event_version`, `occurred_at`,
`aggregate_id`, `aggregate_type`, `aggregate_version`, `tenant_id`, `actor_id`,
`correlation_id`, `causation_id`, and `metadata`. Only `aggregate_id` and
`aggregate_type` are required; the rest carry defaults or default factories.

On the registry side the page covers `EventRegistry` — an `RLock`-guarded mapping with
`register`, `get`, `get_or_none`, `contains`, `list_types`, `list_classes`, `unregister`,
`clear`, and the `len()`, `in`, and iteration dunders — the module-level
`default_registry`, and the `register_event` decorator, which works bare, with an
`event_type` key override, or against an alternate `registry` for test isolation. The two
registry exceptions extend builtins rather than `EventSourceError`:
`EventTypeNotFoundError` is a `KeyError` whose message lists the available types, and
`DuplicateEventTypeError` is a `ValueError` raised only when a *different* class claims an
already-registered name (re-registering the same class is a no-op).

`TenantDomainEvent` — the tenant-scoped subclass — lives in `eventsource.multitenancy`
and is described there.

### Event Stores — `eventsource.stores.interface`, `in_memory`, `postgresql`, `sqlite`

_No dedicated `api/stores.md` reference page exists yet; see the module docstrings and
`src/eventsource/stores/` for the authoritative contract until one is written._

Covers the `EventStore` abstract base class and the value types its methods exchange:
`StoredEvent`, `EventStream`, `AppendResult`, `ReadOptions`, `ReadDirection`, and
`ExpectedVersion`. `EventPublisher` — the narrow protocol for the append half of the
contract — is documented alongside it.

Three backends implement the interface: `InMemoryEventStore` for tests and prototypes,
`PostgreSQLEventStore` over a SQLAlchemy `AsyncEngine`, and `SQLiteEventStore`, which is
exported only when `aiosqlite` is installed. The page also documents the type-conversion
layer that `eventsource.stores.__all__` exposes but the barrel does not — `TypeConverter`,
`DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, and `DEFAULT_STRING_ID_FIELDS` — which
governs how backend-native column types are coerced back into event field types.

Read it for the optimistic-concurrency semantics of `append_events`, the difference
between the bounded `get_events` family and the streaming `read_stream` / `read_all`
iterators, and the conformance suite in `eventsource.testing.conformance` that a
third-party store implementation is expected to pass.

### Aggregates — `api/aggregates.md` (`eventsource.domain.aggregate`, `eventsource.application.aggregates.repository`)

Covers `AggregateRoot[TState]`, the abstract generic base holding state plus uncommitted
events, and `DeclarativeAggregate[TState]`, which routes events to methods marked with
`@handles` instead of requiring a hand-written `apply` dispatch — both in the entities
ring, at `eventsource.domain.aggregate`. `AggregateRepository[TAggregate]`, which loads
an aggregate by replaying its stream, saves uncommitted events with an
`expected_version`, and optionally consults a snapshot store to skip part of the replay,
is one ring out, at `eventsource.application.aggregates.repository`.

`eventsource.application.aggregates.__all__` adds the type variable `TAggregate`
alongside `AggregateRepository`; `TState` is declared in `eventsource.types` and
re-exported from the top-level barrel. The `handles` decorator itself is defined in
`eventsource.handlers` and re-exported from the barrel; the same decorator is used by
`DeclarativeProjection`.

The page also covers the repository's snapshot policy surface — `snapshot_mode`,
`has_snapshot_support`, `create_snapshot`, `pending_snapshot_count`, and
`await_pending_snapshots` — because snapshotting is configured at the repository, not on
the aggregate.

### Snapshots — `api/snapshots.md` (`eventsource.ports.snapshots`, `eventsource.application.aggregates.snapshotting`)

Covers the `Snapshot` value object, the `SnapshotStore` interface (both in
`eventsource.ports.snapshots`), its three backend adapters, the four-member exception
hierarchy (`eventsource.exceptions`), and the `SnapshotPolicy` / `SnapshotScheduler`
collaborators in `eventsource.application.aggregates.snapshotting` — the composable
replacement for ADR 0017's `SnapshotStrategy` — that decide when and how a snapshot is
written (see [ADR 0021](../adrs/0021-snapshot-policy-scheduler-composition.md)).

This is the clearest case of a page documenting more than the barrel exports. The
top-level package re-exports `Snapshot`, `SnapshotStore`, `InMemorySnapshotStore`, and
the four `Snapshot*Error` types. `PostgreSQLSnapshotStore` (from
`eventsource.adapters.postgresql`), `SQLiteSnapshotStore` and its own
`SQLiteNotAvailableError` (from `eventsource.adapters.sqlite`) are not re-exported at
the top level, though top-level `SQLITE_AVAILABLE` does cover SQLite snapshot support;
`SnapshotPolicy`, `SnapshotScheduler`, and their implementations are a further
module-level import from `eventsource.application.aggregates.snapshotting`. Persistent
snapshot storage is therefore always written path-only, e.g.
`from eventsource.adapters.postgresql import PostgreSQLSnapshotStore`.

The page's central invariant: a snapshot is an optimization artifact, never the source of
truth. A missing, unreadable, or schema-mismatched snapshot degrades to a full event
replay rather than surfacing an error.

### Projections — `api/projections.md` (`eventsource.application.projections.base`, `eventsource.readmodels`)

Covers the read side. From `eventsource.application.projections`: the `Projection` base
class, `CheckpointTrackingProjection` for resumable consumers, and `DeclarativeProjection`
for `@handles`-based routing. `DatabaseProjection` lives in `eventsource.adapters.sql`
instead -- its constructor takes a SQLAlchemy `async_sessionmaker`, which makes it an
adapter, not an application-ring class. The barrel exports `Projection`,
`CheckpointTrackingProjection`, `DeclarativeProjection`, and `DatabaseProjection` plus
`ReadModelProjection`; the `application.projections` package additionally exports
`SyncProjection`, `EventHandlerBase`, the `TenantFilter` alias, the `handles` /
`get_handled_event_type` / `is_event_handler` helpers, and the `ProjectionRegistry`,
`ProjectionCoordinator`, and `SubscriberRegistry` that drive projections as a group. The
retry policies (`eventsource.application.projections.retry`) and the checkpoint/DLQ
functions (`eventsource.application.projections.checkpoints`,
`eventsource.application.projections.dlq`) are documented on the same page but must be
imported from their own submodules.

From `eventsource.readmodels`: the `ReadModel` base class, the `ReadModelRepository`
protocol with its in-memory, PostgreSQL, and SQLite implementations, the `Query` and
`Filter` builders, the schema-generation helpers, and the read-model exception types.
Only `ReadModelProjection` reaches the top-level barrel.

### Event Bus — `api/bus.md` (`eventsource.bus.interface`, `memory`, `redis`, `rabbitmq`, `kafka`)

Covers the `EventBus` abstract base class — `async def publish`, plus the synchronous
`subscribe`, `unsubscribe`, `subscribe_all`, `subscribe_to_all_events`, and
`unsubscribe_from_all_events` — the `EventHandlerFunc` alias its subscribe methods
accept, and the handler and subscriber protocols re-exported from `eventsource.protocols`
(`EventHandler`, `AsyncEventHandler`, `SyncEventHandler`, `FlexibleEventHandler`,
`EventSubscriber`, `FlexibleEventSubscriber`).

Four implementations are documented: `InMemoryEventBus` for in-process delivery, and the
Redis Streams, RabbitMQ, and Kafka buses. Each of the three external backends contributes
a consistent quartet — `<Backend>EventBus`, `<Backend>EventBusConfig`,
`<Backend>EventBusStats`, and `<Backend>NotAvailableError` — alongside its availability
flag. All twenty names are in both `eventsource.bus.__all__` and the top-level barrel,
whether or not the drivers are installed; see
[Optional Backends and `*_AVAILABLE` Flags](#optional-backends-and-available-flags) for
why construction rather than import is where a missing driver is reported.

## Import model: the top-level barrel vs. submodule imports

`eventsource/__init__.py` is a *barrel*: it imports names from the implementation
modules and re-binds them at package level. Two import styles therefore reach the same
objects, and they are the same object — the barrel rebinds, it does not wrap or copy.

```python
from eventsource import DomainEvent            # barrel
from eventsource.events.base import DomainEvent  # submodule (identical object)
```

### Prefer the barrel for the public surface

For any name listed in `eventsource.__all__`, import from `eventsource` directly. The
barrel is the supported contract; the module paths beneath it
(`eventsource.events.base`, `eventsource.application.aggregates.repository`,
`eventsource.stores.in_memory`) are implementation detail and may be reorganized without
being treated as a breaking change, provided the barrel name keeps working.

```python
from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    handles,
)
```

Note that the barrel is flat, not namespaced: there are no `eventsource.events.X`-style
attribute chains to walk from the package object. Everything public sits one level down
from `eventsource`.

### Use submodule imports for the deep surface

Several public subsystems are intentionally *not* re-exported. They have their own
`__all__` and must be imported from their own module:

| Module | Exported names | Typical import |
| --- | --- | --- |
| `eventsource.testing` | 11 | `from eventsource.testing import EventBuilder, InMemoryTestHarness` |
| `eventsource.subscriptions` | 123 | `from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig` |
| `eventsource.observability` | 58 | `from eventsource.observability import get_tracer, OTEL_AVAILABLE` |
| `eventsource.migration` | 66 | `from eventsource.migration import Migration, MigrationConfig` |
| `eventsource.locks` | 5 | `from eventsource.locks import PostgreSQLLockManager` |

The size difference is the reason for the split: `subscriptions` and `migration` alone
would more than double the barrel, and most applications need neither.

`eventsource.protocols`, `eventsource.exceptions`, and `eventsource.types` sit in
between. Their commonly used members are re-exported, but the modules remain the
canonical definition sites — import from the module when you need a member the barrel
does not carry.

### Import-time cost and side effects

Importing `eventsource` is not free of side effects, and two of them are worth knowing:

1. **Every backend module is imported eagerly**, including the Redis, RabbitMQ, and
   Kafka bus modules. Each of those guards its own driver import internally, so the
   import succeeds without the driver installed — but the module object, its config
   dataclass, and its `*NotAvailableError` are all created regardless. The one exception
   is SQLite, whose imports are wrapped in a `try/except ImportError` in the barrel
   itself.
2. **Event classes self-register on definition.** `DomainEvent.__init_subclass__` adds
   each subclass to the global `default_registry`, so importing a module that defines
   events mutates process-global state. Importing the same event class twice under two
   different module paths is the usual cause of `DuplicateEventTypeError`.

Because of (2), prefer a single canonical import path per event module in your own code.
Mixing `from myapp.events import OrderPlaced` and
`from myapp import events; events.OrderPlaced` is fine — both resolve to one module — but
arranging for the same file to be imported as two distinct modules (for example via a
mix of relative and `sys.path`-relative imports) is not.

### What not to import

`eventsource._internal` is private. Its contents are helpers for the packages above it
and may be renamed, moved, or removed in any release. Likewise, treat the raw SQL under
`eventsource.migrations` as data applied by tooling rather than an importable API.

## Version metadata (`__version__`, `importlib.metadata`, and the `0.0.0.dev0` source fallback)

`eventsource.__version__` is the only version attribute in the public surface, and it is
listed first in `__all__`. It is a plain `str`, resolved once at import time:

```python
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("eventsource-py")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"
```

Two properties follow from that snippet and are worth stating explicitly.

### The value comes from installed distribution metadata, not from the source tree

The lookup key is the **distribution** name `eventsource-py`, not the import name
`eventsource`. `importlib.metadata.version()` reads the `Version` field of the installed
distribution's metadata, so `__version__` reflects what the environment actually has
installed. The `version = "0.5.0"` line in `pyproject.toml` is the input to that metadata
at build/install time; it is not read at runtime.

```python
import eventsource

eventsource.__version__  # "0.5.0" for the current release
```

Practical consequences:

- An editable install (`uv sync`, `pip install -e .`) still produces real metadata, so
  `__version__` is the version recorded when the editable install was created. Bumping
  `pyproject.toml` without reinstalling leaves the old value in place.
- The value is computed at module import and then bound; it is not a property and does
  not re-read metadata on subsequent access.
- Because the key is the distribution name, a differently-named fork or vendored copy
  that keeps the `eventsource` import name will hit the fallback path below unless it is
  installed as `eventsource-py`.

### `0.0.0.dev0` means "not installed", not "unreleased"

`PackageNotFoundError` is raised when no distribution named `eventsource-py` is visible
on `sys.path`. The barrel catches it and substitutes the sentinel `"0.0.0.dev0"`. This is
the value you see when the package is used straight from a source checkout — for
example a `sys.path` entry pointing at `src/`, or a container that copies the tree
without installing it.

The fallback is deliberate: importing `eventsource` never raises because of missing
metadata. Nothing else in the library branches on `__version__`, so a checkout reporting
`0.0.0.dev0` behaves identically to an installed copy in every other respect.

The sentinel is a valid [PEP 440](https://peps.python.org/pep-0440/) developmental
release, which means it compares as *older* than every real release under
`packaging.version.Version`. Code that gates on a minimum version therefore treats an
uninstalled checkout as "too old":

```python
from packaging.version import Version

Version("0.0.0.dev0") < Version("0.5.0")  # True
```

### Reading the version without importing the package

When you need the installed version but do not want the import-time side effects
described above (eager backend imports, event self-registration), query the metadata
directly:

```python
from importlib.metadata import version

version("eventsource-py")  # raises PackageNotFoundError if not installed
```

This is the same call the barrel makes, minus the fallback — it raises rather than
returning a sentinel, which is usually what packaging and diagnostic tooling wants.

### What the version number does and does not promise

`__version__` is a release identifier, not a capability probe. It tells you which
release of the library is installed; it says nothing about which optional drivers are
present, because extras are installed alongside the same distribution version. Two
environments can both report `0.5.0` while one has `aiosqlite` and the other does not.

Use the `*_AVAILABLE` flags for capability questions — see
[Runtime capability checks and feature detection recipes](#runtime-capability-checks-and-feature-detection-recipes).

## Always-available surface

The names in this part of the reference are importable from `eventsource` with only the
core dependencies installed — `pydantic` and `sqlalchemy`. No extra, no driver, no
running service is required to import them.

With every extra installed, `eventsource.__all__` holds 107 names. Without the SQLite
extra it holds 102: the five SQLite names (`SQLITE_AVAILABLE`, `SQLiteEventStore`,
`SQLiteCheckpointRepository`, `SQLiteOutboxRepository`, `SQLiteDLQRepository`) are the
only entries that are appended conditionally. Everything else in `__all__` is bound
unconditionally by `src/eventsource/__init__.py` and is therefore always available in
the sense used here.

### "Available" means importable, not necessarily usable

Three distinct guarantees are in play, and they are worth separating before reading the
subsections below:

| Guarantee | Applies to |
| --- | --- |
| Imports, constructs, and runs with core deps only | `DomainEvent`, the type aliases, `InMemoryEventStore`, `InMemoryEventBus`, `InMemorySnapshotStore`, the in-memory repositories, aggregates, projections, protocols, multi-tenancy, `SyncEventStoreAdapter`, `EventSourceJSONEncoder`, exceptions |
| Imports and constructs, but needs a reachable service at runtime | `PostgreSQLEventStore`, `PostgreSQLCheckpointRepository`, `PostgreSQLDLQRepository`, `PostgreSQLOutboxRepository` (need `asyncpg` and a PostgreSQL server) |
| Imports, but raises at construction without its driver | `RedisEventBus`, `KafkaEventBus`, `RabbitMQEventBus` — see [Optional backends and availability flags](#optional-backends-and-availability-flags) |

Only the first row is unconditionally safe to instantiate in a bare environment, which
is why the in-memory implementations are the ones used throughout the tutorials and the
test harness.

### Why the surface is flat

Every name below is re-exported at package level. There is no `eventsource.events.X`
attribute chain to walk from the package object: `from eventsource import DomainEvent,
AggregateRoot, InMemoryEventStore` is the whole import model. The subsections that
follow group the names thematically for reading, not by module path — the grouping is
editorial, and the module a name happens to live in
(`eventsource.stores.interface`, `eventsource.repositories`, and so on) is
implementation detail for anything listed in `__all__`.

### Ordering of the subsections

The groups run roughly in dependency order — types, then events, then the store
interface, then the components built on top of it:

1. **Types** — the aliases and the `TState` type variable every other signature is
   written in terms of.
2. **Events and event registry** — `DomainEvent` and the global registry it
   self-registers into.
3. **Event store interface and data structures** — the `EventStore` contract plus the
   value types it exchanges.
4. **Event store implementations** — `InMemoryEventStore` and `PostgreSQLEventStore`.
5. **Aggregates** — `AggregateRoot`, `DeclarativeAggregate`, `AggregateRepository`,
   `handles`.
6. **Event bus** and **handler protocols** — publish/subscribe and the callable shapes
   the bus accepts.
7. **Projections and read models**, **repository infrastructure**, and **snapshots** —
   the read side and its supporting persistence.
8. **Multi-tenancy**, **sync adapters**, **serialization**, and **exceptions** —
   cross-cutting concerns.

### Async unless noted

Store, bus, projection, repository, and snapshot-store methods are all coroutines and
must be awaited. The documented exceptions are `SyncEventStoreAdapter`, which wraps an
async store for synchronous callers, and `tenant_scope_sync`, the synchronous
counterpart of `tenant_scope`. Both are called out in their own subsections.

## Overview

`eventsource-py` is organized as a small set of collaborating interfaces, each with one
or more interchangeable backends. The interfaces live in `interface.py` / `base.py`
modules and the backends are colocated beside them, so choosing PostgreSQL over SQLite
or Redis over RabbitMQ is a construction-time decision that does not change call sites.

### The write path

1. A domain operation loads an aggregate through `AggregateRepository`, which reads the
   aggregate's stream from an `EventStore` (optionally starting from a `Snapshot`) and
   replays it.
2. The aggregate — an `AggregateRoot`, or a `DeclarativeAggregate` that routes events to
   methods marked with `@handles(EventType)` — validates the command and records new
   `DomainEvent` instances.
3. The repository appends those events with an `ExpectedVersion`, and the store raises
   `OptimisticLockError` if another writer advanced the stream first.
4. A successful append returns an `AppendResult`; the events, now `StoredEvent`
   records carrying stream and global positions, are available to readers.

### The read path

`EventStore.read_stream` and the store's global-read methods return an `EventStream`
shaped by `ReadOptions` (which includes a `ReadDirection`). `Projection` and its
subclasses — `CheckpointTrackingProjection`, `DeclarativeProjection`,
`DatabaseProjection`, `ReadModelProjection` — consume those events to build read models,
recording progress through a `CheckpointRepository` and diverting poison events to a
`DLQRepository`. The `eventsource.subscriptions` package drives projections
continuously, adding retry policy, health reporting, and flow control.

### Distribution

An `EventBus` decouples producers from consumers. `InMemoryEventBus` is always usable;
`RedisEventBus`, `RabbitMQEventBus`, and `KafkaEventBus` back the same interface with
external brokers. Where atomicity between the store append and the publish matters, an
`OutboxRepository` records events transactionally for a separate relay to publish.
`EventPublisher` is the narrow protocol for "can publish events", satisfied by stores and
buses alike.

### Cross-cutting concerns

| Concern | Surface |
| --- | --- |
| Multi-tenancy | `TenantDomainEvent`, `tenant_context`, `tenant_scope`, `get_current_tenant` — tenant identity is carried in a `contextvar`, not threaded through every call |
| Serialization | `EventSourceJSONEncoder`, used by the store backends to persist event payloads |
| Snapshots | `SnapshotStore` with in-memory, PostgreSQL, and SQLite implementations, to bound replay cost for long streams |
| Distributed locking | `eventsource.locks` — PostgreSQL advisory locks for single-writer coordination |
| Synchronous callers | `SyncEventStoreAdapter`, which wraps any async `EventStore` |
| Tracing | `eventsource.observability`, an optional OpenTelemetry integration |
| Testing | `eventsource.testing` — assertions, BDD helpers, a harness, and conformance suites that any `EventStore` or `EventBus` implementation can be run against |

### What this page covers

The sections below walk the barrel group by group: importing conventions, then each core
module (events, stores, aggregates, snapshots, projections, bus, locks), then the
optional-dependency rules, the exception hierarchy, and the type aliases. Each group is
summarized here and expanded on its own reference page, linked from
[See also](#see-also).

## Importing from `eventsource`

The top-level package is a barrel: `src/eventsource/__init__.py` re-exports the
user-facing names from the subpackages and declares them in `__all__`. Importing from
the barrel is the supported form, and the one used throughout this documentation:

```python
from eventsource import (
    DomainEvent,
    EventStore,
    InMemoryEventStore,
    AggregateRoot,
    AggregateRepository,
    handles,
)
```

Deeper import paths (`from eventsource.stores.interface import EventStore`) resolve to
the same objects, but the module layout beneath the barrel is an implementation detail
and may be rearranged; only the barrel names and the documented submodule entry points
carry compatibility guarantees.

### What the barrel exports

`__all__` groups the exports by concern. In source order:

| Group | Names |
| --- | --- |
| Version | `__version__` |
| Type aliases | `TState`, `AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId` |
| Events | `DomainEvent` |
| Event registry | `EventRegistry`, `default_registry`, `register_event`, `get_event_class`, `get_event_class_or_none`, `is_event_registered`, `list_registered_events`, `EventTypeNotFoundError`, `DuplicateEventTypeError` |
| Store interface | `EventStore`, `EventPublisher`, `EventStream`, `AppendResult`, `StoredEvent`, `ReadOptions`, `ReadDirection`, `ExpectedVersion` |
| Store backends | `InMemoryEventStore`, `PostgreSQLEventStore` |
| Aggregates | `AggregateRoot`, `AggregateRepository`, `DeclarativeAggregate`, `handles` |
| Event bus | `EventBus`, `EventHandlerFunc`, `AsyncEventHandler`, `InMemoryEventBus` |
| Handler protocols | `EventHandler`, `SyncEventHandler`, `FlexibleEventHandler`, `EventSubscriber`, `FlexibleEventSubscriber` |
| Redis bus | `RedisEventBus`, `RedisEventBusConfig`, `RedisEventBusStats`, `RedisNotAvailableError`, `REDIS_AVAILABLE` |
| RabbitMQ bus | `RabbitMQEventBus`, `RabbitMQEventBusConfig`, `RabbitMQEventBusStats`, `RabbitMQNotAvailableError`, `RABBITMQ_AVAILABLE` |
| Kafka bus | `KafkaEventBus`, `KafkaEventBusConfig`, `KafkaEventBusStats`, `KafkaNotAvailableError`, `KAFKA_AVAILABLE` |
| Exceptions | `EventSourceError`, `AggregateNotCreatedError`, `AggregateNotFoundError`, `EventNotFoundError`, `EventVersionError`, `OptimisticLockError`, `ProjectionError` |
| Checkpoints | `CheckpointRepository`, `PostgreSQLCheckpointRepository`, `InMemoryCheckpointRepository`, `CheckpointData`, `LagMetrics` |
| Dead letter queue | `DLQRepository`, `PostgreSQLDLQRepository`, `InMemoryDLQRepository`, `DLQEntry`, `DLQStats`, `ProjectionFailureCount` |
| Outbox | `OutboxRepository`, `PostgreSQLOutboxRepository`, `InMemoryOutboxRepository`, `OutboxEntry`, `OutboxStats` |
| Serialization | `EventSourceJSONEncoder` |
| Projections | `Projection`, `CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection`, `ReadModelProjection` |
| Snapshots | `Snapshot`, `SnapshotStore`, `InMemorySnapshotStore`, `SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError` |
| Sync adapter | `SyncEventStoreAdapter` |
| Multi-tenancy | `tenant_context`, `tenant_scope`, `tenant_scope_sync`, `get_current_tenant`, `get_required_tenant`, `set_current_tenant`, `clear_tenant_context`, `TenantDomainEvent`, `TenantContextNotSetError`, `TenantMismatchError` |

Every name in that table imports with only the core dependencies installed. The Redis,
RabbitMQ, and Kafka bus classes are unconditional imports — their modules guard the
driver import and set the matching `*_AVAILABLE` flag, so the class is importable even
without the extra and fails only when constructed. See
[Optional Dependencies and Backend Availability](#optional-dependencies-and-backend-availability).

### The SQLite exception

SQLite is the one group whose barrel exports are conditional. `__init__.py` attempts the
imports inside a `try`/`except ImportError`, sets `SQLITE_AVAILABLE` accordingly, and
extends `__all__` only when the import succeeded:

```python
if SQLITE_AVAILABLE:
    __all__.extend([
        "SQLITE_AVAILABLE",
        "SQLiteEventStore",
        "SQLiteCheckpointRepository",
        "SQLiteOutboxRepository",
        "SQLiteDLQRepository",
    ])
```

Consequences worth knowing:

- `from eventsource import SQLITE_AVAILABLE` always works — the name is bound as a module
  attribute in both branches. Only its presence in `__all__` (and therefore in
  `from eventsource import *`) is conditional.
- `from eventsource import SQLiteEventStore` raises `ImportError` when `aiosqlite` is
  not installed, rather than deferring to construction time. Guard on the flag if your
  code must run in both configurations:

  ```python
  from eventsource import SQLITE_AVAILABLE

  if SQLITE_AVAILABLE:
      from eventsource import SQLiteEventStore
  ```

- `SQLiteSnapshotStore` (`eventsource.adapters.sqlite`) is not imported at the top
  level at all, so it is not gated by the top-level `SQLITE_AVAILABLE` flag. The
  `eventsource.adapters.sqlite` package exposes its own `SQLITE_AVAILABLE` (plus
  `AIOSQLITE_AVAILABLE` for the event store) -- import path-only and check that
  flag, or catch `SQLiteNotAvailableError` from the constructor.

### Names imported from submodules

Some public surface is deliberately not re-exported at the top level, either because it
is a cohesive namespace of its own or because it belongs to an optional integration.
Import these from their module:

| Module | Contents |
| --- | --- |
| `eventsource.locks` | `PostgreSQLLockManager`, `LockInfo`, `migration_lock_key`, `LockAcquisitionError`, `LockNotHeldError` |
| `eventsource.subscriptions` | Subscription manager, runners, retry policy, health, and flow control |
| `eventsource.testing` | Assertions, BDD helpers, the test harness, builders, and the conformance suites |
| `eventsource.observability` | OpenTelemetry tracing integration (`telemetry` extra) |
| `eventsource.migration` | Live event-store migration tooling: dual-write, cutover, sync tracking |
| `eventsource.gdpr` | GDPR compliance utilities |
| `eventsource.config` | Configuration helpers |

Two modules are re-exported *and* importable directly, because they are canonical
homes rather than optional add-ons: `eventsource.handlers` (`handles`) and
`eventsource.protocols` (the handler and subscriber protocols). Prefer the barrel.

`eventsource._internal` is private. Nothing under it is part of the public API, and its
contents may change in any release.

### Type checking

The package ships a `py.typed` marker, so mypy and other checkers consume the inline
annotations directly — no stub package is required or published.

### Events

`eventsource.events` defines the event vocabulary: `DomainEvent`, the base class every
event subclasses, and `EventRegistry`, the name-to-class mapping used to rehydrate
persisted events. Both are re-exported from the barrel.

#### `DomainEvent`

A Pydantic `BaseModel` with `model_config = ConfigDict(frozen=True)` — instances are
immutable, and any "change" produces a copy. Subclasses add payload fields:

```python
from uuid import UUID
from eventsource import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID
```

**Fields.** Every event carries the following, independent of its payload:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `event_id` | `UUID` | `uuid4()` | Unique identifier for this event instance |
| `event_type` | `str` | class name (auto-derived) | Type name used for storage and registry lookup |
| `event_version` | `int` (`>= 1`) | `1` | Schema version of this event type, for migrations |
| `occurred_at` | `datetime` | `datetime.now(UTC)` | When the event occurred |
| `aggregate_id` | `UUID` | **required** | Aggregate the event belongs to |
| `aggregate_type` | `str` | **required** | Aggregate type name, e.g. `"Order"` |
| `aggregate_version` | `int` (`>= 1`) | `1` | Aggregate version after this event |
| `tenant_id` | `UUID \| None` | `None` | Tenant, for multi-tenant deployments |
| `actor_id` | `str \| None` | `None` | User or system that triggered the event |
| `correlation_id` | `UUID` | `uuid4()` | Links events belonging to one logical operation |
| `causation_id` | `UUID \| None` | `None` | The `event_id` of the event that caused this one |
| `metadata` | `dict[str, Any]` | `{}` | Free-form additional metadata |

`aggregate_id` and `aggregate_type` are the only required constructor arguments beyond
the subclass's own payload fields. `aggregate_version` defaults to `1` and is normally
overwritten by the aggregate when it records the event.

**`event_type` derivation.** `DomainEvent.__init_subclass__` inspects the subclass body:
if it does not declare `event_type`, the field default is rewritten to the class name, so
`OrderCreated().event_type == "OrderCreated"`. A `model_validator(mode="before")` applies
the same rule when constructing from a dict, so `OrderCreated.model_validate({...})` and
`from_dict` behave identically to the keyword form.

Declaring `event_type` explicitly is supported and takes precedence — useful when the
stored name must stay stable across a class rename, or when a dotted naming scheme is
preferred:

```python
class OrderCreated(DomainEvent):
    event_type: str = "order_created_v2"
    aggregate_type: str = "Order"
    suppress_event_type_warning = True
    order_number: str
```

When the explicit `event_type` differs from the class name, the class emits a
`logging.warning` at definition time. Setting the class variable
`suppress_event_type_warning: ClassVar[bool] = True` silences it; the warning is
advisory only and has no effect on behavior.

**Copy-with methods.** Because events are frozen, these return new instances rather than
mutating:

| Method | Result |
| --- | --- |
| `with_causation(causing_event)` | Copy with `causation_id` set to `causing_event.event_id` and `correlation_id` inherited from `causing_event` |
| `with_metadata(**kwargs)` | Copy whose `metadata` is the existing dict merged with `kwargs` (new keys win) |
| `with_aggregate_version(version)` | Copy with `aggregate_version` set — typically called by the aggregate when recording |

**Serialization.** `to_dict()` is `model_dump(mode="json")`, so `UUID` values become
strings and `datetime` values become ISO-8601 strings; the result is directly
JSON-serializable. `from_dict(data)` is a classmethod wrapping `model_validate` and
raises Pydantic's `ValidationError` when the payload does not match the schema.

**Comparison helpers.** `is_caused_by(event)` returns whether this event's `causation_id`
equals `event.event_id`. `is_correlated_with(event)` returns whether both share a
`correlation_id` — that is, whether they belong to the same logical operation or saga.

**Representations.** `__str__` renders `EventType(event_id=…, aggregate_id=…,
version=…)`; `__repr__` additionally includes `event_type`, `aggregate_type`,
`tenant_id`, and `occurred_at`.

#### `TenantDomainEvent`

`eventsource.multitenancy` provides `TenantDomainEvent`, a `DomainEvent` subclass that
narrows `tenant_id` to required and validates it is not `None`. Its
`with_tenant_context(...)` constructor reads the tenant from the ambient `contextvar`
established by `tenant_scope` / `tenant_context`, so request handlers do not have to pass
the tenant explicitly. It is exported from the barrel alongside the tenant-context
functions.

#### `EventRegistry`

Storage keeps `event_type` as a string; deserialization needs the class back. That
mapping lives in `EventRegistry`, a thread-safe dict guarded by an `RLock`.

Registration is **explicit** — defining a `DomainEvent` subclass does not add it to any
registry. Register with the decorator or the method:

```python
from eventsource import register_event, DomainEvent


@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"


@register_event(event_type="order.shipped")
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
```

`register_event` works with or without parentheses and accepts `event_type=` to override
the stored name and `registry=` to target a registry other than the default.

`EventRegistry` methods:

| Member | Behavior |
| --- | --- |
| `register(event_class, event_type=None)` | Registers and returns the class (so it composes as a decorator). Re-registering the same class under the same name is a no-op |
| `get(event_type)` | Returns the class; raises `EventTypeNotFoundError` if unknown |
| `get_or_none(event_type)` | Returns the class or `None` |
| `contains(event_type)` / `event_type in registry` | Membership test |
| `list_types()` | Sorted list of registered type names |
| `list_classes()` | Registered classes, ordered by type name |
| `unregister(event_type)` | Removes the entry; returns whether it was present |
| `clear()` | Empties the registry — intended for test isolation |
| `len(registry)`, `iter(registry)` | Count of entries; iteration over type names |

Note that `bool(registry)` is always `True`, including for an empty registry; use
`len(registry)` to test for emptiness.

**Name resolution.** When `event_type` is not passed to `register`, the name is resolved
in order: the explicit argument, then the class's `event_type` field default if it is a
string, then the class name. Because `__init_subclass__` has already set that default to
the class name for classes that do not declare one, the common case registers under the
class name.

**Duplicates.** Registering a *different* class under an already-registered name raises
`DuplicateEventTypeError`, which carries `event_type`, `existing_class`, and
`new_class`. Lookups for unknown names raise `EventTypeNotFoundError`, whose message
lists the available types; it carries `event_type` and `available_types`.
`EventTypeNotFoundError` subclasses `KeyError` and `DuplicateEventTypeError` subclasses
`ValueError`, so both are catchable through their builtin bases as well as by name.

**Default registry.** `default_registry` is a module-level `EventRegistry` instance. The
convenience functions `get_event_class`, `get_event_class_or_none`, `is_event_registered`,
and `list_registered_events` operate on it. Constructing a private `EventRegistry()` is
the recommended way to isolate tests from globally registered application events.

### Event Stores

`eventsource.stores` defines the append-and-read contract that everything else is built
on, plus the data structures that contract exchanges. All of the names below are
re-exported from the barrel, except `SQLiteEventStore` (conditional — see
[The SQLite exception](#the-sqlite-exception)) and the type-conversion helpers, which
are imported from `eventsource.stores`.

#### `EventStore`

An ABC, not a Protocol. Four methods are abstract and must be implemented by every
backend; two more have working default implementations that backends may override; one is
optional and raises by default.

| Method | Status | Signature |
| --- | --- | --- |
| `append_events` | abstract | `(aggregate_id: UUID, aggregate_type: str, events: list[DomainEvent], expected_version: int) -> AppendResult` |
| `get_events` | abstract | `(aggregate_id: UUID, aggregate_type: str \| None = None, from_version: int = 0, from_timestamp: datetime \| None = None, to_timestamp: datetime \| None = None) -> EventStream` |
| `get_events_by_type` | abstract | `(aggregate_type: str, tenant_id: UUID \| None = None, from_timestamp: datetime \| None = None) -> list[DomainEvent]` |
| `event_exists` | abstract | `(event_id: UUID) -> bool` |
| `get_global_position` | abstract | `() -> int` |
| `get_stream_version` | default | `(aggregate_id: UUID, aggregate_type: str) -> int` |
| `read_stream` | default | `(stream_id: str, options: ReadOptions \| None = None) -> AsyncIterator[StoredEvent]` |
| `read_all` | optional | `(options: ReadOptions \| None = None) -> AsyncIterator[StoredEvent]` |

Every method is a coroutine; `read_stream` and `read_all` are async generators.

**Write semantics.** `append_events` is the only write operation. Appending an empty
list is not an error — all three backends short-circuit and return
`AppendResult.successful(expected_version)` without touching storage. Appends are
idempotent at event granularity: an event whose `event_id` is already stored is skipped
rather than duplicated, so a retried append does not produce a second copy.

Version conflicts **raise** rather than return. All three backends raise
`OptimisticLockError(aggregate_id, expected_version, current_version)` on mismatch; the
`AppendResult.conflicted(...)` constructor and the `conflict` field exist for
implementations that prefer to report conflicts as data, and are not used by the bundled
backends. Code that calls `append_events` directly should catch `OptimisticLockError`,
not inspect `result.conflict`.

**Version scoping.** The current version is computed per `(aggregate_id,
aggregate_type)` pair, not per `aggregate_id` alone. Two aggregate types sharing an ID
maintain independent version counters.

**Default `read_stream`.** The base implementation parses `stream_id` as
`"<aggregate_id>:<aggregate_type>"`, splitting on the last colon; a string with no colon
is parsed entirely as a UUID and the type filter is dropped. It then delegates to
`get_events`, reverses the list for `ReadDirection.BACKWARD`, truncates to `limit`, and
yields `StoredEvent` values whose `global_position` is `0` — the base class has no way to
know it. Backends that can do better override the method; `PostgreSQLEventStore` and
`SQLiteEventStore` both do, and both return real global positions.

**Default `read_all`.** The base class raises `NotImplementedError`. `InMemoryEventStore`,
`PostgreSQLEventStore`, and `SQLiteEventStore` all implement it, honoring
`ReadOptions.tenant_id` for tenant-scoped iteration.

#### `EventPublisher`

A `Protocol` with a single method, `async publish(events: list[DomainEvent]) -> None`.
It is the narrowest useful contract — "something these events can be handed to" — and is
satisfied structurally by event buses and by any custom sink. Nothing needs to inherit
from it.

#### Data structures

All four are frozen dataclasses.

**`StoredEvent`** wraps a persisted `DomainEvent` with position metadata: `event`,
`stream_id`, `stream_position` (1-based, within the aggregate's stream),
`global_position` (1-based, across the store), and `stored_at`. The properties
`event_id`, `event_type`, `aggregate_id`, and `aggregate_type` delegate to the wrapped
event, so a `StoredEvent` can usually be read without unwrapping it.

**`EventStream`** is what `get_events` returns: `aggregate_id`, `aggregate_type`,
`events` (a `list[DomainEvent]`, oldest first, defaulting to empty), and `version`
(defaulting to `0`). It exposes `is_empty`, `latest_event` (`None` on an empty stream),
and the classmethod `EventStream.empty(aggregate_id, aggregate_type)`. Note that it
carries plain `DomainEvent` values, not `StoredEvent` — position metadata is available
only through `read_stream` / `read_all`.

**`AppendResult`** reports the outcome of a write: `success`, `new_version`,
`global_position` (default `0`), and `conflict` (default `False`). Construct it through
`AppendResult.successful(new_version, global_position=0)` or
`AppendResult.conflicted(current_version)` rather than positionally.

**`ReadOptions`** configures reads:

| Field | Type | Default |
| --- | --- | --- |
| `direction` | `ReadDirection` | `ReadDirection.FORWARD` |
| `from_position` | `int` | `0` |
| `limit` | `int \| None` | `None` (unbounded) |
| `from_timestamp` | `datetime \| None` | `None` |
| `to_timestamp` | `datetime \| None` | `None` |
| `tenant_id` | `UUID \| None` | `None` (all tenants) |

`__post_init__` validates eagerly: `from_position` must be `>= 0` or exactly `-1` (the
sentinel for "the end"), and `limit`, when given, must be `>= 0`. Either violation raises
`ValueError` at construction.

**`ReadDirection`** is an `Enum` with members `FORWARD` (`"forward"`) and `BACKWARD`
(`"backward"`).

#### `ExpectedVersion`

A namespace of three `int` constants passed as `expected_version`:

| Constant | Value | Meaning |
| --- | --- | --- |
| `ExpectedVersion.ANY` | `-1` | Skip the version check entirely |
| `ExpectedVersion.NO_STREAM` | `0` | The stream must not exist yet |
| `ExpectedVersion.STREAM_EXISTS` | `-2` | The stream must already have at least one event |

Any other non-negative integer is an exact-version assertion. Because `NO_STREAM` is
literally `0`, passing `expected_version=0` for a brand-new aggregate and passing
`ExpectedVersion.NO_STREAM` are the same call.

#### Backends

| Backend | Import | Requires | Constructed with |
| --- | --- | --- | --- |
| `InMemoryEventStore` | barrel | core only | `InMemoryEventStore(*, tracer=None, enable_tracing=True)` |
| `PostgreSQLEventStore` | barrel | `asyncpg` at runtime | an `async_sessionmaker[AsyncSession]` |
| `SQLiteEventStore` | barrel when `SQLITE_AVAILABLE` | `aiosqlite` | a database path or `":memory:"` |

**`InMemoryEventStore`** keeps events in dictionaries guarded by an `asyncio.Lock`,
and loses everything on process exit. Beyond the interface it offers test affordances:
`clear()`, `get_all_events()`, `get_event_count()`, and `get_aggregate_ids()` — all
coroutines. It is the intended store for unit tests and prototypes, not for production
or multi-process deployments.

**`PostgreSQLEventStore`** takes a SQLAlchemy `async_sessionmaker` as its only positional
argument; everything else is keyword-only:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from eventsource import PostgreSQLEventStore

engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

store = PostgreSQLEventStore(session_factory, outbox_enabled=True)
```

Keyword arguments: `event_registry` (defaults to `default_registry`), `outbox_enabled`,
`tracer`, `enable_tracing`, `type_converter`, `uuid_fields`, `string_id_fields`, and
`auto_detect_uuid`. With `outbox_enabled=True`, each appended event is also inserted into
`event_outbox` with status `'pending'` **in the same transaction as the event itself** —
that shared transaction is what makes the outbox pattern reliable. Read-only properties
`session_factory`, `event_registry`, and `outbox_enabled` expose the configuration; the
session factory in particular lets callers join the store's transactions.

**`SQLiteEventStore`** is connection-oriented and must be opened before use. It is an
async context manager, and `initialize()` creates the schema (`events`, `event_outbox`,
`projection_checkpoints`, `dead_letter_queue`) idempotently:

```python
from eventsource import SQLiteEventStore

async with SQLiteEventStore(":memory:") as store:
    await store.initialize()
    ...
```

Calling a store method before connecting raises `RuntimeError`. `close()` is safe to call
repeatedly. Constructor arguments beyond `database` and the positional-or-keyword
`event_registry` are keyword-only: `wal_mode` (default `True`), `busy_timeout` in
milliseconds (default `5000`), plus the same tracing and type-conversion knobs as the
PostgreSQL backend. Properties: `database`, `event_registry`, `is_connected`, `wal_mode`,
`busy_timeout`. Because SQLite has no native UUID or timestamp types, the backend stores
UUIDs as 36-character hyphenated text, timestamps as ISO-8601 text, and payloads as JSON
text.

#### Type conversion

Deserializing a stored payload means turning JSON strings back into `UUID` values, and
the store cannot know from JSON alone which strings were UUIDs. `eventsource.stores`
exports the `TypeConverter` protocol, the `DefaultTypeConverter` implementation, and the
frozensets `DEFAULT_UUID_FIELDS` and `DEFAULT_STRING_ID_FIELDS` that drive it.

`DefaultTypeConverter` auto-detects: a field whose name ends in `_id` is treated as a
UUID unless it appears in `string_id_fields`. Both SQL backends accept `uuid_fields`
(names to add), `string_id_fields` (names to exempt — the escape hatch for
`stripe_customer_id` and friends), and `auto_detect_uuid=False` to disable the heuristic.
For full control, `DefaultTypeConverter.strict(uuid_fields)` treats exactly the named
fields as UUIDs and nothing else; the classmethod
`PostgreSQLEventStore.with_strict_uuid_detection(session_factory, uuid_fields, ...)` and
its `SQLiteEventStore` counterpart are shorthand for constructing a store around one.

#### Synchronous access

`SyncEventStoreAdapter` (from the barrel, defined in `eventsource.sync`) wraps any
`EventStore` for callers that cannot await, running each coroutine on a shared
`ThreadPoolExecutor`. Its methods carry a `_sync` suffix — `append_events_sync`,
`get_events_sync`, `get_events_by_type_sync`, `get_stream_version_sync`,
`event_exists_sync`, `read_all_sync` (which collects the iterator into a list), and
`get_global_position_sync` — so the two call styles cannot be confused. It exposes
`wrapped_store` and `timeout` properties and a classmethod `shutdown_executor()` for
teardown.

#### Verifying a custom store

`eventsource.testing.conformance` provides `EventStoreConformanceSuite`, an ABC of
behavioral tests covering append semantics, optimistic locking, idempotency, and reads.
Subclass it, supply a store instance, and any implementation — including one you wrote —
is checked against the same contract the bundled backends satisfy.

### Aggregates

`eventsource.domain.aggregate` holds the consistency boundary: `AggregateRoot`, the base
class that turns commands into events and events into state, and `DeclarativeAggregate`,
which routes events to `@handles`-decorated methods. `AggregateRepository`, which loads
and saves aggregates through an `EventStore`, lives one ring out at
`eventsource.application.aggregates.repository`. All three are re-exported from the
barrel, as is `handles` (whose canonical home is `eventsource.handlers`). The type
variable `TAggregate` is exported from `eventsource.application.aggregates` but not from
the barrel.

#### `AggregateRoot`

`AggregateRoot(Generic[TState], ABC)` — the parameter `TState` is bound to
`pydantic.BaseModel`, so an aggregate's state is a Pydantic model. The constructor takes
one argument, `aggregate_id: UUID`; version starts at `0` and state at `None`.

Two methods are abstract: `_apply(event) -> None`, which updates state for an event, and
`_get_initial_state() -> TState | None`.

**Class attributes.** Configured by overriding in the subclass body:

| Attribute | Default | Meaning |
| --- | --- | --- |
| `aggregate_type` | `"Unknown"` | Type name written onto events and used by the repository. Subclasses are expected to override it |
| `schema_version` | `1` | Schema version of `TState`. Increment when the model changes incompatibly; snapshots whose `schema_version` differs are discarded |
| `validate_versions` | `True` | Whether a new event with an unexpected `aggregate_version` raises `EventVersionError` or only logs a warning |

**Properties.** `aggregate_id`, `version` (the version of the last applied event),
`state` (`TState | None`), `uncommitted_events` (a *copy* of the pending list, so
mutating it has no effect), and `has_uncommitted_events`.

**Applying events.** `apply_event(event, is_new=True)` is the single entry point. For
`is_new=True` it first checks that `event.aggregate_version == self.version + 1`; on
mismatch it raises `EventVersionError(expected_version, actual_version, event_id,
aggregate_id)` when `validate_versions` is `True`, or logs a warning and continues when
it is `False`. It then sets `self._version = event.aggregate_version`, calls `_apply`,
and — only for `is_new=True` — appends the event to the uncommitted list. Replay passes
`is_new=False`, which skips both the version check and the tracking.

`load_from_history(events)` replays a list in order with `is_new=False`.
`get_next_version()` returns `version + 1`. `_raise_event(event)` is an alias for
`apply_event(event, is_new=True)` that reads better inside command methods.

**Recording events with less boilerplate.** `create_event(event_class, **kwargs)`
constructs *and applies* an event, auto-populating `aggregate_id`, `aggregate_type`, and
`aggregate_version=get_next_version()`, plus `tenant_id` from the ambient tenant context
when one is set and `tenant_id` was not passed explicitly. Explicit keyword arguments
override every auto-populated value. It returns the created event.

```python
from eventsource import DeclarativeAggregate, handles


class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"
    requires_creation_event = True

    def ship(self, tracking_number: str) -> None:
        if self.state.status != "paid":
            raise ValueError("Cannot ship unpaid order")
        self.create_event(OrderShipped, tracking_number=tracking_number)
```

The tenant lookup is a lazy `importlib` import of `eventsource.multitenancy` inside a
`try`/`except ImportError`, so aggregates carry no hard dependency on the multitenancy
module and simply see `None` when it is unavailable or no tenant is set.

**Committing.** `mark_events_as_committed()` clears the uncommitted list;
`clear_uncommitted_events()` clears it and returns what it cleared. The repository calls
the former after a successful append.

**Snapshot hooks.** `_serialize_state()` returns `self._state.model_dump(mode="json")`,
or `{}` when state is `None`. `_restore_from_snapshot(state_dict, version)` validates the
dict into the state model and sets the version; an empty dict only sets the version.
`_get_state_type()` recovers the concrete `TState` by walking the MRO's `__orig_bases__`
and raises `RuntimeError` if the class was not parameterized as `AggregateRoot[StateType]`.
These are called by `AggregateRepository`, not by application code.

**Identity.** `__eq__` and `__hash__` are defined on `aggregate_id` alone — two instances
of different aggregate classes sharing an ID compare equal. `__repr__` renders the class
name, id, version, and uncommitted count.

#### `DeclarativeAggregate`

`DeclarativeAggregate(AggregateRoot[TState], ABC)` implements `_apply` for you by
dispatching on `type(event)` through a per-subclass handler registry.
`__init_subclass__` scans the class for methods carrying the `_handles_event_type`
attribute set by `@handles` and records them in a fresh `_event_handlers` dict — fresh
per subclass, so registries are not shared through inheritance.

Aggregate handlers are **synchronous** and take `(self, event)`. (Projection handlers,
which use the same decorator, are async and take `(self, context, event)`.)

Two `ClassVar` knobs:

| Attribute | Default | Meaning |
| --- | --- | --- |
| `requires_creation_event` | `False` | When `True`, `_get_initial_state()` need not be implemented — it returns `None` and the first handler establishes state |
| `unregistered_event_handling` | `"ignore"` | What `_apply` does with an event that has no handler: `"ignore"` (silent), `"warn"` (log, listing the registered handlers), or `"error"` (raise `UnhandledEventError`) |

`"ignore"` is the default because forward compatibility usually matters more than
strictness: a replay that encounters a newer event type keeps working. Set `"error"`
during development to catch a missing `@handles` or a mistyped handler.

When `requires_creation_event` is `False` and the subclass does not override
`_get_initial_state()`, the inherited implementation raises `NotImplementedError`.

**State access.** `DeclarativeAggregate` narrows `state` to `TState` and raises
`AggregateNotCreatedError` when `requires_creation_event=True` and no event has been
applied yet. Two companions avoid the exception: `state_or_none` returns `TState | None`,
and `is_created` returns whether any state exists.

```python
if order.state_or_none is None:
    order.create(customer_id)
else:
    order.ship(tracking_number)
```

#### `handles`

`handles(event_type)` sets `func._handles_event_type = event_type` and returns the
function unchanged, so a decorated method is still an ordinary method. Discovery happens
in the base class's `__init_subclass__`. The companion helpers
`get_handled_event_type(func)` (returns the type or `None`) and `is_event_handler(func)`
are importable from `eventsource.handlers`.

#### `AggregateRepository`

`AggregateRepository(Generic[TAggregate])` mediates between aggregates and the store.
`TAggregate` is bound to `AggregateRoot[Any]`.

```python
from eventsource import AggregateRepository, InMemoryEventStore

repo: AggregateRepository[OrderAggregate] = AggregateRepository(
    event_store=InMemoryEventStore(),
    aggregate_factory=OrderAggregate,
    # aggregate_type inferred from OrderAggregate.aggregate_type
)
```

**Constructor.** `event_store`, `aggregate_factory` (the aggregate *class*, called with
one `UUID`), then optional `aggregate_type`, `event_publisher`, `snapshot_store`,
`snapshot_threshold`, `snapshot_mode`, `tracer`, and `enable_tracing`.

`aggregate_type` is inferred from `aggregate_factory.aggregate_type` when omitted; an
explicit argument wins. Inference deliberately rejects `""` and the base default
`"Unknown"` and raises `ValueError` with instructions, so a subclass that forgot to set
`aggregate_type` fails at construction rather than writing events under a useless type.

**Methods.**

| Member | Behavior |
| --- | --- |
| `load(aggregate_id)` | Restores from a valid snapshot when one exists, reads events from that version (or `0`), replays them, and returns the aggregate. Raises `AggregateNotFoundError` when there is neither snapshot nor event |
| `load_or_create(aggregate_id)` | `load`, falling back to a fresh version-0 instance on `AggregateNotFoundError` |
| `get_or_raise(aggregate_id)` | An alias for `load`, for call sites where the intent reads better |
| `create_new(aggregate_id)` | A new in-memory instance; persists nothing |
| `save(aggregate)` | Appends the uncommitted events, then commits, publishes, and possibly snapshots |
| `exists(aggregate_id)` | Whether the aggregate's stream has any events |
| `get_version(aggregate_id)` | The stream's current version, `0` if absent |
| `create_snapshot(aggregate)` | Snapshots immediately, whatever the mode; raises `RuntimeError` without a `snapshot_store` |
| `await_pending_snapshots()` | Awaits outstanding background snapshot tasks, returning how many; `0` without a snapshot store |

Read-only properties: `aggregate_type`, `event_store`, `event_publisher`,
`snapshot_store`, `snapshot_threshold`, `snapshot_mode`, `has_snapshot_support`, and
`pending_snapshot_count`.

**Save semantics.** `save` returns immediately when there is nothing uncommitted. The
expected version it passes to the store is `aggregate.version - len(uncommitted_events)`
— the version the stream had before this unit of work — which is what makes the store's
optimistic-locking check meaningful; a conflict surfaces as `OptimisticLockError` from
the store. Only after a successful append does the repository call
`mark_events_as_committed()`, await `event_publisher.publish(events)` if a publisher was
supplied, and consult the snapshot strategy. The publish is *not* in the store's
transaction; where that atomicity matters, use the outbox instead of a publisher.

**Load and snapshots.** Snapshot loading is best-effort at every step. A snapshot store
that raises, a missing snapshot, or a `schema_version` that does not match the aggregate
class all yield "no snapshot" and a full replay from version 0 rather than an error. If
the snapshot is found but `_restore_from_snapshot` fails to deserialize, the repository
logs a warning, re-fetches the whole stream, and rebuilds a fresh instance — so a stale
or corrupt snapshot degrades performance, never correctness.

**Snapshot modes.** With a `snapshot_store` configured, `snapshot_mode` selects when
automatic snapshots happen: `"sync"` (default) writes one inline after `save`,
`"background"` schedules a task, and `"manual"` never writes one automatically.
`snapshot_threshold` is the event count between automatic snapshots; leaving it `None`
means snapshots only happen through `create_snapshot()`. Snapshot failures are logged
and do not fail the save. `create_snapshot` upserts — an existing snapshot for the
aggregate is replaced.

`load`, `save`, `exists`, and `create_snapshot` emit OpenTelemetry spans when
`enable_tracing` is `True` and the `telemetry` extra is installed; without it the tracer
is a no-op.

#### Related exceptions

| Exception | Raised when |
| --- | --- |
| `EventVersionError` | A new event's `aggregate_version` is not `version + 1` and `validate_versions=True` |
| `UnhandledEventError` | A `DeclarativeAggregate` with `unregistered_event_handling="error"` applies an event with no handler |
| `AggregateNotCreatedError` | `state` is read on a `requires_creation_event=True` aggregate before any event |
| `AggregateNotFoundError` | `AggregateRepository.load` finds neither snapshot nor events |
| `OptimisticLockError` | The store rejects the append in `save` because the stream moved on |

All five subclass `EventSourceError` and are re-exported from the barrel except
`UnhandledEventError`, which is imported from `eventsource.exceptions`.

### Types (`TState`, `AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId`)

Six names from `eventsource.types` are re-exported at the top level. Five are plain
type aliases; `TState` is a `TypeVar`. The full module definition is three lines of
imports and eight assignments:

```python
TState = TypeVar("TState", bound=BaseModel)

AggregateId = UUID
EventId = UUID
TenantId = UUID | None
CorrelationId = UUID
CausationId = UUID | None
```

| Name | Definition | Kind |
| --- | --- | --- |
| `TState` | `TypeVar("TState", bound=pydantic.BaseModel)` | Type variable |
| `AggregateId` | `UUID` | Alias |
| `EventId` | `UUID` | Alias |
| `TenantId` | `UUID \| None` | Alias |
| `CorrelationId` | `UUID` | Alias |
| `CausationId` | `UUID \| None` | Alias |

#### The aliases are aliases, not new types

None of these are `NewType` or a subclass. `AggregateId` *is* `uuid.UUID` — the same
object, evaluated at import time — so `AggregateId is UUID` holds, `isinstance(x,
AggregateId)` is just an isinstance check against `UUID`, and calling
`AggregateId("...")` constructs an ordinary `UUID`. They carry no runtime behavior and
no validation of their own.

They also give no type-checker separation: because `AggregateId`, `EventId`, and
`CorrelationId` all resolve to `UUID`, mypy accepts an `EventId` wherever an
`AggregateId` is expected. The aliases exist to make signatures self-describing, not to
prevent mixing up identifiers. If you need that separation in your own domain code,
define your own `NewType` on top.

Two of the six are nullable, and that nullability is the meaningful part of their
definition:

- `TenantId` is `UUID | None` — `None` means "no tenant", the single-tenant or
  tenant-unscoped case.
- `CausationId` is `UUID | None` — `None` means the event was not caused by another
  event, i.e. it originates a causation chain.

`AggregateId`, `EventId`, and `CorrelationId` are non-optional: every event has all
three.

#### Where the aliases correspond to event fields

The alias names mirror the identity fields on `DomainEvent`, which is where the
vocabulary comes from:

| Alias | `DomainEvent` field | Field annotation |
| --- | --- | --- |
| `EventId` | `event_id` | `UUID` (defaults to `uuid4()`) |
| `AggregateId` | `aggregate_id` | `UUID` (required) |
| `TenantId` | `tenant_id` | `UUID \| None` |
| `CorrelationId` | `correlation_id` | `UUID` |
| `CausationId` | `causation_id` | `UUID \| None` |

Note that `DomainEvent` declares those fields with the bare `UUID` / `UUID | None`
annotations rather than the aliases. Since the aliases are equal to those types, the
distinction is stylistic — the fields and the aliases describe exactly the same values.

`correlation_id` and `causation_id` are the pair used for tracing: `caused_by()` copies
the causing event's `correlation_id` and sets `causation_id` to the causing event's
`event_id`, so a correlation id spans a whole workflow while a causation id points one
step back. See the events reference for the full field semantics.

#### `TState` and generic aggregates

`TState` is the type variable that parameterizes the aggregate hierarchy. Its bound is
`pydantic.BaseModel`, so aggregate state must be a Pydantic model — this is what lets
`AggregateRoot` serialize state into snapshots and validate it back out.

```python
from pydantic import BaseModel

from eventsource import AggregateRoot


class OrderState(BaseModel):
    order_number: str
    total: int


class Order(AggregateRoot[OrderState]):
    ...
```

`AggregateRoot` is declared as `Generic[TState]` and `DeclarativeAggregate` as
`AggregateRoot[TState]`, so the parameter flows through both. It also shapes the state
accessors, and the two base classes differ here:

- `AggregateRoot.state` returns `TState | None` — state is `None` before the creating
  event has been applied.
- `DeclarativeAggregate.state` returns `TState` (non-optional). It raises
  `AggregateNotCreatedError` when `requires_creation_event` is true and no event has
  been applied yet; `state_or_none` returns `TState | None` for the cases where absence
  is expected.

The concrete argument is recovered at runtime, not only by the type checker:
`_get_state_type()` walks the class's generic bases to find the type substituted for
`TState`, and uses it to deserialize snapshot payloads. A subclass that never
parameterizes its base — `class Order(AggregateRoot)` with no `[OrderState]` — makes
that lookup fail with a `RuntimeError`, so parameterize the base even when you are not
type-checking.

Because the bound is enforced only by the type checker, substituting a non-`BaseModel`
type is a static error rather than a runtime one — but snapshot round-tripping calls
`model_validate` on the recovered type, so it will fail there.

#### Aliases not re-exported at the top level

`eventsource.types` defines three further aliases that are **not** in the package-level
`__all__`:

```python
from eventsource.types import GlobalPosition, StreamPosition, Version
```

All three are `int`: `Version` for optimistic-locking versions, `StreamPosition` for a
position within one stream, and `GlobalPosition` for a position in the global ordering.
Import them from `eventsource.types` directly; see the
[types and protocols reference](types.md) for the whole module.

### Events and event registry (`DomainEvent`, `EventRegistry`, `default_registry`, registry helpers, `EventTypeNotFoundError`, `DuplicateEventTypeError`)

Ten names in this group are re-exported at the top level. They come from two modules:
`DomainEvent` from `eventsource.events.base`, and everything else from
`eventsource.events.registry`.

| Name | Kind | Purpose |
| --- | --- | --- |
| `DomainEvent` | `pydantic.BaseModel` subclass | Frozen base class for all domain events |
| `EventRegistry` | Class | Thread-safe mapping of event-type name to event class |
| `default_registry` | `EventRegistry` instance | Module-level singleton used by the helpers |
| `register_event` | Decorator / function | Registers an event class in a registry |
| `get_event_class` | Function | `default_registry.get` — raises when missing |
| `get_event_class_or_none` | Function | `default_registry.get_or_none` — returns `None` |
| `is_event_registered` | Function | `default_registry.contains` |
| `list_registered_events` | Function | Sorted list of registered type names |
| `EventTypeNotFoundError` | Exception (`KeyError`) | Lookup of an unregistered type |
| `DuplicateEventTypeError` | Exception (`ValueError`) | Two different classes claim one type name |

#### `DomainEvent`

`DomainEvent` is a Pydantic v2 model with `model_config = ConfigDict(frozen=True)`.
Instances are immutable: attribute assignment raises, and every mutation helper returns
a new instance via `model_copy`.

Its fields, all declared on the base class:

| Field | Type | Default |
| --- | --- | --- |
| `event_id` | `UUID` | `uuid4()` |
| `event_type` | `str` | `""`, replaced by the class name (see below) |
| `event_version` | `int` (`ge=1`) | `1` |
| `occurred_at` | `datetime` | `datetime.now(UTC)` |
| `aggregate_id` | `UUID` | **required** |
| `aggregate_type` | `str` | **required** |
| `aggregate_version` | `int` (`ge=1`) | `1` |
| `tenant_id` | `UUID \| None` | `None` |
| `actor_id` | `str \| None` | `None` |
| `correlation_id` | `UUID` | `uuid4()` |
| `causation_id` | `UUID \| None` | `None` |
| `metadata` | `dict[str, Any]` | `{}` |

`aggregate_id` and `aggregate_type` are the only two fields with no default. Subclasses
conventionally pin `aggregate_type` with a class-level default so callers need only pass
`aggregate_id` plus the event's own payload fields.

There is also one `ClassVar`, `suppress_event_type_warning: bool = False`, which is not a
model field.

##### `event_type` is derived from the class name

`DomainEvent.__init_subclass__` inspects the subclass's own `__dict__`. If the subclass
did **not** declare `event_type`, the hook rewrites the field default to `cls.__name__`;
if it did declare a string that differs from the class name, the hook emits a
`logging.warning` unless the subclass also sets `suppress_event_type_warning = True`.

```python
from uuid import UUID, uuid4

from eventsource import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID


event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", customer_id=uuid4())
assert event.event_type == "OrderCreated"
```

A `model_validator(mode="before")` named `_ensure_event_type` covers the dict-construction
paths (`model_validate`, `from_dict`, `Event(**data)`): when the incoming data omits
`event_type` or supplies an empty string, and the field default is empty, it substitutes
the class name. An explicit non-empty default set by the subclass is preserved.

Note that `__init_subclass__` performs **no registration**. Defining an event subclass
does not add it to `default_registry` — registration is always explicit, via
`@register_event` or `EventRegistry.register`.

##### Instance methods

All four `with_*`/`to_*` helpers are non-mutating:

| Method | Returns | Behavior |
| --- | --- | --- |
| `with_causation(causing_event)` | `Self` | Copy with `causation_id = causing_event.event_id` and `correlation_id = causing_event.correlation_id` |
| `with_metadata(**kwargs)` | `Self` | Copy with `kwargs` merged over the existing `metadata` |
| `with_aggregate_version(version)` | `Self` | Copy with `aggregate_version` replaced; called by aggregates when recording |
| `to_dict()` | `dict[str, Any]` | `model_dump(mode="json")` — UUIDs become strings, datetimes ISO strings |
| `from_dict(data)` (classmethod) | `Self` | `model_validate(data)`; raises `pydantic.ValidationError` on mismatch |
| `is_caused_by(event)` | `bool` | `self.causation_id == event.event_id` |
| `is_correlated_with(event)` | `bool` | `self.correlation_id == event.correlation_id` |

Because `with_*` methods go through `model_copy(update=...)`, the update values are not
re-validated — they are assigned directly onto the copy.

`__str__` renders `"{event_type}(event_id=..., aggregate_id=..., version=...)"`;
`__repr__` additionally includes `aggregate_type`, `tenant_id`, and `occurred_at`.

#### `EventRegistry` and `default_registry`

An `EventRegistry` is a `dict[str, type[DomainEvent]]` guarded by a `threading.RLock`.
All operations take the lock, so the registry is safe to read and write from multiple
threads.

| Member | Signature | Notes |
| --- | --- | --- |
| `register` | `(event_class, event_type=None) -> type[TEvent]` | Returns the class, so it works as a decorator |
| `get` | `(event_type) -> type[DomainEvent]` | Raises `EventTypeNotFoundError` |
| `get_or_none` | `(event_type) -> type[DomainEvent] \| None` | |
| `contains` | `(event_type) -> bool` | Also reachable as `event_type in registry` |
| `list_types` | `() -> list[str]` | Sorted |
| `list_classes` | `() -> list[type[DomainEvent]]` | Ordered by type name |
| `unregister` | `(event_type) -> bool` | `True` if it was present |
| `clear` | `() -> None` | Resets the registry; intended for tests |

Dunder behavior: `len(registry)` is the number of entries, `iter(registry)` yields type
names from a snapshot copy (safe against concurrent mutation), and `bool(registry)` is
**always `True`**, including when empty — check `len(registry)` if you mean emptiness.

Type-name resolution in `register` follows a three-step order:

1. the explicit `event_type` argument, if given;
2. the class's `model_fields["event_type"].default`, if it is a `str` (for a subclass
   that did not override it, `__init_subclass__` has already set this to the class name);
3. `event_class.__name__`.

Re-registering the *same* class under the same name is a silent no-op. Registering a
*different* class under a name already taken raises `DuplicateEventTypeError`.

`default_registry` is a module-level `EventRegistry` created at import time. It is
process-global shared state: the four helper functions all delegate to it, and clearing
it affects every consumer in the process. Construct a private `EventRegistry()` when you
need isolation — for example, per-test registries.

#### `register_event`

`register_event` is an overloaded decorator usable bare or called:

```python
from eventsource import DomainEvent, EventRegistry, register_event


@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"


@register_event(event_type="order.shipped")
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"


test_registry = EventRegistry()


@register_event(registry=test_registry)
class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
```

Signature: `register_event(event_class=None, *, event_type=None, registry=None)`. Both
keyword arguments are keyword-only. `registry` defaults to `default_registry`. Called
with a class positionally it registers immediately and returns the class; called with
keywords only it returns a decorator.

Note that `event_type` here overrides only the *registry key*. It does not change the
class's `event_type` field, so a class registered as `"order.shipped"` still produces
instances whose `event_type` is `"OrderShipped"` unless the field default is set to match.
Keeping the two aligned is what makes a round trip through `to_dict()` and
`get_event_class(data["event_type"])` resolve back to the same class.

#### Module-level helpers

Four thin wrappers over `default_registry`:

```python
from eventsource import (
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
)

get_event_class("OrderCreated")          # type[DomainEvent]; raises if unknown
get_event_class_or_none("Nope")          # None
is_event_registered("OrderCreated")      # True
list_registered_events()                 # ['OrderCreated', 'OrderShipped', ...]
```

The typical deserialization path pairs `get_event_class` with `from_dict`:

```python
payload = stored.to_dict()
event_class = get_event_class(payload["event_type"])
event = event_class.from_dict(payload)
```

#### `EventTypeNotFoundError` and `DuplicateEventTypeError`

Both are defined in `eventsource.events.registry` and — unlike most exceptions in this
library — do **not** inherit from `EventSourceError`. They extend builtins instead, so
`except KeyError` and `except ValueError` catch them:

| Exception | Base | Attributes |
| --- | --- | --- |
| `EventTypeNotFoundError` | `KeyError` | `event_type`, `available_types` |
| `DuplicateEventTypeError` | `ValueError` | `event_type`, `existing_class`, `new_class` |

`EventTypeNotFoundError`'s message lists the sorted available types (or `none`) to make
missing-registration bugs self-diagnosing. `DuplicateEventTypeError` names both the
already-registered class and the one being rejected; the usual cause is two distinct
classes sharing a name, or one module imported twice under different paths so that its
event class object is created twice.

### Event store interface and data structures (`EventStore`, `EventPublisher`, `EventStream`, `AppendResult`, `StoredEvent`, `ReadOptions`, `ReadDirection`, `ExpectedVersion`)

Eight names in this group are re-exported at the top level. All eight are defined in the
single module `eventsource.stores.interface`, which depends only on `pydantic` (through
`DomainEvent`) and the standard library — no database driver is involved in importing it.

| Name | Kind | Purpose |
| --- | --- | --- |
| `EventStore` | ABC | The write/read contract every store implementation satisfies |
| `EventPublisher` | `typing.Protocol` | Structural contract for anything that publishes events onward |
| `EventStream` | Frozen dataclass | One aggregate's events plus its version |
| `AppendResult` | Frozen dataclass | Outcome of an append |
| `StoredEvent` | Frozen dataclass | A persisted event plus stream/global position metadata |
| `ReadOptions` | Frozen dataclass | Direction, position, limit, timestamp, and tenant filters for reads |
| `ReadDirection` | `enum.Enum` | `FORWARD` / `BACKWARD` |
| `ExpectedVersion` | Plain class of `int` constants | Sentinels for the `expected_version` argument |

#### `EventStore`

`EventStore` is an `abc.ABC` with five abstract methods and three concrete ones. A
subclass must implement the abstract five; the concrete three have working defaults that
implementations are free to override.

| Method | Abstract? | Signature (return type) |
| --- | --- | --- |
| `append_events` | yes | `(aggregate_id: UUID, aggregate_type: str, events: list[DomainEvent], expected_version: int) -> AppendResult` |
| `get_events` | yes | `(aggregate_id: UUID, aggregate_type: str \| None = None, from_version: int = 0, from_timestamp: datetime \| None = None, to_timestamp: datetime \| None = None) -> EventStream` |
| `get_events_by_type` | yes | `(aggregate_type: str, tenant_id: UUID \| None = None, from_timestamp: datetime \| None = None) -> list[DomainEvent]` |
| `event_exists` | yes | `(event_id: UUID) -> bool` |
| `get_global_position` | yes | `() -> int` |
| `get_stream_version` | no | `(aggregate_id: UUID, aggregate_type: str) -> int` |
| `read_stream` | no | `(stream_id: str, options: ReadOptions \| None = None) -> AsyncIterator[StoredEvent]` |
| `read_all` | no | `(options: ReadOptions \| None = None) -> AsyncIterator[StoredEvent]` |

Every method is a coroutine or an async generator. The two async generators
(`read_stream`, `read_all`) are consumed with `async for` and must not be awaited
directly.

The three non-abstract methods differ in how usable their defaults are:

- **`get_stream_version`** calls `get_events()` and returns `stream.version`. Correct
  everywhere, but it materializes the whole stream; backends override it with a
  `MAX(version)` query.
- **`read_stream`** splits `stream_id` on the last `:` into `aggregate_id` and
  `aggregate_type`, calls `get_events()`, reverses the list for
  `ReadDirection.BACKWARD`, truncates to `options.limit`, and yields `StoredEvent`
  wrappers. Two caveats in the default: `global_position` is always `0` because the
  default has no access to global ordering, and `stored_at` is filled from
  `event.occurred_at` rather than a real persistence timestamp. A `stream_id` with no
  colon is parsed as a bare `UUID` with `aggregate_type=None`, so a malformed value
  raises `ValueError` from `UUID()`.
- **`read_all`** raises `NotImplementedError` by default. Global ordering is treated as
  an optional capability; `InMemoryEventStore` implements it.

##### `append_events` signals conflicts by raising, not by returning

The docstring documents both a returned `AppendResult` and an `OptimisticLockError`, and
the two shipped backends consistently choose the exception. `InMemoryEventStore`,
`PostgreSQLEventStore`, and `SQLiteEventStore` all raise `OptimisticLockError` on a
version mismatch rather than returning `AppendResult.conflicted(...)`. Write the
`except` clause, not the `if result.conflict` branch:

```python
from eventsource import OptimisticLockError

try:
    result = await store.append_events(
        aggregate_id=order_id,
        aggregate_type="Order",
        events=[event],
        expected_version=5,
    )
except OptimisticLockError:
    ...  # reload and retry
```

`AppendResult.conflict` remains part of the data type for implementations that prefer to
report conflicts as values; nothing in the library produces it today.

One more behavior worth knowing: an empty `events` list is not an error in the shipped
backends. `InMemoryEventStore` and `PostgreSQLEventStore` short-circuit with
`AppendResult.successful(expected_version)` before any version check, so no conflict can
be raised for an empty append. (The abstract docstring mentions `ValueError` for an empty
list; the concrete stores do not raise it.)

Appends are also idempotent at the event level: an event whose `event_id` is already
present is skipped rather than duplicated.

#### `ExpectedVersion`

`ExpectedVersion` is a plain class holding three `int` class attributes — not an enum, so
the values are ordinary integers and compare equal to the integers you pass:

```python
class ExpectedVersion:
    ANY: int = -1
    NO_STREAM: int = 0
    STREAM_EXISTS: int = -2
```

| Constant | Value | Meaning when passed as `expected_version` |
| --- | --- | --- |
| `ANY` | `-1` | Skip the version check entirely |
| `NO_STREAM` | `0` | The stream must not exist; raises if `current_version != 0` |
| `STREAM_EXISTS` | `-2` | The stream must exist; raises if `current_version == 0` |

Any other integer is treated as an exact version: the append succeeds only if the
aggregate's current version equals it. All three branches are implemented identically in
the in-memory, PostgreSQL, and SQLite stores.

Note that `NO_STREAM` is `0`, the same value used for "this is a brand-new aggregate at
version 0" — the two readings coincide, which is why `expected_version=0` is the
idiomatic argument when creating an aggregate. `ANY` is `-1`, which means passing `-1`
because you meant "version minus one" silently disables optimistic locking.

#### `EventStream`

A frozen dataclass returned by `get_events()`.

| Field | Type | Default |
| --- | --- | --- |
| `aggregate_id` | `UUID` | required |
| `aggregate_type` | `str` | required |
| `events` | `list[DomainEvent]` | `[]` (via `default_factory`) |
| `version` | `int` | `0` |

Events are in chronological order, oldest first. `version` is the aggregate version after
all of them have been applied — for a store that assigns one version per event, it equals
`len(events)` for a full read, but it is a separate field and a partial read
(`from_version=...`) does not make it a count of the returned slice.

Members: `is_empty` (property, `len(self.events) == 0`), `latest_event` (property,
`events[-1]` or `None`), and the classmethod `EventStream.empty(aggregate_id,
aggregate_type)` which builds a stream with no events and `version=0`.

`__post_init__` substitutes `[]` if `events` was explicitly passed as `None`, using
`object.__setattr__` to work around the frozen dataclass. Note that "frozen" here means
the *fields* cannot be rebound — the `events` list itself is a mutable list and is not
copied on construction.

#### `AppendResult`

A frozen dataclass returned by `append_events()`.

| Field | Type | Default |
| --- | --- | --- |
| `success` | `bool` | required |
| `new_version` | `int` | required |
| `global_position` | `int` | `0` |
| `conflict` | `bool` | `False` |

Two classmethod constructors:

- `AppendResult.successful(new_version, global_position=0)` → `success=True`,
  `conflict=False`.
- `AppendResult.conflicted(current_version)` → `success=False`, `conflict=True`,
  `global_position=0`, and `new_version` set to the *actual* current version.

`new_version` therefore means "the aggregate version after this append" on success and
"the version that was actually there" on a conflict. As noted above, the shipped stores
never return the conflicted form.

#### `StoredEvent`

A frozen dataclass wrapping a `DomainEvent` with persistence metadata. It is what
`read_stream()` and `read_all()` yield; `get_events()` returns bare `DomainEvent`s
instead.

| Field | Type | Notes |
| --- | --- | --- |
| `event` | `DomainEvent` | The wrapped event |
| `stream_id` | `str` | Conventionally `"{aggregate_id}:{aggregate_type}"` |
| `stream_position` | `int` | 1-based position within the aggregate's stream |
| `global_position` | `int` | 1-based position across the whole store |
| `stored_at` | `datetime` | When the event was persisted |

Four read-only properties forward to the wrapped event: `event_id`, `event_type`,
`aggregate_id`, and `aggregate_type`. Anything else — the event's payload fields,
`metadata`, `correlation_id` — is reached through `stored.event`.

`__str__` renders `"StoredEvent({event_type}, stream_pos=..., global_pos=...)"`.

`stored_at` is distinct from `event.occurred_at`: the former is a storage timestamp, the
latter is when the event was created in the domain. The default `read_stream()`
implementation cannot distinguish them and copies `occurred_at` into both.

#### `ReadOptions` and `ReadDirection`

`ReadDirection` is a two-member `Enum` with string values `"forward"` and `"backward"`.
`ReadOptions` is the frozen dataclass that carries it, along with the rest of the read
filters. Every field has a default, so `ReadOptions()` is valid and means "everything,
forward, from the beginning".

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `direction` | `ReadDirection` | `FORWARD` | Read order |
| `from_position` | `int` | `0` | Starting position; `-1` means "from the end" |
| `limit` | `int \| None` | `None` | Maximum events; `None` for unlimited |
| `from_timestamp` | `datetime \| None` | `None` | Only events after this instant |
| `to_timestamp` | `datetime \| None` | `None` | Only events before this instant |
| `tenant_id` | `UUID \| None` | `None` | Restrict to one tenant; `None` means all tenants |

`__post_init__` validates two of them and raises `ValueError`:

- `from_position` must be `>= 0`, with `-1` allowed as the "end" sentinel. Any other
  negative value is rejected.
- `limit`, when not `None`, must be `>= 0`. A `limit` of `0` is legal and yields nothing.

```python
from eventsource import ReadDirection, ReadOptions

ReadOptions(limit=100)                                        # first 100, forward
ReadOptions(direction=ReadDirection.BACKWARD, limit=10)       # last 10
ReadOptions(from_position=50, limit=100)                      # a window
ReadOptions(tenant_id=tenant_uuid)                            # one tenant, all events
```

Note that `tenant_id=None` is "all tenants", not "the untenanted events" — the field
cannot express a filter for events whose own `tenant_id` is `None`.

#### `EventPublisher`

`EventPublisher` is a `typing.Protocol` with a single method:

```python
async def publish(self, events: list[DomainEvent]) -> None: ...
```

Because it is a Protocol, conformance is structural: any object with a matching async
`publish` satisfies it without inheriting from anything. It is not decorated with
`@runtime_checkable`, so `isinstance(obj, EventPublisher)` raises `TypeError` — use it as
a type annotation, not a runtime check.

```python
from eventsource import DomainEvent, EventPublisher


class NotificationPublisher:
    async def publish(self, events: list[DomainEvent]) -> None:
        for event in events:
            await send_notification(event)


def wire(publisher: EventPublisher) -> None: ...
```

It is deliberately narrower than `EventBus`: a publisher only pushes events outward and
takes a *batch*, whereas `EventBus` also handles subscription. `EventBus` implementations
satisfy the protocol only if they expose a matching `publish` — check the bus reference
before relying on the substitution.
