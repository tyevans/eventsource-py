# How to choose an event store backend

`eventsource` ships three event store adapters, structurally conforming to the
`FullEventStore` protocol in `eventsource.ports.store` (no shared ABC —
conformance is duck-typed): `InMemoryEventStore`, `SQLiteEventStore`, and
`PostgreSQLEventStore`. They implement the same five store ports (`append`,
`read_stream`, `read_category`, `read_all`, `event_exists`, plus
`current_position` / `get_stream_version`), so the choice is an infrastructure
decision — your aggregates, projections, and repositories do not change when
you switch.

Use this guide to pick a backend for a specific situation and wire it up:

- **Tests and examples** — `InMemoryEventStore()` takes no dependencies, needs
  no extra, and requires no setup or teardown.
- **Local development and single-node deployments** — `SQLiteEventStore` needs
  the `sqlite` extra (aiosqlite), self-initializes on first use (no
  `initialize()` call, no context manager), and is only importable from
  `eventsource.adapters.sqlite` when aiosqlite is installed.
- **Production** — `PostgreSQLEventStore` needs the `postgresql` extra
  (asyncpg), takes a SQLAlchemy `AsyncEngine` you own, and is the only backend
  with the transactional outbox (`outbox_enabled=True`).

Each backend section below covers install, construction, lifecycle, and the
caveats that tend to bite. If you only need to know which one to reach for,
read [Decide which backend fits](#decide-which-backend-fits) and stop there.

## Before you start

This guide assumes you already have a working `eventsource` install and some
`DomainEvent` subclasses to store. Specifically:

- **Python 3.11+** with `eventsource-py` installed. The base install pulls only
  `pydantic` and `sqlalchemy`; every backend beyond `InMemoryEventStore`
  requires an optional extra (`sqlite` or `postgresql`), installed separately.
- **An async runtime.** The store ports are async throughout (`append`,
  `read_stream`, `read_all`), so all examples here run inside `asyncio` —
  under `asyncio.run(...)`, an async web framework, or `pytest-asyncio`. If
  you need to call a store from synchronous code, wrap it with
  `SyncEventStoreAdapter` instead of changing backends.
- **Your event classes importable at startup.** `DomainEvent` subclasses
  self-register into an `EventRegistry` when their module is imported (via
  `@register_event`). Every backend deserializes stored rows through a
  registry, so a module that is never imported means events that never come
  back out.
- **For `PostgreSQLEventStore`: a reachable PostgreSQL server and its schema
  already provisioned**, unless you pass `create_schema=True`. Apply the SQL
  under `src/eventsource/adapters/sql/schemas/schemas/` (or your own migration tool)
  before first use, or let the adapter create it for you. For local work, the
  repo's `docker-compose.test.yml` brings up a suitable instance.

You do not need to decide permanently. Because all three backends satisfy the
same ports, swapping one for another is a change at your composition root,
not in your domain code.

## Decide which backend fits

Pick by deployment shape, then confirm the constraints below match what you
need:

- **Choose `InMemoryEventStore`** when the store's lifetime is the process's
  lifetime: unit tests, examples, and prototypes. No extra to install, nothing
  to provision, nothing to tear down.
- **Choose `SQLiteEventStore`** when you want durability on one machine with no
  server to run: local development, CLI tools, embedded or single-node
  deployments. It self-initializes its schema on first use.
- **Choose `PostgreSQLEventStore`** when more than one process writes, or when
  you need the transactional outbox. It is the only backend that writes outbox
  rows in the same transaction as the events.

Two constraints decide most cases:

- **Do multiple processes write to the same streams?** If yes, PostgreSQL. The
  in-memory store is per-instance, and SQLite serializes all writers behind a
  single connection — safe, but not a scaling story.
- **Do you need reliable publishing via the outbox?** If yes, PostgreSQL with
  `outbox_enabled=True`. Only `PostgreSQLEventStore` writes outbox rows on
  append, in the same transaction as the event insert; neither
  `InMemoryEventStore` nor `SQLiteEventStore` supports an outbox.

Everything else — optimistic concurrency via `ExpectedVersion`, `read_stream`,
`read_all`, and stream identity via `StreamId` — behaves the same across all
three.

### Comparison at a glance

Use this table to confirm a choice you have already narrowed down.

| | `InMemoryEventStore` | `SQLiteEventStore` | `PostgreSQLEventStore` |
|---|---|---|---|
| **Persistence** | None — state lives in the instance and dies with it | Durable file, or `:memory:` (per connection, non-durable) | Durable, managed by the PostgreSQL server |
| **Install extra** | None; part of the base install | `pip install eventsource-py[sqlite]` (aiosqlite) | `pip install eventsource-py[postgresql]` (asyncpg) |
| **Import** | `eventsource.adapters.memory` (also top-level `eventsource`) | `eventsource.adapters.sqlite`, guarded by `AIOSQLITE_AVAILABLE`/`SQLITE_AVAILABLE` — importable but raises `ImportError` on construction when aiosqlite is missing | `eventsource.adapters.postgresql`, guarded by `ASYNCPG_AVAILABLE` |
| **Construction** | `InMemoryEventStore(store_id="memory", event_registry=None)` | `SQLiteEventStore(database, event_registry=None, store_id=None, wal_mode=True, busy_timeout=5000)` | `PostgreSQLEventStore(engine, event_registry=None, store_id=None, create_schema=False, outbox_enabled=False)` |
| **Lifecycle** | None — construct and use | None — construct and use; the connection and schema are created lazily on first call | You own the `AsyncEngine`; dispose it on shutdown. Pass `create_schema=True` to have the adapter provision its own schema, or provision it yourself via the SQL under `migrations/schemas/` |
| **Schema** | N/A | Created lazily on first use (idempotent) | Provisioned externally via migrations, unless `create_schema=True` |
| **Concurrency** | Single process; appends serialized behind a `threading.Lock` | Single writer per database file/connection | Multi-process and multi-node, with row-level locking in the server |
| **Outbox support** | No | No | Yes — `outbox_enabled=True` writes outbox rows in the append transaction |

## Use InMemoryEventStore for tests

`InMemoryEventStore` keeps every event on the instance, structurally
conforming to `FullEventStore`. It is the right choice when the store should
not outlive the test, example, or prototype that created it.

```python
from uuid import uuid4

from eventsource import ExpectedVersion, InMemoryEventStore, StreamId

store = InMemoryEventStore()
stream = StreamId(aggregate_id=uuid4(), category="Order")

result = await store.append(
    stream,
    [OrderCreated(...)],
    expected=ExpectedVersion.no_stream(),
)
assert result.new_version == 1
```

### Install (no extra required)

Nothing to install beyond the base package:

```bash
pip install eventsource-py
```

`InMemoryEventStore` pulls in no database driver — only the standard library
and the core `eventsource` modules — so it is available on every install.
Import it from either the package root or the adapter module:

```python
from eventsource import InMemoryEventStore
# or
from eventsource.adapters.memory import InMemoryEventStore
```

Both names are exported unconditionally. Unlike `SQLiteEventStore`, which is
guarded behind `AIOSQLITE_AVAILABLE`, there is no `*_AVAILABLE` flag to check
here.

### Construct: `InMemoryEventStore(store_id="memory", *, event_registry=None)`

```python
def __init__(
    self,
    store_id: str = "memory",
    *,
    event_registry: EventRegistry | None = None,
) -> None: ...
```

The common case is a bare call:

```python
from eventsource import InMemoryEventStore

store = InMemoryEventStore()
```

The store starts empty and is immediately usable. `store_id` names the
store for `Position` comparisons (positions from different `store_id`s raise
`PositionForeignError` if you try to order them); pass `event_registry` if you
are not using the module-level default registry.

There is no `tracer` or `enable_tracing` constructor argument on this adapter
— check `src/eventsource/adapters/memory/store.py` if you need to confirm
current OpenTelemetry integration before relying on any tracing hook here.

### Lifecycle: no setup or teardown; state dies with the instance

There is nothing to connect, initialize, or close. `InMemoryEventStore` is not
an async context manager. Construct it, use it, drop the reference; the
events go with it. That makes the cleanest test fixture a fresh instance per
test:

```python
import pytest
from eventsource import InMemoryEventStore

@pytest.fixture
def store() -> InMemoryEventStore:
    return InMemoryEventStore()
```

Prefer a fresh instance per test over trying to reset a shared one.

### Caveats

**Single process only.** State lives on one Python object. Two instances
share nothing, and neither do two processes — a `pytest-xdist` worker, a
subprocess, or a separate web worker sees an empty store. Anything exercising
cross-process behavior (distributed locks, several subscription runners, real
contention) needs SQLite or PostgreSQL.

**Nothing is durable, including `read_all`.** The global feed and its
`Position` counter are in-process state that starts fresh for each new
instance. Projection catch-up, checkpoints, and replay therefore only work
within the lifetime of that instance: a test that restarts a component and
expects to resume from a stored position is not reproducing production
behavior.

**Concurrency is guarded by a `threading.Lock`, not a database.** Every
append acquires the same store-wide lock, so concurrent coroutines on one
event loop are safe and `ExpectedVersion` mismatches raise
`OptimisticLockError` as they should — a plain `threading.Lock` is used
(rather than `asyncio.Lock`) because `SyncEventStoreAdapter` runs each call in
its own event loop via `asyncio.run`, and the critical section never awaits.
This lock is not a substitute for a real database's contention behavior; do
not use in-memory timings to reason about throughput.

**No outbox.** `InMemoryEventStore` never writes outbox rows. Tests covering
transactional outbox behavior need `PostgreSQLEventStore` with
`outbox_enabled=True`.

**Versions are counted per stream.** A stream is `StreamId(aggregate_id,
category)`; the current version and the optimistic-lock check are computed
from the events under that exact stream identity, so the same `aggregate_id`
under two different categories gets two independent version sequences.

**Appends are rejected on duplicate `event_id`, not silently skipped.** An
event whose `event_id` is already in the store — from any stream — raises
`DuplicateEventError` rather than being silently absorbed.

**An empty `events` list raises, it does not short-circuit.** `store.append(stream,
[], expected=...)` raises `ValueError("cannot append an empty batch of
events")` rather than returning a successful no-op result.

## Use SQLiteEventStore for single-node durability

`SQLiteEventStore` needs the `sqlite` extra:

```bash
pip install eventsource-py[sqlite]
```

Import it from `eventsource.adapters.sqlite` (or the top-level package, where
it is re-exported when `aiosqlite` is installed):

```python
from eventsource.adapters.sqlite import AIOSQLITE_AVAILABLE, SQLITE_AVAILABLE, SQLiteEventStore
```

Construct it directly against a database path — there is no `initialize()`
call and no `async with store:` context manager:

```python
from eventsource.adapters.sqlite import SQLiteEventStore

store = SQLiteEventStore("events.db")
```

```python
def __init__(
    self,
    database: str,
    event_registry: EventRegistry | None = None,
    *,
    store_id: str | None = None,
    wal_mode: bool = True,
    busy_timeout: int = 5000,
) -> None: ...
```

The connection and schema are created lazily, on first call, guarded by an
internal `asyncio.Lock` so concurrent first-callers do not race each other.
There is nothing to await before use and nothing to close explicitly beyond
normal process shutdown. `wal_mode` (default `True`) enables SQLite's
write-ahead log for better read/write concurrency within the single-writer
constraint; `busy_timeout` controls how long a writer waits on lock
contention before raising.

`SQLiteEventStore` has no outbox support — `outbox_enabled` is a
`PostgreSQLEventStore`-only constructor keyword.

## Use PostgreSQLEventStore for production

`PostgreSQLEventStore` needs the `postgresql` extra:

```bash
pip install eventsource-py[postgresql]
```

It takes a plain SQLAlchemy `AsyncEngine` — not a `session_factory` or
`async_sessionmaker` — and builds its own session factory internally:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from eventsource.adapters.postgresql import PostgreSQLEventStore

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/app")
store = PostgreSQLEventStore(engine)
```

```python
def __init__(
    self,
    engine: AsyncEngine,
    event_registry: EventRegistry | None = None,
    *,
    store_id: str | None = None,
    create_schema: bool = False,
    outbox_enabled: bool = False,
) -> None: ...
```

By default the store assumes the schema (`events`, and — if
`outbox_enabled=True` — `event_outbox`) already exists, applied via the SQL
under `src/eventsource/adapters/sql/schemas/schemas/` or your own migration tool. Pass
`create_schema=True` to have the adapter provision it lazily on first use
instead.

### The transactional outbox: `outbox_enabled=True`

`PostgreSQLEventStore` is the only backend with outbox support. When
`outbox_enabled=True`, every `append` call writes a matching row into
`event_outbox` in the *same* database transaction as the event insert — the
outbox row and the event row commit or roll back together, which is the
entire point of the transactional outbox pattern:

```python
store = PostgreSQLEventStore(engine, outbox_enabled=True)
```

The outbox *reader* — the component that drains `event_outbox` and publishes
to a bus — lives separately in `eventsource.ports.outbox` (the
`OutboxRepository` contract) and its adapters
(`eventsource.adapters.postgresql.PostgreSQLOutboxRepository`,
`eventsource.adapters.sqlite.SQLiteOutboxRepository`,
`eventsource.adapters.memory.InMemoryOutboxRepository`); enabling the flag
here only controls whether `append` writes the row.

### Reading the global feed: `current_position()`

`current_position()` returns the store's current `Position | None` (an
opaque, ordered, store-scoped token defined in `eventsource.ports.positions`)
— not an integer. Compare and persist `Position` values; do not do arithmetic
on them:

```python
position = await store.current_position()
async for envelope in store.read_all(from_position=position):
    ...
```

PostgreSQL applies a safe-horizon predicate on `read_all`/`current_position`
so that a `global_position` allocated (but not yet committed) by one
transaction cannot cause a reader to skip a lower position that is still in
flight — this is specific to PostgreSQL's out-of-order commit visibility;
SQLite's single-writer model has no equivalent gap to guard against.

## Appending events: the common shape across all three backends

Every backend takes the same `append(stream, events, expected)` call:

```python
from uuid import uuid4

from eventsource import ExpectedVersion, StreamId

stream = StreamId(aggregate_id=uuid4(), category="Order")

result = await store.append(
    stream,
    [OrderCreated(...)],
    expected=ExpectedVersion.no_stream(),
)
```

`ExpectedVersion` is a frozen dataclass in `eventsource.ports.positions` with
classmethod constructors: `ExpectedVersion.any_()` (skip the check),
`ExpectedVersion.no_stream()` (stream must not exist yet), `
ExpectedVersion.stream_exists()` (stream must already have events), and
`ExpectedVersion.exact(n)` (stream must currently have exactly `n` events). A
mismatch raises `OptimisticLockError` on every backend.

## Reading events

- `store.read_stream(stream, options=None)` — an `AsyncIterator[EventEnvelope]`
  for one stream.
- `store.read_category(category, options=None)` — an
  `AsyncIterator[EventEnvelope]` across every stream in a category.
- `store.read_all(from_position=None, options=None)` — the store's global,
  ordered feed, also an `AsyncIterator[EventEnvelope]`.
- `await store.get_stream_version(stream)` — the stream's current version.
- `await store.event_exists(event_id)` — whether a specific event id has been
  stored.

`eventsource.ports.store.collect(iterator)` is a small helper for draining any
of the async iterators above into a `list[EventEnvelope]`.
