# How to choose an event store backend

`eventsource` ships three `EventStore` implementations, all exported from
`eventsource.stores`: `InMemoryEventStore`, `SQLiteEventStore`, and
`PostgreSQLEventStore`. They implement the same `EventStore` interface, so the
choice is an infrastructure decision — your aggregates, projections, and
repositories do not change when you switch.

Use this guide to pick a backend for a specific situation and wire it up:

- **Tests and examples** — `InMemoryEventStore()` takes no dependencies, needs
  no extra, and requires no setup or teardown.
- **Local development and single-node deployments** — `SQLiteEventStore` needs
  the `sqlite` extra (aiosqlite), creates its own schema via `initialize()`, and
  is only importable from `eventsource.stores` when aiosqlite is installed.
- **Production** — `PostgreSQLEventStore` needs the `postgresql` extra
  (asyncpg), takes a SQLAlchemy `async_sessionmaker` you own, and is the only
  backend with the transactional outbox (`outbox_enabled=True`).

Each backend section below covers install, construction, lifecycle, and the
caveats that tend to bite. If you only need to know which one to reach for,
read [Decide which backend fits](#decide-which-backend-fits) and stop there.

## Before you start

This guide assumes you already have a working `eventsource` install and some
`DomainEvent` subclasses to store. Specifically:

- **Python 3.11+** with `eventsource-py` installed. The base install pulls only
  `pydantic` and `sqlalchemy`; every backend beyond `InMemoryEventStore`
  requires an optional extra (`sqlite` or `postgresql`), installed separately.
- **An async runtime.** The `EventStore` interface is async throughout
  (`append_events`, `read_stream`, `read_all`), so all examples here run inside
  `asyncio` — under `asyncio.run(...)`, an async web framework, or
  `pytest-asyncio`. If you need to call a store from synchronous code, wrap it
  with `SyncEventStoreAdapter` instead of changing backends.
- **Your event classes importable at startup.** `DomainEvent` subclasses
  self-register into the global `EventRegistry` when their module is imported.
  Every backend deserializes stored rows through that registry, so a module
  that is never imported means events that never come back out.
- **For `PostgreSQLEventStore`: a reachable PostgreSQL server and its schema
  already provisioned.** Unlike the SQLite backend, it does not create tables
  for you — apply the SQL under `src/eventsource/migrations/schemas/` (or your
  own migration tool) before first use. For local work, the repo's
  `docker-compose.test.yml` brings up a suitable instance.

You do not need to decide permanently. Because all three backends satisfy the
same interface, swapping one for another is a change at your composition root,
not in your domain code — see
[Switch backends without changing application code](#switch-backends-without-changing-application-code).

## Decide which backend fits

Pick by deployment shape, then confirm the constraints below match what you
need:

- **Choose `InMemoryEventStore`** when the store's lifetime is the process's
  lifetime: unit tests, examples, and prototypes. No extra to install, nothing
  to provision, nothing to tear down.
- **Choose `SQLiteEventStore`** when you want durability on one machine with no
  server to run: local development, CLI tools, embedded or single-node
  deployments. It creates its own schema via `initialize()`.
- **Choose `PostgreSQLEventStore`** when more than one process writes, or when
  you need the transactional outbox. It is the only backend that writes outbox
  rows in the same transaction as the events.

Two constraints decide most cases:

- **Do multiple processes write to the same streams?** If yes, PostgreSQL. The
  in-memory store is per-instance, and SQLite serializes all writers behind a
  single file lock — safe, but not a scaling story.
- **Do you need reliable publishing via the outbox?** If yes, PostgreSQL with
  `outbox_enabled=True`. `SQLiteEventStore.initialize()` creates an
  `event_outbox` table, but the SQLite store never writes to it; only
  `PostgreSQLEventStore` populates the outbox on append.

Everything else — optimistic locking via `expected_version`, `read_stream`,
`read_all`, tracing via `enable_tracing`, and UUID field conversion — behaves
the same across all three.

### Comparison at a glance

Use this table to confirm a choice you have already narrowed down; the sections
below cover each backend in detail.

| | `InMemoryEventStore` | `SQLiteEventStore` | `PostgreSQLEventStore` |
|---|---|---|---|
| **Persistence** | None — state lives in the instance and dies with it | Durable file, or `:memory:` (per connection, non-durable) | Durable, managed by the PostgreSQL server |
| **Install extra** | None; part of the base install | `pip install eventsource-py[sqlite]` (aiosqlite) | `pip install eventsource-py[postgresql]` (asyncpg) |
| **Import** | Always exported from `eventsource.stores` | Exported only when `aiosqlite` is importable — the name is absent otherwise | Always exported from `eventsource.stores` |
| **Construction** | `InMemoryEventStore(enable_tracing=...)` | `SQLiteEventStore(database, event_registry, wal_mode=..., busy_timeout=...)` | `PostgreSQLEventStore(session_factory, outbox_enabled=...)` |
| **Lifecycle** | None — construct and use | `async with store: ...`, or call `initialize()` (which opens the connection) and `close()` yourself | You own the engine and `async_sessionmaker`; dispose the engine on shutdown |
| **Schema** | N/A | Created by `initialize()`: `events`, `event_outbox`, `projection_checkpoints`, `dead_letter_queue` (idempotent) | Provisioned externally via migrations; the store never creates tables |
| **Concurrency** | Single process; appends serialized behind an `asyncio.Lock` | Single writer per database file; tune with `wal_mode=True` and `busy_timeout` | Multi-process and multi-node, with row-level locking in the server |
| **Outbox support** | No | Table is created but never written to | Yes — `outbox_enabled=True` writes outbox rows in the append transaction |

Behavior that is identical across all three: the `EventStore` interface itself
(`append_events`, `read_stream`, `read_all`), optimistic locking via
`expected_version` raising `OptimisticLockError`, tracing via `enable_tracing`
or an injected `tracer`, and UUID field conversion via `uuid_fields`,
`string_id_fields`, and `auto_detect_uuid`.

## Use InMemoryEventStore for tests

`InMemoryEventStore` keeps every event in Python dictionaries on the instance.
It is the right choice when the store should not outlive the test, example, or
prototype that created it.

```python
from eventsource import InMemoryEventStore

store = InMemoryEventStore()

result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[OrderCreated(...)],
    expected_version=0,
)
assert result.success
assert result.new_version == 1
```

### Install (no extra required)

Nothing to install beyond the base package:

```bash
pip install eventsource-py
```

`InMemoryEventStore` pulls in no database driver — only the standard library
and the core `eventsource` modules — so it is available on every install.
Import it from either the package root or `eventsource.stores`:

```python
from eventsource import InMemoryEventStore
# or
from eventsource.stores import InMemoryEventStore
```

Both names are exported unconditionally. Unlike `SQLiteEventStore`, which is
re-exported only when `aiosqlite` is importable, there is no guarded import
here and no `*_AVAILABLE` flag to check first.

### Construct: `InMemoryEventStore(enable_tracing=...)`

The constructor takes no connection string, path, or event registry — only two
keyword-only tracing arguments:

```python
InMemoryEventStore(*, tracer: Tracer | None = None, enable_tracing: bool = True)
```

So the common case is a bare call:

```python
from eventsource import InMemoryEventStore

store = InMemoryEventStore()
```

The store starts empty and is immediately usable; there is no registry to pass
because it holds live `DomainEvent` objects rather than serialized rows.

**Turn spans off with `enable_tracing=False`.**

```python
store = InMemoryEventStore(enable_tracing=False)
```

`enable_tracing` defaults to `True`, but it only has an effect when
OpenTelemetry is installed: the constructor calls
`create_tracer(__name__, enable_tracing)`, which returns an
`OpenTelemetryTracer` when `enable_tracing` is true *and* OTel is importable,
and a `NullTracer` otherwise. With the base install (no `opentelemetry` extra),
the default is already a no-op and passing `enable_tracing=False` changes
nothing observable.

Pass `False` when OTel *is* installed in your test environment and you would
rather not emit the store's four spans —
`inmemory_event_store.append_events`, `.get_events`, `.read_stream`, and
`.read_all` — into whatever exporter the test process has configured.

**Inject a tracer with `tracer=...`.**

```python
from eventsource.observability import MockTracer

tracer = MockTracer()
store = InMemoryEventStore(tracer=tracer)

await store.append_events(...)
assert "inmemory_event_store.append_events" in tracer.span_names
```

An explicit `tracer` takes precedence: the store uses `tracer or
create_tracer(...)`, so when you pass one, `enable_tracing` is ignored entirely
and the injected tracer decides what happens. Any object satisfying the
`Tracer` protocol works; `MockTracer`, `NullTracer`, and `OpenTelemetryTracer`
are all exported from `eventsource.observability`.

One consequence worth knowing: the store sets its internal
`_enable_tracing` from `tracer.enabled`, not from the argument you passed. If
you inject a `NullTracer` while leaving `enable_tracing=True`, tracing is off —
the tracer wins.

### Lifecycle: no setup or teardown; state dies with the instance

There is nothing to connect, initialize, or close. `InMemoryEventStore` defines
no `connect()`, `initialize()`, or `close()` method, and it is not an async
context manager — `async with store:` raises `AttributeError`, unlike
`SQLiteEventStore`. Construct it, use it, drop the reference; the events go
with it. That makes the cleanest test fixture a fresh instance per test:

```python
import pytest
from eventsource import InMemoryEventStore

@pytest.fixture
def store() -> InMemoryEventStore:
    return InMemoryEventStore()
```

If you must share one instance across tests — say, a module- or session-scoped
fixture — reset it explicitly with `await store.clear()`, which empties the
per-aggregate event map, the aggregate-type index, the seen-`event_id` set, and
the global log, and resets the global position counter to `0`:

```python
@pytest.fixture
async def store(shared_store: InMemoryEventStore) -> InMemoryEventStore:
    await shared_store.clear()
    return shared_store
```

Prefer a fresh instance. `clear()` exists for when you cannot have one.

The store also exposes a handful of inspection helpers that have no counterpart
on the durable backends, useful for assertions:

- `await store.get_all_events()` — every event, sorted by `occurred_at`
- `await store.get_event_count()` — total events stored
- `await store.get_aggregate_ids()` — aggregates that have events
- `await store.get_global_position()` — the highest global position assigned

Lean on these for debugging, but write assertions you intend to keep against
the `EventStore` interface (`get_events`, `read_stream`, `read_all`) so the
same test can run against SQLite or PostgreSQL later.

### Caveats

**Single process only.** State lives in the dictionaries on one Python object.
Two instances share nothing, and neither do two processes — a `pytest-xdist`
worker, a subprocess, or a separate web worker sees an empty store. Anything
exercising cross-process behavior (distributed locks, several subscription
runners, real contention) needs SQLite or PostgreSQL.

**Nothing is durable, including `read_all`.** The global log behind `read_all`
is an in-process list and the global position is a plain counter that starts at
`0` for each new instance. Projection catch-up, checkpoints, and replay
therefore only work within the lifetime of that instance: a test that restarts
a component and expects to resume from a stored position is not reproducing
production behavior.

**Concurrency is guarded by an `asyncio.Lock`, not a database.** Every append
and every read acquires the same store-wide lock, so concurrent coroutines on
one event loop are safe and `expected_version` mismatches raise
`OptimisticLockError` as they should. But that lock protects a single event
loop in a single process — it is not thread-safe across event loops, and
serializing all traffic hides the contention you would hit against a real
database. Do not use in-memory timings to reason about throughput.

**No outbox.** `InMemoryEventStore` never writes outbox rows. Tests covering
transactional outbox behavior need `PostgreSQLEventStore` with
`outbox_enabled=True`.

**Versions are counted per aggregate type.** The current version and the
optimistic-lock check are computed from the events matching the given
`aggregate_type` under an `aggregate_id`, so reusing one ID across two
aggregate types gives each its own independent version sequence.

**Appends are idempotent by `event_id`.** An event whose `event_id` is already
in the store — from any aggregate — is silently skipped rather than duplicated.
That is useful for testing at-least-once delivery, but it means re-appending a
"new" event built from a copied ID neither raises nor advances the version.

**An empty `events` list short-circuits.** `append_events(..., events=[])`
returns a successful result at `expected_version` without taking the lock or
running any version check, so it never raises `OptimisticLockError`.
