# Snapshotting

Loading an aggregate replays every event in its stream. Once a stream grows to
thousands of events, that replay dominates command latency. A *snapshot* stores
the aggregate's serialized state at a known version, so `AggregateRepository.load()`
can restore that state and replay only the events recorded after it.

This guide shows you how to:

- Turn snapshots on for a repository with `snapshot_store`, `snapshot_threshold`,
  and `snapshot_mode`.
- Implement the two aggregate hooks snapshots require, `_serialize_state()` and
  `_restore_from_snapshot()`.
- Pick between the `sync`, `background`, and `manual` snapshot modes, and take
  snapshots by hand at business milestones with `create_snapshot()`.
- Invalidate snapshots written by an older state shape by bumping the aggregate's
  `schema_version`.
- Recognize and diagnose *silent fallback* — snapshotting is designed so that a
  broken snapshot degrades into a full event replay instead of an error, which
  means a snapshot problem shows up as a latency regression, not a failure.
- Wait for background snapshots in tests with `await_pending_snapshots()` and
  `pending_snapshot_count`.

Snapshots are strictly a read optimization. The event stream remains the system
of record: a snapshot that is missing, unreadable, or written against an
incompatible schema never changes the state an aggregate loads with, only how
long loading takes.

## When to reach for snapshots

Reach for snapshots when **aggregate load time is your problem**, and you can
point at long streams as the cause. Without a snapshot, `load()` fetches every
event for the aggregate from version 0 and applies each one; with a usable
snapshot it restores state at `snapshot.version` and fetches only
`from_version=snapshot.version` onward. The win is proportional to how much of
the stream you get to skip.

Add snapshots when:

- **A few aggregates have long-lived streams** -- a running account, an
  inventory item, a subscription -- that accumulate events indefinitely and are
  loaded on every command.
- **Load latency grows over time** for the same operation, and profiling shows
  the time in event fetch and `apply()`, not in the command handler or the bus.
- **Replay cost is concentrated**, not uniform. Snapshots pay off best when a
  small number of hot aggregates dominate; snapshotting every aggregate in a
  large, mostly-cold set mostly buys you storage and write-path work.

Skip snapshots when:

- **Streams are naturally short.** Aggregates that close out after a few dozen
  events (an order, a support ticket, a single booking) replay cheaply already.
  Adding a snapshot per aggregate is overhead on the write path with nothing to
  reclaim on the read path.
- **The bottleneck is elsewhere.** Snapshots do nothing for projection lag,
  event bus throughput, slow handlers, or contention on
  `OptimisticLockError`. Measure before you configure.
- **You want a *correctness* fix.** Snapshots are not a schema migration
  mechanism, not a way to prune the event store, and not a substitute for
  splitting an aggregate whose stream is long because its boundary is too wide.
  If an aggregate has ten thousand events because it models too much, resizing
  the aggregate is the real fix and a snapshot only hides the symptom.

Two properties shape how you should think about the feature:

- **Snapshots are a pure read optimization.** The event stream stays the system
  of record. A snapshot that is missing, corrupt, or written under an
  incompatible `schema_version` is discarded and the aggregate replays in full.
  Enabling snapshots therefore cannot make an aggregate load with wrong state.
- **The same design makes failures quiet.** Because every snapshot problem
  degrades into a correct-but-slower full replay, a misconfigured or
  permanently-invalid snapshot shows up as a latency regression and a log line,
  never as an exception at the call site. `SnapshotDeserializationError` and
  `SnapshotSchemaVersionError` are caught internally, not raised to you -- see
  [Who raises the snapshot
  exceptions](#who-raises-the-snapshot-exceptions-and-how-to-opt-into-strictness)
  for what they are for. Budget for the verification step in [Verify it is
  on](#verify-it-is-on) -- "no errors" is not evidence that snapshots are working.

If you are unsure, the cheapest experiment is `InMemorySnapshotStore` with a
threshold in a test that loads a representative aggregate, comparing load time
with and without the store. If the difference is not worth a table and a write-path
cost, stop here.

## Before you start

You need a working aggregate and repository before snapshots are worth adding.
Specifically:

- **An `AggregateRoot` subclass parameterized with a Pydantic state model** --
  `class OrderAggregate(AggregateRoot[OrderState])`. The default
  `_serialize_state()` calls `self._state.model_dump(mode="json")`, and the
  default `_restore_from_snapshot()` resolves `TState` from that generic
  parameter and calls `model_validate()` on it. An aggregate that keeps state
  somewhere other than `self._state`, or that does not supply a concrete
  `TState`, must override both hooks itself.
- **An `AggregateRepository` you construct yourself**, since snapshotting is
  configured through its `snapshot_store`, `snapshot_threshold`, and
  `snapshot_mode` constructor arguments.
- **A stream long enough to matter.** Snapshots trade storage and write-path work
  for shorter replays. On aggregates with a few dozen events the full replay is
  already cheap, and the snapshot is overhead.

You also need a snapshot store. Pick one from `eventsource.adapters`:

| Store | Import | Requires |
| --- | --- | --- |
| `InMemorySnapshotStore` | `from eventsource import InMemorySnapshotStore` | nothing -- tests and development only, state is lost on restart |
| `PostgreSQLSnapshotStore` | `from eventsource.adapters.postgresql import PostgreSQLSnapshotStore` | `pip install "eventsource-py[postgresql]"`, plus a SQLAlchemy `async_sessionmaker` |
| `SQLiteSnapshotStore` | `from eventsource.adapters.sqlite import SQLiteSnapshotStore` | `pip install "eventsource-py[sqlite]"` (aiosqlite), plus a database path |

The SQLite store is an optional import: if `aiosqlite` is missing,
`eventsource.adapters.sqlite.AIOSQLITE_AVAILABLE` is `False`, `SQLiteSnapshotStore` is
`None`, and constructing it raises `SQLiteNotAvailableError`. Guard on
`AIOSQLITE_AVAILABLE` if your code must run in both configurations.

`PostgreSQLSnapshotStore` needs the `snapshots` table to exist before first
use; `SQLiteSnapshotStore` applies its own schema (idempotently) when it opens
its connection. The schema ships with the library either way:

- PostgreSQL: `src/eventsource/adapters/sql/schemas/templates/snapshots.sql`
- SQLite: `src/eventsource/adapters/sql/schemas/templates/sqlite/snapshots.sql`

Both keep **one snapshot per `(aggregate_id, aggregate_type)`**, upserted on
save, so a newer snapshot replaces the older one rather than accumulating
history.

Finally, two things that are optional but make the rest of this guide much
easier to follow:

- **Logging at `WARNING` and `INFO` for the `eventsource` logger.** Every
  fallback path described under [graceful
  degradation](#understand-graceful-degradation-snapshot-failures-never-break-correctness)
  is reported through the standard `logging` module and nowhere else. With
  those records suppressed, a broken snapshot is invisible.
- **OpenTelemetry tracing** (`pip install "eventsource-py[telemetry]"`), which
  adds the `snapshot.used`, `snapshot.found`, and `snapshot.version` span
  attributes used in the diagnosis section. Without it, tracing calls are no-ops
  and you rely on logs alone.

Nothing here changes correctness. If you skip the table, skip the logging, or
skip tracing, aggregates still load correctly by replaying their full event
stream.

## Step 1: Make the aggregate snapshot-capable

Snapshots call exactly two hooks on your aggregate:

- `_serialize_state()` on the write path, to produce the JSON payload stored in
  the `snapshots` table.
- `_restore_from_snapshot(state_dict, version)` on the read path, called by
  `AggregateRepository.load()` before it replays the events recorded after the
  snapshot.

**Most aggregates need to implement neither.** `AggregateRoot` ships working
defaults:

```python
def _serialize_state(self) -> dict[str, Any]:
    if self._state is None:
        return {}
    return self._state.model_dump(mode="json")

def _restore_from_snapshot(self, state_dict: dict[str, Any], version: int) -> None:
    if not state_dict:
        self._version = version
        return
    state_type = self._get_state_type()
    self._state = state_type.model_validate(state_dict)
    self._version = version
```

If your aggregate is `class OrderAggregate(AggregateRoot[OrderState])` with a
Pydantic `OrderState` held in `self._state`, it is already snapshot-capable. The
defaults work when three things hold:

1. **All aggregate state lives in `self._state`.** Anything cached on other
   instance attributes is not serialized; after a restore it holds whatever
   `__init__` set, not the value it had when the snapshot was taken.
2. **`self._state` is a Pydantic model** whose `model_dump(mode="json")` output
   round-trips through `model_validate()`. `mode="json"` is what makes UUIDs,
   `datetime`s, `Decimal`s, enums, and nested models storable as JSON.
3. **The class is parameterized with a concrete state type.**
   `_get_state_type()` walks `type(self).__mro__` inspecting `__orig_bases__`
   for a base whose origin is a subclass of `AggregateRoot`, and returns its
   first type argument. That resolves through `DeclarativeAggregate` and through
   your own intermediate subclasses; if no parameterization is found it raises
   `RuntimeError`.

Note the empty-payload convention that ties the two together: state of `None`
serializes to `{}`, and restoring `{}` sets only the version and leaves state at
its initial value. Preserve that if you override -- an empty payload must not
raise.

Override both hooks -- always as a pair, so they stay inverses -- when state is
not a single Pydantic model, when it contains values that do not survive JSON,
or when you keep derived caches that must be rebuilt after a restore:

```python
class InventoryAggregate(AggregateRoot[InventoryState]):
    aggregate_type = "Inventory"
    schema_version = 2

    def __init__(self, aggregate_id: UUID) -> None:
        super().__init__(aggregate_id)
        self._reserved: set[str] = set()
        self._sku_index: dict[str, int] = {}   # derived, not snapshotted

    def _serialize_state(self) -> dict[str, Any]:
        if self._state is None:
            return {}
        return {
            "state": self._state.model_dump(mode="json"),
            "reserved_skus": sorted(self._reserved),
        }

    def _restore_from_snapshot(self, state_dict: dict[str, Any], version: int) -> None:
        if not state_dict:
            self._version = version
            return
        self._state = InventoryState.model_validate(state_dict["state"])
        self._reserved = set(state_dict.get("reserved_skus", ()))
        self._rebuild_sku_index()          # recompute derived data
        self._version = version
```

Three rules for an override:

- **Return only JSON-compatible values from `_serialize_state()`.** The payload
  is stored as JSON, so a `UUID` or `datetime` left as a Python object fails at
  the store, not at your hook.
- **Set `self._version = version` in `_restore_from_snapshot()`.** The repository
  has already fetched events with `from_version=snapshot.version`; leaving the
  version at 0 makes `load_from_history()` see a version gap on the next event.
- **Do not touch `self._uncommitted_events`.** Restore is hydration, not a
  command. The aggregate must come back from the store with nothing pending.

### Set `schema_version` on the aggregate class

`schema_version` is a class attribute on `AggregateRoot`, defaulting to `1`. It
is stamped onto every snapshot the aggregate writes, and checked against the
aggregate class on every load:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 1
```

On write, the snapshot manager reads `getattr(type(aggregate), "schema_version", 1)`
and stores it in the `schema_version` column of the snapshot row. On load, it
compares `snapshot.schema_version` with
`getattr(aggregate_factory, "schema_version", 1)` -- the class you passed to the
repository. If they differ -- in either direction, so a rollback invalidates
just as a bump does -- it logs at `INFO` ("Snapshot schema version mismatch ...
Falling back to full event replay") and returns `None`, so the aggregate replays
its full stream. Nothing is raised: `SnapshotSchemaVersionError` exists in
`eventsource.domain.exceptions` for your own code to raise, but the load path
never does. The stale row is overwritten in place the next time the aggregate is
snapshotted, since snapshots are upserted per `(aggregate_id, aggregate_type)`.

Set it explicitly even at `1`, so the field is visible where a future change to
`OrderState` will be made. Bumping it is how you invalidate snapshots written
under an incompatible state shape; see
[Handling `schema_version` bumps](#handling-schema_version-bumps).

`schema_version` is independent of `aggregate_type`, which keys the snapshot row
together with `aggregate_id`. Two aggregate classes sharing an `aggregate_type`
share snapshot rows -- if they declare different `schema_version` values, each
will invalidate the other's snapshots on every load and neither will ever get a
snapshot hit.

### Verify `_restore_from_snapshot` round-trips your state model

Test both hooks together, before you configure a store. Neither one needs a
store, a database, or `async` to exercise -- they are plain synchronous methods
on the aggregate -- and a unit test is the only cheap way to catch a broken
override. At runtime, an exception raised inside `_restore_from_snapshot()` is
caught by `AggregateRepository.load()`, logged at `WARNING` with a traceback
("Failed to restore from snapshot ... Falling back to full event replay"),
and turned into a fresh aggregate replaying from version 0. The load returns
correct state either way, so the only production symptom is the latency you
added snapshots to remove.

Start with a pure round-trip: serialize, force the payload through real JSON,
restore into a fresh instance, and compare.

```python
def test_snapshot_round_trip() -> None:
    original = OrderAggregate(uuid4())
    original.create(customer_id=uuid4())
    original.add_item("sku-1", quantity=2, price=Decimal("9.99"))

    state_dict = original._serialize_state()
    # Prove it is really JSON, not just dict-shaped.
    state_dict = json.loads(json.dumps(state_dict))

    restored = OrderAggregate(original.aggregate_id)
    restored._restore_from_snapshot(state_dict, original.version)

    assert restored.state == original.state
    assert restored.version == original.version
    assert restored.uncommitted_events == []
```

The `json.dumps` in the middle is the part people skip, and the part that
catches real bugs. A hand-built dict containing a raw `UUID`, `datetime`, or
`Decimal` will pass `model_validate()` and compare equal to the original, but
cannot be stored -- and that failure surfaces on the write path in production,
where the snapshot manager logs it rather than raising.

The `uncommitted_events` assertion matters for the same reason. Restore is
hydration, not a command: an aggregate that comes back from a snapshot with
pending events will re-append them on the next `save()`.

Then prove that restore *composes with replay*, which is what `load()` actually
does. Restore at version *N*, feed the events for versions *N+1...M* through
`load_from_history()`, and assert the result matches an aggregate built by
replaying all *M* events from scratch:

```python
def test_snapshot_plus_replay_equals_full_replay() -> None:
    events = build_order_history()            # versions 1..M

    full = OrderAggregate(aggregate_id)
    full.load_from_history(events)

    mid = 5
    snapshotted = OrderAggregate(aggregate_id)
    snapshotted.load_from_history(events[:mid])
    state_dict = json.loads(json.dumps(snapshotted._serialize_state()))

    from_snapshot = OrderAggregate(aggregate_id)
    from_snapshot._restore_from_snapshot(state_dict, version=mid)
    from_snapshot.load_from_history(events[mid:])

    assert from_snapshot.state == full.state
    assert from_snapshot.version == full.version
```

That equivalence is the entire contract snapshots have to honor. If it holds for
a representative history, enabling snapshots cannot change the state your
aggregates load with. Note that `load_from_history()` applies events with
`is_new=False`, which skips version validation -- so a `_restore_from_snapshot()`
that forgets to set `self._version` will *not* raise here. Asserting on
`from_snapshot.version` is what catches it.

Two cases worth covering explicitly if you overrode either hook:

- **Empty payload.** `_restore_from_snapshot({}, version=10)` must set the
  version and leave state untouched, not raise or `KeyError`. This is the
  convention the default implementation follows for aggregates whose state was
  `None` when snapshotted.
- **Invalid payload.** A dict missing a required field should raise -- the
  default implementation lets Pydantic's `ValidationError` propagate, and the
  repository's fallback depends on the exception escaping the hook. An override
  that swallows errors and returns half-restored state defeats the safety net,
  because `load()` will then trust it and replay only the tail of the stream.

## Step 2: Choose a snapshot store backend

Every store implements the same three-method `SnapshotStore` interface --
`save_snapshot()`, `get_snapshot()`, `delete_snapshot()` -- plus two methods
with default implementations, `snapshot_exists()` and
`delete_snapshots_by_type()`. The repository only ever calls the interface, so
the backend is a swap: pick the in-memory store for unit tests and the durable
one that matches where your events already live.

| Store | Import from | Constructor argument | Extra required |
| --- | --- | --- | --- |
| `InMemorySnapshotStore` | `eventsource` or `eventsource.adapters.memory` | none | none |
| `SQLiteSnapshotStore` | `eventsource.adapters.sqlite` | `database_path: str` | `eventsource-py[sqlite]` |
| `PostgreSQLSnapshotStore` | `eventsource.adapters.postgresql` | `session_factory: async_sessionmaker[AsyncSession]` | `eventsource-py[postgresql]` |

Only `Snapshot`, `SnapshotStore`, and `InMemorySnapshotStore` are re-exported
from the top-level `eventsource` package. The two durable stores are imported
path-only from their adapter packages.

All three take the same optional keyword arguments: `tracer` (a custom
`Tracer`) and `enable_tracing` (default `True`, ignored when `tracer` is
passed). Tracing emits `eventsource.snapshot.save`, `.get`, `.delete`,
`.exists`, and `.delete_by_type` spans carrying `aggregate_id`,
`aggregate_type`, and `version` attributes.

Every store keys snapshots by `(aggregate_id, aggregate_type)` and upserts on
save, so a store holds exactly one snapshot per aggregate -- the latest. There
is no snapshot history to prune.

### `InMemorySnapshotStore` for unit tests

A `dict` keyed by `(aggregate_id, aggregate_type)` guarded by an
`asyncio.Lock`. No optional dependency, no table to provision, nothing to clean
up between runs except the store object itself:

```python
from eventsource import InMemorySnapshotStore

store = InMemorySnapshotStore()
```

`InMemorySnapshotStore` is one of the three snapshot names re-exported from the
top-level package, so `from eventsource import InMemorySnapshotStore` works
equally well.

Two members that only this store has make it the right choice for tests:

- `await store.clear()` -- drop every snapshot. Use it in fixture teardown, or
  between phases of a test that needs to prove a cold load.
- `store.snapshot_count` -- a plain synchronous property returning the number of
  snapshots held. This is how you assert that a snapshot was actually written,
  which matters because nothing on the write path raises when it is not: a
  snapshot that never gets taken looks exactly like a passing test.

```python
@pytest.fixture
async def snapshot_store():
    store = InMemorySnapshotStore()
    yield store
    await store.clear()


async def test_threshold_triggers_snapshot(snapshot_store, repo):
    order = await repo.load(order_id)
    order.add_item("sku-1", quantity=1)
    await repo.save(order)

    assert snapshot_store.snapshot_count == 1
```

That assertion is only valid for `snapshot_mode="sync"`. Under `"background"`
the write is scheduled on a task and `snapshot_count` will usually still be `0`
when `save()` returns -- call `await repo.await_pending_snapshots()` first (see
[Draining background snapshots before shutdown](#draining-background-snapshots-before-shutdown-await_pending)).

To assert on the snapshot's contents rather than just its existence, read it
back through the interface:

```python
snapshot = await snapshot_store.get_snapshot(order_id, "Order")
assert snapshot is not None
assert snapshot.version == order.version
assert snapshot.schema_version == OrderAggregate.schema_version
```

The store also fully implements `delete_snapshots_by_type()`, including the
`schema_version_below` filter, so bulk-invalidation logic for a
[`schema_version` bump](#handling-schema_version-bumps) can be exercised
without a database. Its `snapshot_exists()` is a dict membership test, and
`repr(store)` reports the snapshot count -- useful in an assertion message.

Two limits keep it to tests and local development: state is lost when the
process ends, and the dict is per-process, so nothing is shared between
workers. `snapshot_count` reads the dict without taking the lock, which is fine
for assertions between awaits but is not a consistent view while concurrent
saves are in flight.

### `SQLiteSnapshotStore` for embedded and single-node deployments

Backed by `aiosqlite`, one file on disk:

```python
from eventsource.adapters.sqlite import SQLiteSnapshotStore

store = SQLiteSnapshotStore("snapshots.db")
try:
    ...
finally:
    await store.close()
```

Saves use `INSERT OR REPLACE`; the `state` dict is stored as a JSON `TEXT`
column via `json.dumps()`, and `created_at` as an ISO-8601 string. Both are
decoded on read, so the `Snapshot` you get back has a real `dict` and a real
`datetime`.

Four things to know before you rely on it:

- **The import is optional.** If `aiosqlite` is not installed,
  `eventsource.adapters.sqlite.AIOSQLITE_AVAILABLE` is `False` and `SQLiteSnapshotStore`
  is bound to `None` -- so a missing dependency surfaces as a `TypeError` on
  call, not a clean `ImportError`. Guard on `AIOSQLITE_AVAILABLE` in code that must
  run in both configurations. When the class is importable but `aiosqlite` is
  not, the constructor raises `SQLiteNotAvailableError`.
- **You must close it.** The store opens one `aiosqlite` connection on first
  use and reuses it for its lifetime; `aiosqlite` backs each connection with a
  **non-daemon** thread, which keeps the interpreter alive at shutdown until
  someone closes it. `SQLiteSnapshotStore` implements
  [`SupportsClose`](../architecture.md), so `await store.close()` releases it,
  idempotently, and a later call reopens. Nothing else in the library closes a
  snapshot store for you.
- **`":memory:"` works.** Because the connection is held open for the store's
  lifetime, an in-memory path behaves like any other -- `close()` discards the
  database with the connection. (Before the connection was shared, every
  operation opened its own, so an in-memory store saw a fresh empty database on
  every call; saves vanished and every load missed.) `InMemorySnapshotStore` is
  still the simpler choice for tests that don't need SQL.
- **SQLite is single-writer.** Concurrent snapshot writes serialize. That is
  fine at the volumes snapshotting produces (one write per aggregate per
  threshold crossing), but it is the reason this store is scoped to single-node
  deployments.

It exposes `database_path` as a read-only property and implements
`snapshot_exists()` with a `SELECT EXISTS` query rather than fetching the whole
row.

### `PostgreSQLSnapshotStore` for production

Takes a SQLAlchemy `async_sessionmaker`, the same one your PostgreSQL event
store uses:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from eventsource.adapters.postgresql import PostgreSQLSnapshotStore

engine = create_async_engine("postgresql+asyncpg://localhost/app")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

store = PostgreSQLSnapshotStore(session_factory)
```

Configure the sessionmaker with `expire_on_commit=False`. The store opens a
short transaction per operation (`async with self._session_factory() as
session, session.begin()`), so it does not join a transaction you already have
open -- a snapshot write is never part of your event append, which is
deliberate: snapshot failures must not roll back a successful `save()`.

Saves use `INSERT ... ON CONFLICT (aggregate_id, aggregate_type) DO UPDATE`,
which makes the upsert atomic under concurrent writers. `state` is a `JSONB`
column, so the driver typically hands the state back already decoded; the store
still tolerates a `str` and parses it.

Like the SQLite store, it overrides `snapshot_exists()` with `SELECT EXISTS`
and implements `delete_snapshots_by_type()` with the optional
`schema_version_below` filter -- the bulk-invalidation tool you will want when
you bump `schema_version` (see
[Handling `schema_version` bumps](#handling-schema_version-bumps)).

### Provision the snapshot table

Both durable stores expect a `snapshots` table to already exist. Nothing
creates it for you, and nothing checks for it at construction -- a missing
table surfaces as a database error on the first snapshot operation, which on
the write path is logged rather than raised.

The DDL ships with the library. Load it with `get_schema()` and execute it:

```python
from sqlalchemy import text

from eventsource.adapters.sql.schemas import get_schema

async with engine.begin() as conn:
    await conn.execute(text(get_schema("snapshots")))            # PostgreSQL
    # await conn.execute(text(get_schema("snapshots", backend="sqlite")))
```

The PostgreSQL table:

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_snapshots_aggregate UNIQUE (aggregate_id, aggregate_type)
);
```

The SQLite variant is the same shape with `TEXT` for `aggregate_id`, `state`,
and `created_at`, `INTEGER PRIMARY KEY AUTOINCREMENT` for `id`, and a plain
`UNIQUE (aggregate_id, aggregate_type)` constraint.

The `UNIQUE (aggregate_id, aggregate_type)` constraint is what the upsert
targets -- it is not optional. Both schemas also create indexes on
`(aggregate_id, aggregate_type)`, `(aggregate_type)`,
`(aggregate_type, schema_version)`, and `(created_at)`; the third is what makes
`delete_snapshots_by_type(..., schema_version_below=N)` cheap.

If you are provisioning the whole schema at once, `get_schema("all")` (or
`get_all_schemas()`) includes `snapshots` alongside `events`, `event_outbox`,
`projection_checkpoints`, and `dead_letter_queue`. Both accept
`backend="sqlite"`.

Because snapshots are a pure read optimization, this table is safe to truncate
or drop at any time. Doing so costs one full replay per aggregate and nothing
else.

## Enable snapshots on a repository (snapshot_store, snapshot_threshold, snapshot_mode)

Snapshotting is configured entirely at repository construction. Pass a
`snapshot_store`, and optionally a `snapshot_threshold` and `snapshot_mode`:

```python
from eventsource import AggregateRepository
from eventsource.adapters.postgresql import PostgreSQLSnapshotStore

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    snapshot_store=PostgreSQLSnapshotStore(session_factory),
    snapshot_threshold=100,
    snapshot_mode="sync",
)
```

There is no `enable_snapshots` flag. **`snapshot_store` is the switch**: when it
is `None` (the default), the repository builds no snapshot strategy and no
snapshot manager, `load()` always replays from version 0, and
`repo.has_snapshot_support` is `False`.

### What each argument does

`snapshot_store` -- the `SnapshotStore` implementation to read and write
snapshots. Providing it turns on snapshot-aware loading immediately, before you
configure anything else: `load()` will look for a snapshot on every call and use
one if it finds a valid one. This matters when adopting snapshots incrementally
-- reads start consulting the store as soon as you pass it, even with
`snapshot_mode="manual"` and no threshold.

`snapshot_threshold` -- how many events between automatic snapshots. Defaults to
`None`, which means *no automatic snapshots at all*, whatever the mode. A
repository with a store but no threshold reads snapshots but only writes them
when you call `create_snapshot()` yourself.

`snapshot_mode` -- `"sync"` (default), `"background"`, or `"manual"`; how an
automatic snapshot is executed once the threshold triggers. `AggregateRepository.__init__`
maps the mode/threshold combination directly to a `SnapshotPolicy` and
`SnapshotScheduler` (see [ADR 0021](../adrs/0021-snapshot-policy-scheduler-composition.md)):
`snapshot_mode="background"` selects `BackgroundScheduler`, everything else
selects `ImmediateScheduler`; a non-`None` threshold outside `"manual"` mode
selects `EveryNEvents(threshold)`, everything else selects `Never()`. The
parameter is typed `Literal["sync", "background", "manual"]`, so mypy rejects
an unrecognized string, but nothing raises at runtime for one that slips
through untyped code -- it is simply treated the same as `"sync"`. Prefer
passing `snapshot_policy=`/`snapshot_scheduler=` directly (mutually exclusive
with `snapshot_mode`/`snapshot_threshold`) if you want behavior beyond the
three named modes. See
[Choose a snapshot mode](#choose-a-snapshot-mode-sync-background-or-manual)
for the trade-offs.

The three arguments combine like this:

| `snapshot_store` | `snapshot_threshold` | Effect |
| --- | --- | --- |
| `None` | anything | Snapshots off. `load()` always replays from 0. |
| set | `None` | Reads use snapshots; writes only via `create_snapshot()`. |
| set | `100` | Reads use snapshots; writes happen automatically per `snapshot_mode`. |

With `snapshot_mode="manual"`, the threshold is ignored entirely -- the manual
strategy is constructed with no threshold and never auto-snapshots.

### How the threshold is actually evaluated

The threshold is **not** "N events since the last snapshot." After a successful
`save()`, the repository asks the strategy whether to snapshot, and the default
check is:

```python
aggregate.version // threshold > (aggregate.version - events_in_save) // threshold
```

That is, a snapshot is taken when the save carried the aggregate out of one
block of `threshold` versions and into a later one. Two consequences worth
planning around:

- **Crossing counts; landing does not.** With `snapshot_threshold=100`, an
  aggregate that goes from version 98 to 103 in one save snapshots at 103. A
  save that crosses several multiples at once still takes one snapshot, at the
  version it reached — the intermediate one would be overwritten immediately,
  since each aggregate keeps a single upserted snapshot row.
- **The version a snapshot lands on depends on how events were batched.**
  Snapshots no longer accumulate at exactly 100, 200, 300. Nothing reads a
  snapshot by version, so this costs nothing; do not build anything that
  assumes the old set.

Until ADR 0049 the check required the version to *land* on a multiple
(`version % threshold == 0`). That was not merely stricter: an aggregate whose
saves advance by a constant stride can miss every multiple forever — six events
per save from version 1 leaves the version permanently odd, so a threshold of
50 was never satisfiable — and such an aggregate snapshotted never rather than
late.

If you need a different rule -- elapsed time, event type, state size --
implement the `SnapshotPolicy` protocol (one method,
`should_snapshot(aggregate, events_since_snapshot) -> bool`) and pass it as
`snapshot_policy=` rather than tuning the threshold; the mode/threshold
knobs are a convenience over the built-in `EveryNEvents`/`Never` policies, but
`SnapshotPolicy` is the extension point.

### Verify it is on

The repository exposes its snapshot configuration read-only, which is the
quickest way to confirm wiring in a test or a REPL:

```python
assert repo.has_snapshot_support is True
assert repo.snapshot_store is snapshot_store
assert repo.snapshot_threshold == 100
assert repo.snapshot_mode == "sync"
```

These properties tell you the repository is *configured*. They do not tell you
snapshots are being read on load -- because every snapshot failure degrades
silently into a full replay, that requires checking logs or span attributes. See
[Detect silent fallback](#detect-silent-fallback-which-log-records-and-trace-attributes-to-watch).

### Enabling snapshots on an existing aggregate

Adding a store to a live repository is safe and needs no backfill. Aggregates
that have no snapshot yet simply replay in full, exactly as before; the first
save that hits a threshold boundary writes the first snapshot, and subsequent
loads use it. There is no window in which an aggregate loads with incomplete
state: `load()` fetches events `from_version=snapshot.version` only after the
snapshot has been validated and applied.

## Choose a snapshot mode: sync, background, or manual

`snapshot_mode` decides *how* an automatic snapshot runs once
`should_snapshot()` returns `True`. It does not decide whether snapshots are
read -- reads are on as soon as `snapshot_store` is set, in every mode.

`AggregateRepository.__init__` maps the mode/threshold pair to a
`SnapshotPolicy` and `SnapshotScheduler` (`eventsource.application.aggregates.snapshotting`):

| Mode | Policy | Scheduler | `save()` waits for the write? |
| --- | --- | --- | --- |
| `"sync"` (default) | `EveryNEvents(threshold)` | `ImmediateScheduler` | yes |
| `"background"` | `EveryNEvents(threshold)` | `BackgroundScheduler` | no -- `BackgroundTaskManager` |
| `"manual"` | `Never()` -- threshold discarded | `ImmediateScheduler` | n/a -- never auto-snapshots |

Mypy rejects any string outside `Literal["sync", "background", "manual"]` at
the type-check level; there is no runtime `ValueError` for an unrecognized
mode string reaching the constructor (see the note above under
[the three arguments](#what-each-argument-does)).

### `"sync"` -- write the snapshot before `save()` returns

`ImmediateScheduler.schedule()` awaits the write (`take_snapshot()`)
inline, so the snapshot is durable by the time `save()` returns.

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
    snapshot_mode="sync",   # the default; can be omitted
)

await repo.save(order)      # at version 100, returns only after the snapshot lands
```

Choose `sync` when:

- You want deterministic tests. A save that crosses a threshold boundary leaves
  a snapshot in the store with no waiting and no polling.
- Throughput is modest and one extra store round-trip per *N* saves is
  acceptable.
- You want the snapshot to reflect exactly the version that was just committed,
  with no window in which a later mutation can change what gets serialized.

The cost is latency, and it lands unevenly: 99 saves are unaffected and the
100th pays for a full state serialization plus a store write. If your p99 matters
more than your mean, that spike is the reason to look at `background`.

A failed sync snapshot is caught inside `ImmediateScheduler.schedule()`, logged at
`WARNING` (`Failed to create snapshot for ...`, with `exc_info`), and turned
into `None`. `save()` still succeeds. See
[Sync-mode snapshot writes that fail do not fail save()](#sync-mode-snapshot-writes-that-fail-do-not-fail-save).

### `"background"` -- hand the snapshot to a fire-and-forget task

`BackgroundScheduler.schedule()` submits the write to a
`BackgroundTaskManager` (`eventsource.application.background_tasks`), which
tracks it in a pending set, attaches a done-callback that discards it and
logs any exception, and returns `None` immediately. `save()` does not wait.

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
    snapshot_mode="background",
)

await repo.save(order)             # returns without the snapshot write
assert repo.pending_snapshot_count >= 0   # task may already have completed
```

Choose `background` when save latency is the constraint -- high-throughput
command handlers, or a snapshot store slower than your event store.

Three properties of this mode to plan around:

- **The task serializes the aggregate later, not now.** `save()` builds the
  `take_snapshot(...)` coroutine before handing it to the scheduler, but the
  coroutine's body -- reading `aggregate.version` and calling
  `aggregate._serialize_state()` -- only runs when the background task is
  actually scheduled by the event loop. If you keep mutating the same
  aggregate instance after `save()` returns, the snapshot can capture a state
  *newer* than the version it was scheduled at -- and it records whatever
  `aggregate.version` reads at that moment. Reload from the repository rather
  than continuing to command an instance you have already saved.
- **Failures are invisible to the caller.** The scheduler's guarded wrapper
  catches everything and logs at `WARNING`; nothing propagates into `save()`,
  and the returned value was already `None`.
- **Tasks are only tracked, not bounded.** There is no concurrency limit and no
  backpressure. Completed tasks are pruned whenever a new one is scheduled or
  `pending_count` is read. If the process exits with tasks outstanding, those
  snapshots are simply never written -- which is safe, because the event stream
  is the system of record.

In tests, never assert on the store right after `save()`. Await the tasks first;
see [Wait for background snapshots in
tests](#wait-for-background-snapshots-in-tests-with-await_pending_snapshots-and-pending_snapshot_count).

### `"manual"` -- read snapshots, write only when you say so

`Never().should_snapshot()` returns `False` unconditionally, and
`AggregateRepository.__init__` selects it whenever `snapshot_mode == "manual"`
-- a `snapshot_threshold` passed alongside `snapshot_mode="manual"` is
silently discarded.

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    snapshot_store=snapshot_store,
    snapshot_mode="manual",
)

await repo.save(order)                 # never snapshots, at any version
snapshot = await repo.create_snapshot(order)   # you decide when
```

Choose `manual` when event count is a poor proxy for when a snapshot is worth
taking: snapshot at "order fulfilled" or "period closed" instead of "version
% 100 == 0", pre-warm hot aggregates on a schedule, or snapshot before a
maintenance window. See [Create snapshots manually at business
milestones](#create-snapshots-manually-at-business-milestones-with-create_snapshot).

Note that `manual` still reads snapshots on every `load()`. It is also the mode
that gives you the cleanest adoption path: turn on the store in `manual`, write
a few snapshots by hand, confirm they are being used, then switch to `sync` or
`background` with a threshold.

### Behavior that does not vary by mode

- **`create_snapshot()` always writes synchronously**, in every mode, ignoring
  both `snapshot_mode` and `snapshot_threshold`. It is also the one snapshot
  path that raises: `RuntimeError` when no store is configured, and store errors
  propagate to the caller instead of being swallowed.
- **Loading is identical.** All three modes go through the same
  `read_valid_snapshot()` function, with the same schema-version
  validation and the same fallback to full replay.
- **No mode can corrupt state.** `sync` and `background` failures are both
  caught and logged; the aggregate still loads correctly by replaying events.
- **Snapshot writes upsert.** One row per `(aggregate_id, aggregate_type)`,
  regardless of mode, so a slow background task that lands after a newer manual
  snapshot overwrites it. In practice this only costs you a shorter replay
  prefix, never correctness.

### Switching modes later

The mode is fixed at construction -- there is no setter, and `repo.snapshot_mode`
is read-only. To change it, build a new repository. No data migration is
involved: snapshots written under one mode are read identically under another,
since the mode affects only the write path.

A workable progression for a service adopting snapshots:

1. `snapshot_mode="manual"`, no threshold -- reads are live, writes are explicit,
   nothing about save latency changes.
2. `snapshot_mode="sync"` with a threshold in a staging environment -- verify
   snapshots appear at the versions you expect and that `load()` reports
   `snapshot.used`.
3. `snapshot_mode="background"` in production once you have confirmed the
   snapshots are actually being used and the write path is the thing you want
   off the critical path.

## Create snapshots manually at business milestones with create_snapshot()

`AggregateRepository.create_snapshot(aggregate)` writes a snapshot on demand. It
ignores both `snapshot_mode` and `snapshot_threshold`, always writes
synchronously, and returns the `Snapshot` it wrote:

```python
order = await repo.load(order_id)
order.complete_fulfillment()
await repo.save(order)

snapshot = await repo.create_snapshot(order)
print(f"snapshotted {snapshot.aggregate_type} at v{snapshot.version}")
```

Reach for it when *event count* is the wrong trigger:

- **Business milestones.** An order that is fulfilled, an invoice that is
  settled, an accounting period that is closed -- states an aggregate reaches
  once and is then read from repeatedly.
- **Pre-warming.** Snapshot a hot aggregate on a schedule so the next cold read
  does not pay for a full replay.
- **Before maintenance.** Force a current snapshot ahead of a deploy or a
  migration window.
- **Tests.** The most direct way to get a known-good snapshot into a store
  without arranging a threshold boundary.

It works in every mode, including `manual`, where it is the only way a snapshot
ever gets written.

### Save first, then snapshot

`create_snapshot()` reads `aggregate.version` and `aggregate._serialize_state()`
as they are *right now*. `version` advances when an event is applied, not when
it is persisted, so an aggregate with uncommitted events already reports the
higher version.

Snapshotting such an aggregate writes a snapshot at a version the event store
has never seen. On the next `load()`, the repository restores that state and
then asks the store for events after `snapshot.version` -- so if those events
are never committed, the snapshot silently pins state that no longer has an
event stream behind it, and nothing detects the discrepancy.

```python
order.complete_fulfillment()
await repo.create_snapshot(order)   # WRONG: version includes uncommitted events
await repo.save(order)

await repo.save(order)              # RIGHT: commit first
await repo.create_snapshot(order)
```

The rule is one line: **call `create_snapshot()` only on an aggregate with no
uncommitted events** -- one freshly returned by `load()`, or one you have just
passed to `save()`. `aggregate.has_uncommitted_events` is the check if you want
to assert it.

### What gets written

Each call builds a `Snapshot` from the live aggregate and calls
`snapshot_store.save_snapshot()`:

| Field | Source |
| --- | --- |
| `aggregate_id` | `aggregate.aggregate_id` |
| `aggregate_type` | the repository's configured type, not the aggregate's |
| `version` | `aggregate.version` at call time |
| `state` | `aggregate._serialize_state()` |
| `schema_version` | `type(aggregate).schema_version`, defaulting to `1` |
| `created_at` | `datetime.now(UTC)` |

The write is an upsert on `(aggregate_id, aggregate_type)`, so calling it twice
leaves one row at the newer version -- there is no snapshot history to prune,
and no need to delete the old one first. A successful write logs at `INFO`:

```
Created snapshot for Order/550e8400-... at version 100 (schema_version=1)
```

### It is the one snapshot path that raises

Every automatic snapshot swallows its errors so that `save()` cannot fail
because of an optimization. `create_snapshot()` deliberately does not:

- **No `snapshot_store` configured** raises `RuntimeError` (`Cannot create
  snapshot: snapshot_store is not configured.`). Automatic snapshotting in the
  same repository would simply be inert.
- **Store write errors propagate.** A dead connection or a missing `snapshots`
  table surfaces to your caller as the store's own exception, rather than a
  `WARNING` and a `None`.
- **`_serialize_state()` errors propagate**, which makes this the fastest way to
  smoke-test a hand-written serialization hook against a real store.

That is the intended asymmetry: you asked for this snapshot explicitly, so you
find out whether you got it. See [Exception: create_snapshot() raises on a
missing store and propagates write
errors](#exception-create_snapshot-raises-on-a-missing-store-and-propagates-write-errors).

If a manual snapshot is best-effort in your code path -- a pre-warming job, say
-- catch around it rather than relying on the library to absorb the failure:

```python
try:
    await repo.create_snapshot(order)
except Exception:
    logger.warning("pre-warm snapshot failed for %s", order.aggregate_id, exc_info=True)
```

Skipping it costs a longer replay on the next load, nothing more.

### Milestone snapshots in a command handler

The natural place for a manual snapshot is immediately after the command that
reaches the milestone, in a repository configured with `snapshot_mode="manual"`:

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    snapshot_store=snapshot_store,
    snapshot_mode="manual",
)

async def fulfill_order(order_id: UUID) -> None:
    order = await repo.load(order_id)
    order.complete_fulfillment()
    await repo.save(order)

    if order.state.status is OrderStatus.FULFILLED:
        await repo.create_snapshot(order)
```

Manual and automatic snapshots also compose: nothing stops you running
`snapshot_mode="sync"` with a threshold *and* calling `create_snapshot()` at
milestones. Because both upsert the same row, the last write wins and the only
consequence of the overlap is a slightly different replay prefix.

## Invalidate stale snapshots by bumping schema_version

A snapshot stores whatever `_serialize_state()` produced *at the time it was
written*. Change the shape of your state model and the stored payload no longer
matches what `_restore_from_snapshot()` expects. `schema_version` is the
class-level integer that tells the library those payloads are obsolete:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 2   # was 1, before OrderState gained a required field
```

`AggregateRoot.schema_version` defaults to `1`, so an aggregate that never sets
it is already at version 1 and can be bumped to 2 the first time its state shape
changes incompatibly.

### How the check works

The value is written into every snapshot and compared on every load:

- **On write** -- `take_snapshot()`, the single function both the manual and
  automatic write paths call, reads
  `getattr(type(aggregate), "schema_version", 1)` and stores it on the `Snapshot`.
- **On read** -- `read_valid_snapshot()` reads
  `getattr(aggregate_factory, "schema_version", 1)` and compares:

  ```python
  if snapshot.schema_version != aggregate_schema_version:
      # log at INFO, return None
  ```

The comparison is **inequality, not "older than"**. A snapshot at version 3 is
rejected by code expecting version 2 just as a snapshot at version 1 is, which
means a rollback to a previous deploy is as safe as a roll-forward: the old code
ignores the newer snapshots rather than misreading them.

A mismatch is not an error. `load_valid_snapshot()` returns `None`, `load()`
replays the aggregate's full event stream, and the aggregate comes back correct
-- just more slowly. Nothing raises, and nothing is written to the DLQ.
`SnapshotSchemaVersionError` exists in `eventsource.domain.exceptions` for
callers that want to signal a mismatch themselves; the load path does not raise
it.

### When to bump

Bump `schema_version` whenever an old serialized payload would restore into a
*wrong or invalid* state under the new code:

- A new **required** field on the state model -- `model_validate()` on an old
  payload raises `ValidationError`.
- A field **renamed**, **removed**, or **retyped** (`str` to `UUID`, `float` to
  `Decimal`).
- The **meaning** of a field changed -- amounts moved from dollars to cents, a
  status enum's values redefined. This is the dangerous case: the old payload
  still validates, so without a bump the aggregate loads silently wrong.
- A hand-written `_serialize_state()` changed its output structure at all.

You do **not** need to bump for:

- A new **optional** field with a default. Old payloads validate; the field
  takes its default, which is what a replay would have produced anyway.
- Changes to command methods, event handlers, or invariants that do not alter
  the serialized shape.
- Adding an event type, unless applying it changes the state model's shape.

When in doubt, bump. The cost is one slow replay per aggregate; the cost of not
bumping is silently incorrect state.

### What happens after the bump

Deploy the bump and no migration is required:

1. Every load of an affected aggregate finds a snapshot whose
   `schema_version` no longer matches, logs at `INFO`, and replays in full.
2. The first save that hits a threshold boundary -- or your first
   `create_snapshot()` call -- writes a new snapshot at the new
   `schema_version`, upserting over the stale row.
3. Loads go back to being fast.

The window between (1) and (2) is a latency regression, not an outage, and its
length depends on your mode. With `snapshot_mode="sync"` and
`snapshot_threshold=100`, an aggregate at version 1,340 will not be re-snapshotted
until version 1,400; a rarely-written aggregate may never be. If a prolonged
full-replay window is unacceptable, pre-warm the hot aggregates explicitly after
deploying:

```python
for order_id in hot_order_ids:
    order = await repo.load(order_id)      # full replay, once
    await repo.create_snapshot(order)      # new snapshot at schema_version=2
```

That is exactly the pre-warming use case described in
[Create snapshots manually at business
milestones](#create-snapshots-manually-at-business-milestones-with-create_snapshot).

### Watch for the INFO line

The bump is only observable through logging. The mismatch is reported by the
`eventsource` logger at `INFO`:

```
Snapshot schema version mismatch for Order/550e8400-...: snapshot has v1, aggregate expects v2. Falling back to full event replay.
```

Seeing this line for a while after a deploy is expected and healthy. Seeing it
*indefinitely* means new snapshots are not being written -- typically a
`snapshot_threshold` of `None`, `snapshot_mode="manual"` with no manual calls, or
aggregates whose versions never land on a threshold boundary. See
[schema_version mismatches fall back to full event
replay](#schema_version-mismatches-fall-back-to-full-event-replay).

### Optionally reclaim the stale rows

Because each aggregate keeps a single upserted row per
`(aggregate_id, aggregate_type)`, obsolete snapshots are replaced rather than
accumulated, and leaving them alone is harmless -- they are ignored on read. To
reclaim the space for aggregates that may never be re-snapshotted, the store
interface offers a bulk delete:

```python
deleted = await snapshot_store.delete_snapshots_by_type(
    aggregate_type="Order",
    schema_version_below=2,
)
```

Omitting `schema_version_below` deletes every snapshot for the type. The filter
is a strict `<`, so `schema_version_below=2` removes v1 rows and leaves the new
v2 ones. `InMemorySnapshotStore`, `PostgreSQLSnapshotStore`, and
`SQLiteSnapshotStore` all implement it; the base `SnapshotStore` raises
`NotImplementedError`, so a custom store must provide its own. Deleting
snapshots never affects correctness -- the event stream is untouched.

### The alternative: upcast instead of invalidating

Bumping throws away the old payloads. If replays are expensive enough that you
would rather keep reading them, handle the old shape inside
`_restore_from_snapshot()` and leave `schema_version` where it is:

```python
def _restore_from_snapshot(self, state_dict: dict[str, Any], version: int) -> None:
    if not state_dict:
        self._version = version
        return
    if "total_cents" not in state_dict:                     # v1 payload
        state_dict = {**state_dict, "total_cents": int(state_dict.pop("total") * 100)}
    self._state = OrderState.model_validate(state_dict)
    self._version = version
```

The trade-off is ownership of the compatibility code: an upcast that is wrong
produces silently incorrect state, whereas a bump can only ever produce a slower
load. Bump by default; upcast only where the replay cost genuinely justifies
maintaining and testing the conversion. Note also that upcasting keeps working
only while the snapshot rows survive -- once a new snapshot is written for an
aggregate, its old payload is gone.

## Understand graceful degradation: snapshot failures never break correctness

Every automatic snapshot path in the library is wrapped in a `try/except` that
catches `Exception`, logs, and continues. The design rule is that a snapshot is
an optimization, so a broken snapshot must cost time, never correctness -- an
aggregate whose snapshot cannot be read, validated, or deserialized simply
replays its full event stream and comes back exactly as it would have without
snapshotting at all.

The corollary is the thing to internalize: **snapshot failures are invisible
unless you look for them.** No exception reaches your code, no health check
turns red. A store outage, a botched `_serialize_state()` override, or a
forgotten `schema_version` bump all present as "loads got slower," which is why
the next section is about detection.

The five automatic failure modes, and the one deliberate exception:

| Failure | Where it is caught | Log level | Result |
| --- | --- | --- | --- |
| Snapshot store read error | `read_valid_snapshot()` | `WARNING` | full replay |
| `schema_version` mismatch | `read_valid_snapshot()` | `INFO` | full replay |
| `_restore_from_snapshot()` raises | `AggregateRepository.load()` | `WARNING` | full replay |
| Sync snapshot write fails | `ImmediateScheduler.schedule()` | `WARNING` | `save()` succeeds, no snapshot |
| Background snapshot task fails | `BackgroundScheduler`'s guarded task wrapper | `WARNING` | `save()` succeeds, no snapshot |
| **`create_snapshot()` fails** | **not caught** | -- | **raises to the caller** |

### Snapshot store read errors fall back to full event replay

`read_valid_snapshot()` wraps the `snapshot_store.get_snapshot()` call:

```python
try:
    snapshot = await self._snapshot_store.get_snapshot(aggregate_id, self._aggregate_type)
except Exception as e:
    logger.warning(
        "Error loading snapshot for %s/%s: %s. Falling back to event replay.", ...
    )
    return None
```

The `except` clause is `Exception`, so a dead connection pool, a missing
`snapshots` table, a query timeout, and a permissions error are all absorbed
identically. `load()` sees `None`, leaves `from_version` at `0`, and fetches the
whole stream.

Two details worth noting:

- **This is the one fallback logged without `exc_info`.** You get the exception's
  `str()` in the message, not a traceback -- enough to see `connection refused`
  or `relation "snapshots" does not exist`, not enough to see where in the
  driver it came from.
- **A missing snapshot is not a failure.** `get_snapshot()` returning `None` --
  an aggregate that has simply never been snapshotted -- returns `None` with no
  log line at all. Silence therefore means either "no snapshot yet" or
  "snapshots are working"; only the span attributes distinguish them.

A snapshot store outage degrades the whole service to pre-snapshot load latency,
uniformly and without errors. If your replay times are long enough that this is
effectively an outage for you, the `WARNING` line is the only signal you will
get, so alert on it.

### schema_version mismatches fall back to full event replay

Immediately after the read, `read_valid_snapshot()` compares the stored
`schema_version` against `getattr(aggregate_factory, "schema_version", 1)` and
returns `None` on any inequality, logging at `INFO` rather than `WARNING`.

The lower level is deliberate: unlike the other fallbacks, a mismatch is an
*expected* consequence of a deploy that bumped `schema_version`, and it resolves
itself as new snapshots are written. It is only a problem if it persists. See
[Invalidate stale snapshots by bumping
schema_version](#invalidate-stale-snapshots-by-bumping-schema_version) for the
mechanics, and for what "persists" means in each mode.

Because the check runs before the span attributes are set, a mismatched snapshot
records neither `snapshot.found` nor `snapshot.used` -- in traces it is
indistinguishable from "no snapshot," and only the `INFO` line names it.

### State deserialization (_restore_from_snapshot) failures fall back to full event replay

This fallback lives in `AggregateRepository.load()` itself, not in
`read_valid_snapshot()`, because by the time it fires the repository has
already fetched only the events *after* the snapshot:

```python
try:
    aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
except Exception as e:
    logger.warning(
        "Failed to restore from snapshot for %s/%s: %s. Falling back to full event replay.",
        ..., exc_info=True,
    )
    event_stream = await self._event_store.get_events(
        aggregate_id, aggregate_type=self._aggregate_type, from_version=0,
    )
    if not event_stream.events:
        raise AggregateNotFoundError(aggregate_id, self._aggregate_type) from None
    aggregate = self._aggregate_factory(aggregate_id)
```

The recovery is a full re-do: re-fetch from version 0, discard the partially
restored instance, and construct a fresh aggregate to replay into. That double
fetch makes this the most expensive fallback -- two event-store round trips plus
the full replay -- but the aggregate that comes back is correct.

What triggers it in practice:

- A Pydantic `ValidationError` from `model_validate()` on a payload whose shape
  changed without a `schema_version` bump. This is the common case, and it is
  why forgetting to bump is a performance bug rather than a correctness bug.
- A `RuntimeError` from `_get_state_type()` when the aggregate is not
  parameterized with a concrete state type.
- Any exception from a hand-written `_restore_from_snapshot()` override -- a
  `KeyError` on a renamed key, a failed upcast, an error while rebuilding
  derived data.

Note the one case where `load()` still raises: if the re-fetch from version 0
returns no events, you get `AggregateNotFoundError`. That means a snapshot
existed for an aggregate with an empty event stream, which should not happen in
a healthy system -- it points at snapshot and event stores that have diverged,
or at a `create_snapshot()` called on an aggregate whose events were never
committed.

This is also the fallback most able to hide a broken override indefinitely. A
restore hook that raises on every load costs an extra fetch and a full replay on
every load, forever, while reporting nothing but `WARNING` lines. Unit-test the
round-trip; see [Implement _serialize_state() and
_restore_from_snapshot()](#implement-_serialize_state-and-_restore_from_snapshot-on-your-aggregate).

### Sync-mode snapshot writes that fail do not fail save()

`ImmediateScheduler.schedule()` is the entire sync write path,
and its body is one `try/except`:

```python
try:
    return await write
except Exception as e:
    logger.warning("Failed to create snapshot for %s/%s: %s", ..., exc_info=True)
    return None
```

where `write` is the `take_snapshot(aggregate, aggregate_type, snapshot_store)`
coroutine `save()` builds. `save()` awaits the scheduler's `schedule()` call
last -- after the events are appended, after `mark_events_as_committed()`,
after publishing -- so a snapshot failure cannot leave the aggregate or the
event store in a bad state. The events are already durable; only the
optimization was lost.

Because `take_snapshot()` does both the serialization and the store write,
this `except` also catches `_serialize_state()` raising. A serializer that is
broken for a subset of aggregates therefore produces no snapshots for them, and
the only evidence is the `WARNING` -- which does carry `exc_info`, so you get a
traceback here.

Nothing retries. The next opportunity is the next threshold boundary -- version
200 if 100 failed -- or an explicit `create_snapshot()` call.

### Background-mode snapshot tasks that fail do not fail save()

Background mode swallows failures twice over. `BackgroundScheduler.schedule()`
returns `None` the moment the task is submitted to its
`BackgroundTaskManager`, so `save()` has no failure to observe even in
principle; and the scheduler's guarded wrapper around the `take_snapshot(...)`
coroutine has its own `try/except Exception`, logging `Background snapshot
creation failed for ...` at `WARNING` with `exc_info`.

Additional ways a background snapshot can quietly not happen:

- **Process exit with tasks pending.** The scheduler's `BackgroundTaskManager`
  produces fire-and-forget tasks that are never awaited unless you call
  `await_pending_snapshots()`. A shutdown that tears down the loop drops them --
  no log, no snapshot.
- **Cancellation.** A cancelled task raises `CancelledError`, which derives from
  `BaseException` and so is *not* caught by the `except Exception` in
  the scheduler's guarded wrapper. It propagates into the task, where the
  `BackgroundTaskManager`'s done-callback discards it as cancelled without
  logging.
- **Errors surfaced only on await.** `await_pending()` gathers with
  `return_exceptions=True` and logs anything unexpected at `ERROR`
  (`Background task failed: ...`). In normal operation this never fires,
  because the per-task guarded wrapper has already caught everything --
  seeing it means something escaped that handler.

If you need certainty that a snapshot landed, background mode is the wrong tool.
Use `create_snapshot()`, which is synchronous and raises.

### Exception: create_snapshot() raises on a missing store and propagates write errors

`AggregateRepository.create_snapshot()` is the one snapshot API with no
`try/except` anywhere in its path. It:

- raises `RuntimeError` when no store is configured (`Cannot create snapshot:
  snapshot_store is not configured. Provide a snapshot_store when creating the
  repository.`) rather than silently doing nothing;
- lets `_serialize_state()` exceptions propagate;
- lets `snapshot_store.save_snapshot()` exceptions propagate.

The asymmetry is intentional, and it is the rule for the whole feature: **the
library swallows failures for work it decided to do, and reports failures for
work you asked for.** An automatic snapshot is the library's optimization, so it
must not become your error; a manual snapshot is your explicit request, so you
get to know whether it succeeded.

Practically, that makes `create_snapshot()` the tool for verifying that the
snapshot path works at all. If you suspect snapshots are silently failing, call
it once against a real aggregate: a working configuration returns a `Snapshot`,
and a broken one raises the underlying error instead of hiding it behind a
`WARNING`.

If you are calling it somewhere best-effort -- a pre-warming loop, a maintenance
script -- wrap it yourself, as shown in [Create snapshots manually at business
milestones](#create-snapshots-manually-at-business-milestones-with-create_snapshot).

### Who raises the snapshot exceptions, and how to opt into strictness

`SnapshotError` and its three subclasses -- `SnapshotDeserializationError`,
`SnapshotSchemaVersionError`, `SnapshotNotFoundError` -- are exported from
`eventsource`, and **the library's load path raises none of them.** That is not
an oversight, and the types are not dead. They exist for two audiences the
sections above have referred to as "your own code":

**`SnapshotStore` implementors.** A store that validates state or versions
itself raises the matching type instead of letting a bare `ValidationError`
escape, so the reason a snapshot was unusable is named rather than inferred.
`read_valid_snapshot()` still catches it and degrades -- but the `WARNING` line
now says what actually went wrong.

**Callers who want stricter behavior than degradation.** The extension point is
the store, not a configuration flag. Wrap whatever store you use, decide there
what is fatal, and let it propagate:

```python
class StrictSnapshotStore:
    """Refuses to silently ignore a corrupt snapshot.

    Only `get_snapshot` needs new behavior; the other four `SnapshotStore`
    methods delegate unchanged. Delegate them explicitly rather than relying
    on `__getattr__` -- `SnapshotStore` is a `Protocol`, so a missing method
    is a type error at the call site, not an `AttributeError` you will see
    in a test.
    """

    def __init__(self, inner: SnapshotStore) -> None:
        self._inner = inner

    async def get_snapshot(
        self, aggregate_id: UUID, aggregate_type: str
    ) -> Snapshot | None:
        snapshot = await self._inner.get_snapshot(aggregate_id, aggregate_type)
        if snapshot is not None and not self._looks_valid(snapshot):
            raise SnapshotDeserializationError(
                aggregate_id=aggregate_id,
                aggregate_type=aggregate_type,
            )
        return snapshot

    def _looks_valid(self, snapshot: Snapshot) -> bool:
        """Whatever "usable" means for your state model -- for example,
        `YourState.model_validate(snapshot.state)` inside a try/except."""
        ...

    # Delegated unchanged.
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        await self._inner.save_snapshot(snapshot)

    async def delete_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        return await self._inner.delete_snapshot(aggregate_id, aggregate_type)

    async def snapshot_exists(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        return await self._inner.snapshot_exists(aggregate_id, aggregate_type)

    async def delete_snapshots_by_type(
        self, aggregate_type: str, schema_version_below: int | None = None
    ) -> int:
        return await self._inner.delete_snapshots_by_type(
            aggregate_type, schema_version_below
        )
```

Note what this does and does not buy you. `read_valid_snapshot()` catches
everything, so raising from the store gets you a precise log line and a full
replay -- not a failed load. If you want the load itself to fail, wrap
`repository.load()` instead, and read the next paragraph first.

**Think hard before making a snapshot failure fatal.** A snapshot is a memoized
fold over a prefix of the event stream, and the stream is the system of record;
deleting every snapshot in the database changes load latency and nothing else.
Raising instead of replaying converts a *correct but slower* load into a failed
one, on the hottest path in the library -- one corrupt row would take every load
of that aggregate to an error, permanently, until someone deletes it. A model
change that invalidates many rows at once would become an outage rather than a
performance regression. What most operators want here is an alert, not an
exception: prefer detecting the condition -- see [Verify it is
on](#verify-it-is-on) and [Watch for the INFO line](#watch-for-the-info-line) --
over failing the request that happened to hit it.

`except SnapshotError` catches the whole family, which is the other reason the
base class exists.

### Count the degradation: `eventsource.snapshot.miss`

Everything above is why snapshot failures are quiet. This is how you stop them
being invisible.

Every read that falls back to a full replay increments a counter,
`eventsource.snapshot.miss`, with two attributes: `reason` and
`aggregate_type`. It is an OpenTelemetry counter when
`pip install "eventsource-py[telemetry]"` is present and a no-op otherwise, so
it costs nothing if you are not collecting.

| `reason` | What happened | Routine? | What to do |
| --- | --- | --- | --- |
| `missing` | No snapshot stored yet | **Yes** | Nothing. Every aggregate's first load counts here, and so does every aggregate below the threshold |
| `schema_mismatch` | Stored `schema_version` differs from the aggregate's | **Yes** | Nothing, if you just bumped it. Reclaim the dead rows with `delete_snapshots_by_type(schema_version_below=N)` |
| `store_error` | The snapshot store raised | **No** | Check the store. Usually transient |
| `deserialization_error` | The store reported the stored state as unusable | **No** | Find and rewrite the row |
| `state_restore_failed` | `_restore_from_snapshot()` raised | **No** | Find and rewrite the row, or fix the hook |

**The axis that matters is permanence, not severity.** The last three all mean
"something is wrong", but they call for opposite responses:

- `store_error` is usually **transient and broad**. The store is unreachable, so
  *every* aggregate misses at once and the rate collapses on its own when the
  backend comes back. Alert on the rate; do not go looking for a bad row.
- `deserialization_error` and `state_restore_failed` are **permanent and
  narrow**. One aggregate's row is unusable, so that aggregate replays its
  entire stream on *every single load*, forever, until someone rewrites the row.
  The rate is low and steady rather than spiky, and it will never recover by
  itself.

Before this counter both logged at `WARNING` and were indistinguishable without
reading individual lines — which is exactly the gap
[ADR 0017](../adrs/0017-snapshot-strategy-pattern.md) recorded against itself:
"silent failure means snapshot loss is only visible in logs/metrics."

A practical alert: a *sustained non-zero* rate of the permanent reasons is
always worth investigating, however small, because the cost per occurrence
grows with the stream. A spike in `store_error` is an infrastructure page. A
spike in `schema_mismatch` right after a deploy is expected and should subside
once snapshots are rewritten — if it does *not* subside, the aggregate is being
loaded but never re-snapshotted, which usually means the threshold is never
reached or the mode is `"manual"`.

Without an OpenTelemetry exporter you can still read the tally in-process,
which is what the library's own tests do:

```python
from eventsource.application.aggregates import (
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)

reset_snapshot_miss_counts()
await repo.load(aggregate_id)
print(snapshot_miss_counts())   # {'schema_mismatch': 1}
```

An empty dict after a load means the snapshot was used. That is the positive
signal the logs never gave you — "no errors" and "snapshots are working" finally
mean the same thing.
