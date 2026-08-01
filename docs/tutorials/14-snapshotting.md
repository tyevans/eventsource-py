# 14. Snapshotting

An event-sourced aggregate is rebuilt by replaying every event it ever recorded. That is
cheap at ten events and expensive at ten thousand. Snapshots are the fix: a periodic
capture of the aggregate's state so a load can start from that point and replay only what
came after.

In this tutorial you will feel the cost of a full replay, turn snapshotting on, watch the
first snapshot appear, take one by hand, invalidate one with a schema bump, and finally
move the snapshots onto disk.

## What you'll build

A `Counter` aggregate with 500 events behind it, loaded four different ways:

1. With no snapshot store at all -- full replay, every time.
2. With an `InMemorySnapshotStore` and `snapshot_threshold=100` -- snapshots written
   automatically at version boundaries.
3. With `snapshot_mode="manual"` -- nothing automatic, snapshots only when you ask.
4. With a `SQLiteSnapshotStore` -- snapshots that survive the process.

Everything runs in one file. The event store stays in memory the whole way through, so
the only thing changing between runs is the snapshot configuration.

## Prerequisites

- Tutorial 3, [Your First Aggregate](03-first-aggregate.md) -- you should be comfortable
  with `DeciderAggregate`, `decide()`/`evolve()`, and `AggregateRepository`.
- Python 3.13 or newer.
- `eventsource-py` installed. Steps 1-9 need nothing beyond the core package; Step 10
  writes snapshots to a file and needs the `sqlite` extra (`aiosqlite`):

```bash
uv sync --all-extras          # or: pip install "eventsource-py[sqlite]"
```

Create a file called `snapshotting.py` and add to it as you go.

## Step 1: Define an aggregate with a long event history

You need an aggregate whose state grows with each event, so a replay does real work.
`Counter` accumulates a running total plus one string per event:

```python
import asyncio
import time
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from eventsource import (
    AggregateRepository,
    DeclarativeAggregate,
    DomainEvent,
    InMemorySnapshotStore,
    handles,
)
from eventsource.adapters.memory import InMemoryEventStore


class CounterState(BaseModel):
    counter_id: UUID
    total: int = 0
    entries: list[str] = Field(default_factory=list)


class PointsAwarded(DomainEvent):
    event_type: str = "PointsAwarded"
    aggregate_type: str = "Counter"
    points: int
    reason: str


class Counter(DeclarativeAggregate[CounterState]):
    aggregate_type = "Counter"
    schema_version = 1

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def award(self, points: int, reason: str) -> None:
        self.create_event(PointsAwarded, points=points, reason=reason)

    @handles(PointsAwarded)
    def _on_points(self, event: PointsAwarded) -> None:
        state = self._state or self._get_initial_state()
        self._state = state.model_copy(
            update={
                "total": state.total + event.points,
                "entries": [*state.entries, event.reason],
            }
        )
```

Two details matter for snapshotting:

- `schema_version = 1` is a class attribute on `AggregateRoot`, defaulting to `1`. It is
  read off the aggregate class and stamped onto every snapshot this aggregate produces,
  then checked again on every load. You will change it in Step 9.
- `CounterState` is a Pydantic model. `AggregateRoot._serialize_state()` dumps it with
  `model_dump(mode="json")`, and `_restore_from_snapshot()` rebuilds it with
  `model_validate()` -- so whatever your state model can round-trip through JSON, a
  snapshot can carry.

The `entries` list is there on purpose: it makes the state grow linearly with the event
count, so a 500-event replay does 500 list copies rather than 500 integer additions. That
is what makes the timing difference in Step 6 visible in an in-memory example.

## Step 2: Write 500 events and load without snapshots

Start with a repository that has no snapshot store -- the default:

```python
async def main() -> None:
    event_store = InMemoryEventStore()

    plain_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Counter,
        aggregate_type="Counter",
    )

    counter_id = uuid4()
    counter = plain_repo.create_new(counter_id)
    for i in range(500):
        counter.award(points=1, reason=f"entry-{i}")
    await plain_repo.save(counter)

    print("has_snapshot_support:", plain_repo.has_snapshot_support)
    print("version:", counter.version)


asyncio.run(main())
```

Run it:

```
has_snapshot_support: False
version: 500
```

`create_new()` just calls the factory -- it builds an in-memory aggregate at version 0 and
persists nothing. The 500 `award()` calls each append one uncommitted event; the single
`save()` appends them all to the store atomically, marks them committed, and leaves the
aggregate at version 500. (`save()` is a no-op if there is nothing uncommitted.)

`has_snapshot_support` is `False` because no `snapshot_store` was passed. Note what is
*not* enough on its own: `snapshot_mode` already defaults to `"sync"`, but with no store
there is nothing to write to and nothing to read from, so every load in Steps 2 and 3
replays all 500 events. This is the baseline you will measure against.

## Step 3: Time the cold load and see the cost of full replay

Add a timed load -- three times, so you can see it is not a warm-up artifact:

```python
    for _ in range(3):
        start = time.perf_counter()
        loaded = await plain_repo.load(counter_id)
        elapsed_ms = (time.perf_counter() - start) * 1000
        print(f"full replay: {elapsed_ms:.1f} ms  version={loaded.version} total={loaded.state.total}")
```

```
full replay: 1.8 ms  version=500 total=500
full replay: 1.7 ms  version=500 total=500
full replay: 1.7 ms  version=500 total=500
```

Your numbers will differ, but the shape will not: the three loads cost the same, because
each one does exactly the same work. With no snapshot manager configured, `load()` sets
`from_version = 0`, calls `get_events(aggregate_id, aggregate_type=..., from_version=0)`,
builds a brand-new aggregate with the factory, and replays the whole stream through
`load_from_history()`. Nothing is cached between calls -- the repository holds no identity
map, so the second load is as expensive as the first.

Two things worth noticing while you are here:

- `loaded` is a different object from `counter`. Every `load()` reconstructs from events;
  the aggregate you saved is not handed back to you.
- If there were no events *and* no snapshot, `load()` would raise
  `AggregateNotFoundError` rather than return an empty aggregate. (Use
  `load_or_create()` when you want the empty one.)

In this tutorial the events are already in RAM and the state is a small Pydantic model, so
500 events cost under two milliseconds. That number is not the point. The point is that it
is *linear in the number of events*, and events only ever accumulate: double the history
and you double the load. Try changing `range(500)` to `range(5000)` and re-running -- the
per-load time scales with it. Against a real event store, with a network round trip and
richer state, this is the cost that eventually makes a long-lived aggregate too slow to
load on a request path.

That linear growth is the problem snapshots solve. Everything from here on is about
putting a floor under it.

## Step 4: Add an InMemorySnapshotStore to the repository

Snapshotting is enabled by handing the repository a snapshot store. Keep the same event
store, so the same 500 events are still there:

```python
    snapshot_store = InMemorySnapshotStore()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Counter,
        aggregate_type="Counter",
        snapshot_store=snapshot_store,
        snapshot_threshold=100,
        snapshot_mode="sync",
    )

    print("has_snapshot_support:", repo.has_snapshot_support)
    print("mode:", repo.snapshot_mode, "threshold:", repo.snapshot_threshold)
    print("store:", snapshot_store, "count:", snapshot_store.snapshot_count)
```

```
has_snapshot_support: True
mode: sync threshold: 100
store: InMemorySnapshotStore(snapshots=0) count: 0
```

`InMemorySnapshotStore` is a dict keyed by `(aggregate_id, aggregate_type)` -- no setup, no
schema, no I/O. It is the right store for this tutorial and for tests, and the wrong one
for production (Step 10 fixes that).

The three snapshot parameters:

- `snapshot_store` -- any `SnapshotStore` implementation. Passing it is the switch:
  internally the repository composes a `SnapshotPolicy` and `SnapshotScheduler`, and
  `has_snapshot_support` is literally "is `snapshot_store` not `None`". Leave it out and
  the policy is `Never()`, which is the Step 2 behavior.
- `snapshot_threshold` -- how many events between automatic snapshots. `None` (the
  default) means the repository selects the `Never()` policy, whose `should_snapshot()`
  always returns `False`, so nothing is ever written automatically, whatever the mode.
- `snapshot_mode` -- `"sync"` (the default: `EveryNEvents(threshold)` paired with
  `ImmediateScheduler`, which writes the snapshot before `save()` returns), `"background"`
  (`EveryNEvents(threshold)` paired with `BackgroundScheduler`, a fire-and-forget task --
  use `repo.pending_snapshot_count` and `await repo.await_pending_snapshots()` to observe
  it), or `"manual"` (`Never()`, never automatic). See
  [ADR 0021](../adrs/0021-snapshot-policy-scheduler-composition.md) for how these
  collaborators fit together.

Mode and threshold are independent, and both must be set for anything to happen on its
own: `snapshot_mode="sync"` with no threshold writes nothing, and `snapshot_threshold=100`
with `snapshot_mode="manual"` also writes nothing. The combination above -- store, plus a
threshold, plus a non-manual mode -- is the one that snapshots by itself.

Two things this step does *not* do. It does not write a snapshot: the manager only
considers one after a successful `save()`, and you have not saved through `repo` yet. And
it does not touch the 500 events already in `event_store` -- this is the same store object
from Step 2, so `repo` is looking at the same aggregate. Adding a snapshot store to an
aggregate that already has history is safe and retroactive: with no snapshot on file,
`load_valid_snapshot()` returns `None` and `load()` falls back to the same full replay you
timed in Step 3. Nothing gets faster until a snapshot exists, which is Step 5.

## Step 5: Set snapshot_threshold and let the first snapshot be written

You set the threshold in Step 4 (`snapshot_threshold=100`). Now earn a snapshot with it.

The rule is exact, and it is worth reading before you run anything. After a successful
`save()`, the repository asks the strategy `should_snapshot()`, which returns:

```python
aggregate.version > 0 and aggregate.version % self._threshold == 0
```

That is a check on the *resulting version*, evaluated once per save. It is not a count of
events written, and it does not track how long it has been since the last snapshot.
Append one event and see:

```python
    c = await repo.load(counter_id)
    c.award(points=1, reason="off-boundary")
    await repo.save(c)

    print("version:", c.version)
    print("snapshot:", await snapshot_store.get_snapshot(counter_id, "Counter"))
    print("count:", snapshot_store.snapshot_count)
```

```
version: 501
snapshot: None
count: 0
```

Nothing was written. The aggregate crossed version 500 -- a perfectly good multiple of 100
-- but it crossed it back in Step 2, on a repository that had no snapshot store. This save
landed on 501, and 501 % 100 is 1, so `should_snapshot()` returned `False` and the manager
returned early without touching the store. Note that `load()` here was still a full replay
of 500 events, and `save()` appended exactly one.

Now push the aggregate onto the next boundary:

```python
    c = await repo.load(counter_id)
    for i in range(99):
        c.award(points=1, reason=f"batch-{i}")
    await repo.save(c)

    print("version:", c.version)
    print("snapshot:", await snapshot_store.get_snapshot(counter_id, "Counter"))
```

```
version: 600
snapshot: Snapshot(Counter/4687a41d-..., v600, schema_v1)
```

There it is -- your first snapshot, written by the library rather than by you. 99 events
took the aggregate from 501 to 600, `600 % 100 == 0`, and `ImmediateScheduler`
serialized the state and called `save_snapshot()` before `save()` returned. That is what
`snapshot_mode="sync"` buys you: by the time the `await repo.save(c)` line finishes, the
snapshot is durable. (Under `"background"` it would not be -- you would need
`await repo.await_pending_snapshots()` first.)

Three consequences of "once per save, on the resulting version" that will bite you if you
skip past them:

- **A single save can jump the boundary.** Had you awarded 198 points instead of 99, the
  aggregate would have gone 501 -> 699 in one save, and 699 is not a multiple of 100. No
  snapshot, no warning; the next chance is 700. Aggregates that save large batches want a
  small threshold, an explicit `create_snapshot()` (Step 8), or both.
- **The snapshot reflects the whole aggregate, not the batch.** The state written at
  version 600 includes all 600 events, including the 500 that predate the snapshot store
  existing. Snapshots are always a full state capture, never a delta.
- **The events are still there.** Nothing was deleted or compacted. Run
  `get_events(counter_id, aggregate_type="Counter")` and all 600 come back. The snapshot
  only changes where a *load* starts reading.

One more property worth relying on: snapshotting cannot break a save. Every strategy wraps
creation in a `try/except` that logs a warning and returns `None`. If your snapshot store
is down, the events are already committed, the aggregate is already at its new version,
and the only cost is that the next load replays more.

The snapshot exists now. Step 6 measures what it is worth.

## Step 6: Load again and compare the timings

Same aggregate, same event store, same loop as Step 3:

```python
    for _ in range(3):
        start = time.perf_counter()
        warm = await repo.load(counter_id)
        elapsed_ms = (time.perf_counter() - start) * 1000
        print(f"snapshot load: {elapsed_ms:.1f} ms  version={warm.version} total={warm.state.total}")
```

```
snapshot load: 0.1 ms  version=600 total=600
snapshot load: 0.1 ms  version=600 total=600
snapshot load: 0.1 ms  version=600 total=600
```

From ~1.8 ms to ~0.1 ms, and the reconstructed state is identical: `total=600` at version
600. The load did this:

1. Asked the snapshot manager for a valid snapshot -- got one at version 600.
2. Called `get_events(..., from_version=600)` -- which returned nothing, because there are
   no events past 600 yet.
3. Restored state from the snapshot with `_restore_from_snapshot()` and returned.

Zero events replayed. Add events after the snapshot and only those get replayed; the
snapshot version is the floor.

## Step 7: Inspect the Snapshot object (version, state, schema_version, created_at)

A `Snapshot` is a frozen dataclass with six fields. Print it:

```python
    snap = await snapshot_store.get_snapshot(counter_id, "Counter")

    print("aggregate_id: ", snap.aggregate_id)
    print("aggregate_type:", snap.aggregate_type)
    print("version:      ", snap.version)
    print("schema_version:", snap.schema_version)
    print("created_at:   ", snap.created_at)
    print("state keys:   ", list(snap.state.keys()))
    print("state total:  ", snap.state["total"], "entries:", len(snap.state["entries"]))
    print("repr:", repr(snap))
```

```
aggregate_id:  4db40c1e-f905-45e6-97b5-91156de82ed1
aggregate_type: Counter
version:       600
schema_version: 1
created_at:    2026-07-28 03:47:39.325273+00:00
state keys:    ['counter_id', 'total', 'entries']
state total:   600 entries: 600
repr: Snapshot(aggregate_id=UUID('4db40c1e-...'), aggregate_type='Counter', version=600, schema_version=1, state_keys=['counter_id', 'total', 'entries'], created_at=datetime.datetime(2026, 7, 28, 3, 47, 39, 325273, tzinfo=datetime.timezone.utc))
```

What to notice:

- `version` is the aggregate version the state corresponds to. Events with version greater
  than this still need replaying.
- `state` is a plain JSON-compatible dict -- exactly your `CounterState` fields.
- `schema_version` came from the aggregate class, not from anything you passed in.
- `created_at` is timezone-aware UTC.
- `__repr__` prints `state_keys` rather than the state itself, so logging a snapshot does
  not dump your whole aggregate.
- The store holds exactly one snapshot per `(aggregate_id, aggregate_type)`. Saving a
  newer one replaces the old (upsert).

Snapshots are a cache, never the source of truth. Delete every snapshot you have and the
system still reconstructs identical state from events.

## Step 8: Take a snapshot explicitly with snapshot_mode="manual" and create_snapshot()

Sometimes the interesting moment is a business milestone, not an arithmetic boundary. Use
`snapshot_mode="manual"`, which installs a strategy that never fires automatically:

```python
    manual_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Counter,
        aggregate_type="Counter",
        snapshot_store=snapshot_store,
        snapshot_mode="manual",
    )

    m = await manual_repo.load(counter_id)
    m.award(points=1, reason="milestone")
    await manual_repo.save(m)

    stored = await snapshot_store.get_snapshot(counter_id, "Counter")
    print("aggregate version:", m.version, " snapshot version:", stored.version)

    explicit = await manual_repo.create_snapshot(m)
    print("explicit:", explicit)
```

```
aggregate version: 601  snapshot version: 600
explicit: Snapshot(Counter/4db40c1e-..., v601, schema_v1)
```

The save left the old v600 snapshot alone; `create_snapshot()` replaced it with v601.
`create_snapshot()` writes immediately and synchronously whatever the configured mode --
you can call it on a `"sync"` or `"background"` repository too.

It does need a store, though:

```python
    no_store = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Counter,
        aggregate_type="Counter",
    )
    try:
        await no_store.create_snapshot(m)
    except RuntimeError as exc:
        print("RuntimeError:", exc)
```

```
RuntimeError: Cannot create snapshot: snapshot_store is not configured. Provide a snapshot_store when creating the repository.
```

## Step 9: Bump schema_version and watch the stale snapshot fall back to full replay

A snapshot stores serialized state. Change the shape of that state -- rename a field, drop
one, change its meaning -- and old snapshots are lies. `schema_version` is how you declare
that break. Define a version-2 aggregate and load through it:

```python
class CounterV2(Counter):
    aggregate_type = "Counter"
    schema_version = 2
```

```python
    v2_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=CounterV2,
        aggregate_type="Counter",
        snapshot_store=snapshot_store,
        snapshot_threshold=100,
        snapshot_mode="sync",
    )

    start = time.perf_counter()
    v2 = await v2_repo.load(counter_id)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"v2 load: {elapsed_ms:.1f} ms  version={v2.version} total={v2.state.total}")

    still_there = await snapshot_store.get_snapshot(counter_id, "Counter")
    print("snapshot on disk:", still_there.version, "schema", still_there.schema_version)
```

```
v2 load: 2.3 ms  version=601 total=601
snapshot on disk: 601 schema 1
```

The timing is back to full-replay territory, and the state is still correct. The manager
fetched the snapshot, saw `schema_version=1` against the aggregate's `2`, logged a
mismatch at INFO level, and returned `None` -- which the repository treats exactly like
"no snapshot": replay from version 0.

This is the general shape of snapshot failure handling. A snapshot store that raises, a
snapshot whose state no longer validates against `TState` -- both are caught, logged as a
warning, and fall back to full replay. A bad snapshot slows you down; it never corrupts
you.

The stale snapshot is still sitting in the store, useless. Clean up in bulk:

```python
    removed = await snapshot_store.delete_snapshots_by_type("Counter", schema_version_below=2)
    print("deleted:", removed)
```

```
deleted: 1
```

`delete_snapshots_by_type()` is optional on the `SnapshotStore` base class (the default
raises `NotImplementedError`); `InMemorySnapshotStore`, `PostgreSQLSnapshotStore`, and
`SQLiteSnapshotStore` all implement it. For a single aggregate there is also
`delete_snapshot(aggregate_id, aggregate_type)`, which returns `True` if something was
removed.

## Step 10: Swap InMemorySnapshotStore for a persistent store

`InMemorySnapshotStore` loses everything when the process exits -- which makes it useless
for the one thing snapshots are for, namely making the *first* load after a restart fast.
Swap in `SQLiteSnapshotStore`. It needs its table created first; the schema ships with the
library:

```python
    import aiosqlite

    from eventsource.adapters.sql.schemas import get_schema
    from eventsource.adapters.sqlite import SQLiteSnapshotStore

    db_path = "snapshots.db"

    async with aiosqlite.connect(db_path) as db:
        await db.executescript(get_schema("snapshots", backend="sqlite"))
        await db.commit()

    sqlite_store = SQLiteSnapshotStore(db_path)

    sqlite_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Counter,
        aggregate_type="Counter",
        snapshot_store=sqlite_store,
        snapshot_threshold=100,
        snapshot_mode="sync",
    )

    agg = await sqlite_repo.load(counter_id)
    print("saved:", await sqlite_repo.create_snapshot(agg))

    # A brand-new store object, reading the same file:
    reopened = SQLiteSnapshotStore(db_path)
    print("read back:", await reopened.get_snapshot(counter_id, "Counter"))
```

```
saved: Snapshot(Counter/84fbcd7b-..., v601, schema_v1)
read back: Snapshot(Counter/84fbcd7b-..., v601, schema_v1)
```

The repository code is unchanged except for the store instance -- that is the whole point
of the `SnapshotStore` interface. For PostgreSQL the swap is the same shape:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker

from eventsource.adapters.postgresql import PostgreSQLSnapshotStore

session_factory = async_sessionmaker(engine, expire_on_commit=False)
snapshot_store = PostgreSQLSnapshotStore(session_factory)
```

with `get_schema("snapshots")` (PostgreSQL is the default backend) applied to the database
first. `SQLiteSnapshotStore` requires the `aiosqlite` extra; check
`eventsource.adapters.sqlite.SQLITE_AVAILABLE` if you need to degrade gracefully.

## What you learned

- A repository without a snapshot store replays every event on every load, and that cost
  grows with the aggregate's age.
- Snapshotting turns on by passing `snapshot_store` to `AggregateRepository`;
  `snapshot_threshold` and `snapshot_mode` control when snapshots get written.
- The automatic rule is `version % threshold == 0`, evaluated once per successful save --
  so a save that jumps past a boundary skips it.
- `"sync"` writes before `save()` returns, `"background"` writes in a task
  (`await_pending_snapshots()` waits for it), `"manual"` never writes automatically.
- `repo.create_snapshot(aggregate)` writes one on demand, in any mode, and raises
  `RuntimeError` if no store is configured.
- A `Snapshot` carries `aggregate_id`, `aggregate_type`, `version`, JSON `state`,
  `schema_version`, and `created_at`; stores keep only the latest one per aggregate.
- Bumping the aggregate's `schema_version` invalidates old snapshots -- the load logs a
  mismatch and falls back to full replay, still producing correct state.
- Every snapshot failure mode degrades to full replay, never to wrong state. Snapshots
  are a cache; events are the truth.
- Swapping `InMemorySnapshotStore` for `SQLiteSnapshotStore` or
  `PostgreSQLSnapshotStore` changes one line, plus creating the table with
  `get_schema("snapshots", backend=...)`.

## Next steps

- Try `snapshot_mode="background"` and confirm with `repo.pending_snapshot_count` and
  `await repo.await_pending_snapshots()` that the snapshot really is written off the save
  path.
- Read `eventsource/application/aggregates/snapshotting.py` and write your own
  `SnapshotPolicy` -- for example, one that snapshots on elapsed time rather than event
  count -- and pass it as `snapshot_policy=` to `AggregateRepository`.
- Plan a real schema migration: bump `schema_version`, deploy, then run
  `delete_snapshots_by_type(aggregate_type, schema_version_below=N)` to evict the stale
  snapshots in one pass.
