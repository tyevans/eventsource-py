# Repository Operations

This guide shows you how to operate the three infrastructure repositories that
`eventsource` ships in `eventsource.repositories`:

- **Outbox** (`OutboxRepository`) -- the transactional outbox: events are written
  alongside your business data, then drained to the bus by a separate loop.
- **Dead letter queue** (`DLQRepository`) -- events a projection failed to
  process, held for inspection, replay, and resolution.
- **Checkpoints** (`CheckpointRepository`) -- per-projection positions and
  per-subscription cursors, so consumers resume where they left off.

Each of the three is defined as a `runtime_checkable` Protocol with three
interchangeable implementations -- `PostgreSQL*`, `SQLite*`, and `InMemory*` --
so the recipes below work the same whichever backend you wire up. All methods
are `async`; every call in this guide must be awaited.

```python
from eventsource.repositories import (
    CheckpointRepository,
    DLQRepository,
    OutboxRepository,
    PostgreSQLCheckpointRepository,
    PostgreSQLDLQRepository,
    PostgreSQLOutboxRepository,
)
```

The `*RepositoryProtocol` names exported alongside them (for example
`OutboxRepositoryProtocol`) are aliases of the same Protocols, kept for
backward compatibility.

Use this guide when you are running the library in production and need to drain
the outbox, keep the DLQ from growing without bound, watch projection lag, or
build a scheduled maintenance job that does all three. It assumes you already
have an event store and at least one projection running; if you are still
choosing components, start with the subscription and error-handling guides
first. For the exact signatures, return types, and per-backend notes, see the
repositories API reference; for *why* the outbox exists at all, see the
explanation of the transactional outbox pattern.

## Before you begin

You need three things in place before any recipe in this guide will run:

1. **The tables exist.** All three repositories read and write dedicated
   tables -- `event_outbox`, `dead_letter_queue`, and `projection_checkpoints`.
   None of the implementations create their own schema. Load the DDL from
   `eventsource.migrations` and apply it once, at deploy time:

   ```python
   from eventsource.migrations import get_schema

   ddl = get_schema("all", backend="postgresql")  # or backend="sqlite"
   ```

   `get_schema` also takes `"outbox"`, `"checkpoints"`, and `"dlq"`
   individually if you only need one table.

2. **A live connection.** The PostgreSQL and SQLite repositories take a
   connection (or engine) in their constructor and hold onto it; they do not
   open one for you.

3. **An event loop.** Every repository method is a coroutine, including the
   read-only ones like `get_stats()` and `get_lag_metrics()`.

### Choosing a backend (PostgreSQL, SQLite, InMemory)

All three implementations satisfy the same Protocol, so the choice is
operational rather than an API difference:

| Implementation | Use it for | Notes |
| --- | --- | --- |
| `PostgreSQL*` | Production | Native `UUID` and `JSONB` columns, `COUNT(*) FILTER` aggregates, and interval arithmetic. The schema also ships helper functions (`get_pending_outbox_events`, `add_to_dlq`, `update_projection_checkpoint`, ...) and reporting views (`event_outbox_stats`, `dlq_summary`, `projection_checkpoint_stats`). |
| `SQLite*` | Single-node deployments, embedded use, local development | Same behaviour, different storage: UUIDs are stored as 36-character hyphenated `TEXT`, timestamps as ISO 8601 `TEXT`, JSON as `TEXT`, and the aggregates use `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` because SQLite has no `FILTER` clause. Requires SQLite 3.24+ for the `ON CONFLICT` upserts. |
| `InMemory*` | Tests | No schema, no connection, no persistence -- everything is lost when the process exits. State is guarded by an `asyncio.Lock`, so it is consistent within a single process and invisible outside it. |

The constructors differ (see the next section), but nothing else does: swapping
`PostgreSQLOutboxRepository` for `SQLiteOutboxRepository` changes no call site
in the recipes below.

Pick one backend for all three repositories in a given deployment. Nothing
enforces that, but a checkpoint in SQLite and an outbox in PostgreSQL cannot
participate in the same transaction, which defeats the point of the outbox.

Two caveats when you scale out:

- **Use PostgreSQL if more than one process is involved.** The in-memory
  implementation is per-process, and SQLite's single-writer model serializes
  concurrent writers and will surface lock errors under load.
- **Even on PostgreSQL, `get_pending_events` does not lock rows.** The
  repository method is a plain `SELECT ... WHERE status = 'pending' ORDER BY
  created_at ASC LIMIT :limit`, so two drain loops running at once will claim
  the same batch and publish it twice. Run a single drain worker, or call the
  `get_pending_outbox_events(...)` SQL function from the shipped schema, which
  does use `FOR UPDATE SKIP LOCKED`. Consumers must deduplicate regardless --
  see the idempotency section below.

### Constructing the repositories from an engine or session factory

All three PostgreSQL repositories take the same first argument, named `conn`,
and accept either an `AsyncEngine` or an `AsyncConnection`. The distinction
decides who owns the transaction:

- **Pass an `AsyncEngine`** and each method opens and closes its own
  connection: writes run inside `engine.begin()` (committed when the method
  returns), reads on a bare `engine.connect()`. Use this for the drain loop,
  the DLQ sweep, checkpoint updates, and monitoring, where each call stands
  alone.
- **Pass an `AsyncConnection`** and every method executes on that connection
  and does *nothing* about transactions -- you own the `begin()`/`commit()`.
  Use this for `add_event`, so the outbox row commits atomically with the
  business data written on the same connection. If you forget to commit, the
  outbox row is silently rolled back with the rest of your work, which is
  exactly the behaviour the outbox pattern relies on.

```python
from sqlalchemy.ext.asyncio import create_async_engine

from eventsource.repositories import (
    PostgreSQLCheckpointRepository,
    PostgreSQLDLQRepository,
    PostgreSQLOutboxRepository,
)

engine = create_async_engine("postgresql+asyncpg://localhost/app")

# Engine-backed: for the background drain, sweeps, and metrics.
outbox = PostgreSQLOutboxRepository(engine)
dlq = PostgreSQLDLQRepository(engine)
checkpoints = PostgreSQLCheckpointRepository(engine)

# Connection-backed: for enqueuing inside your own write transaction.
async with engine.begin() as conn:
    await write_business_data(conn, ...)
    await PostgreSQLOutboxRepository(conn).add_event(event)
    # both rows commit together when the block exits
```

If you work with `async_sessionmaker` rather than raw connections, get the
underlying connection from the session -- `await session.connection()` -- and
hand that to the repository so the outbox write joins the session's
transaction.

Both forms are cheap to construct -- the repositories hold a reference and
nothing else -- so building a short-lived, connection-backed repository inside
a write path, as above, is the intended usage rather than a workaround.

The SQLite repositories take a single `aiosqlite.Connection` instead, and the
in-memory ones take no connection at all:

```python
import aiosqlite

from eventsource.repositories import (
    InMemoryOutboxRepository,
    SQLiteOutboxRepository,
)

async with aiosqlite.connect("events.db") as db:
    outbox = SQLiteOutboxRepository(db)

test_outbox = InMemoryOutboxRepository()
```

One behavioural difference to plan around: the SQLite repositories call
`await connection.commit()` themselves at the end of every write, including
`add_event`. They cannot join a wider transaction of yours, so on SQLite the
outbox write is not atomic with your business write unless you arrange the
ordering yourself (write the business data first, enqueue second, and be
prepared for a crash in between to lose the outbox row). If you need the
transactional guarantee, use PostgreSQL.

Every constructor also accepts `tracer=` and `enable_tracing=`; leave them at
their defaults unless you are wiring OpenTelemetry, which the observability
guide covers.

## Drain the outbox

The outbox is drained by code you write: nothing in `eventsource` runs a drain
loop for you. The repository gives you five primitives -- `add_event`,
`get_pending_events`, `mark_published`, `increment_retry`, and `mark_failed` --
and the recipe below wires them into a worker that publishes each pending entry
exactly once per successful attempt and never loses one on failure.

The full loop, which the next five steps take apart:

```python
from eventsource import EventTypeNotFoundError, get_event_class

MAX_RETRIES = 5


async def drain_once(outbox, bus, batch_size: int = 100) -> int:
    entries = await outbox.get_pending_events(limit=batch_size)
    for entry in entries:
        try:
            event = rehydrate(entry)
            await bus.publish([event])
        except EventTypeNotFoundError as exc:
            await outbox.mark_failed(entry.id, f"unknown event type: {exc}")
        except Exception as exc:
            if entry.retry_count + 1 >= MAX_RETRIES:
                await outbox.mark_failed(entry.id, str(exc))
            else:
                await outbox.increment_retry(entry.id, str(exc))
        else:
            await outbox.mark_published(entry.id)
    return len(entries)
```

### Step 1: Enqueue events with `add_event` inside the write transaction

`add_event(event)` inserts one row with `status = 'pending'`, `retry_count = 0`,
and `created_at` set to the current UTC time, then returns the row's `UUID` --
the `outbox_id` every other method takes. It takes exactly one event and
publishes nothing.

```python
async with engine.begin() as conn:
    await store.append(stream, [event], expected)   # on the same connection
    outbox_id = await PostgreSQLOutboxRepository(conn).add_event(event)
```

Three things matter here:

- **Use a connection-backed repository.** As covered above, a repository built
  on an `AsyncConnection` does not manage transactions, so the outbox row
  commits or rolls back with your business write. A repository built on an
  `AsyncEngine` opens its own transaction and commits immediately -- which
  would let an event be published for a write that then rolled back. (On
  SQLite there is no such choice: `add_event` commits the connection itself.)
- **The event is snapshotted, not referenced.** `add_event` stores a JSON
  document containing `event_id`, `aggregate_id`, `aggregate_type`,
  `tenant_id`, `occurred_at`, and a `payload` holding the full
  `event.model_dump_json()`. The drain reads only that document; it never
  reads the event store, so the outbox survives the event being archived or
  the aggregate being deleted.
- **The returned id is not the event id.** It is a fresh `uuid4()` per row.
  Call `add_event` twice with the same event and you get two pending rows with
  two `outbox_id` values and one `event_id` -- which is why deduplication
  downstream keys on `event_id`, not on the outbox id.

The scalar columns -- `event_type`, `aggregate_id`, `aggregate_type`,
`tenant_id` -- are copied out of the event alongside the JSON document, so you
can query and route on them without parsing it.

Enqueue one row per event you want delivered. Appending a batch to the store
means a loop:

```python
async with engine.begin() as conn:
    outbox = PostgreSQLOutboxRepository(conn)
    await store.append(stream, events, expected)
    for event in events:
        await outbox.add_event(event)
```

Nothing deduplicates on the way in, and no unique constraint stops a repeated
`event_id`, so make the enqueue happen exactly where the state change is
committed -- once -- rather than in a retry-prone caller above it.

### Step 2: Claim a batch with `get_pending_events(limit=...)` (FIFO by `created_at`)

```python
entries = await outbox.get_pending_events(limit=100)
```

Returns up to `limit` (default 100) `OutboxEntry` dataclasses whose `status` is
`'pending'`, oldest first by `created_at`, and an empty list when there is
nothing to do. `list_pending_events(limit=...)` is an alias that calls the same
method, so use whichever name reads better.

"Claim" overstates what happens. On every backend the read is a plain
`SELECT ... WHERE status = 'pending' ORDER BY created_at ASC LIMIT ...` (the
in-memory version sorts the same way in Python) with no row locking and no
status change. Two callers running at once get the same rows, and a row stays
in the pending set until you call `mark_published` or `mark_failed` on it --
`increment_retry` does not remove it. So:

- **Run one drain worker per outbox table.** This is the simple, correct
  answer, and the reason the background-loop section below says to run exactly
  one.
- **Or claim with locking in SQL.** The PostgreSQL schema ships
  `get_pending_outbox_events(batch_size, max_retries)`, which does the same
  ordered read but adds `FOR UPDATE SKIP LOCKED` and a `retry_count <
  max_retries` filter, so concurrent workers take disjoint batches. Call it
  yourself; the repository method does not use it.
- **Either way, consumers still have to deduplicate** -- a crash between
  publishing and marking replays the entry. See the idempotency section.

Size `limit` to how much you are willing to redeliver after a crash, not to how
much you can fetch. A full batch is also your backlog signal: if the call
returns `limit` entries, more work is waiting.

Each entry carries `id` (the `outbox_id` for the mark/retry calls), `event_id`,
`event_type`, `aggregate_id`, `aggregate_type`, `tenant_id`, `event_data`,
`created_at`, `status`, and `retry_count`. Read `retry_count` here -- it is how
step 4 decides whether the entry has any budget left. The SQL-backed
implementations do not select `published_at` or `last_error` for this query, so
those attributes are `None` on returned entries regardless of what is in the
row; if you want the last error text, query the table directly.

`event_data` is typed `str | dict[str, Any]` and the backends genuinely differ:
PostgreSQL's `JSONB` column deserializes to a `dict`, while SQLite and InMemory
hand you the JSON `str` that was stored. Normalize before use, and remember the
event itself lives under the `payload` key of that document:

```python
import json

from eventsource import get_event_class


def rehydrate(entry):
    data = entry.event_data
    if isinstance(data, str):
        data = json.loads(data)
    return get_event_class(entry.event_type).model_validate(data["payload"])
```

`get_event_class` raises `EventTypeNotFoundError` when nothing is registered
under that `event_type`. The registry is populated as a side effect of
importing the module that defines the event, so a drain worker that does not
import your event modules will fail on every entry. Import them at startup;
treat a miss as a poison entry (step 5), not a transient error.

One in-memory quirk worth knowing if you assert against entries in tests:
`InMemoryOutboxRepository` returns the live `OutboxEntry` objects it stores, not
copies, so a later `mark_published` mutates the entry you are already holding.
The SQL backends build fresh objects per call.

### Step 3: Publish each entry to the bus and call `mark_published(outbox_id)`

```python
await bus.publish([event])
await outbox.mark_published(entry.id)
```

`EventBus.publish` takes a *list* of events, so a single entry goes out as a
one-element list. `mark_published(outbox_id)` then sets `status = 'published'`
and stamps `published_at` with `datetime.now(UTC)`. It returns nothing.

Those two lines are the whole happy path, and the order is the only thing about
them that matters:

- **Publish first, mark second.** If the process dies between the two calls,
  the entry is still `pending`, so the next pass republishes it -- a duplicate,
  which consumers are required to absorb (see the idempotency section below).
  Mark first and die before the publish and the event is gone: no repository
  method moves a `published` entry back into the pending set.
- **Mark immediately, one entry at a time.** Do not collect ids and flush the
  marks at the end of the batch. Every id you are holding when the worker dies
  is an id that gets republished, so batching the marks trades nothing for a
  wider duplicate window.
- **Await the publish for real.** `publish(events, background=True)` on the
  buses that support it hands off to a background task and returns before
  delivery is attempted, so `mark_published` right after it marks an entry that
  may never reach a subscriber. Leave `background` at its default in a drain
  loop; the loop is already the background.

Publishing one event per call also keeps the publish/mark mapping one-to-one.
If you pass a whole batch to a single `bus.publish(events)` call and it raises
part-way through, you cannot tell which events were delivered, so you cannot
tell which entries to mark -- mark none of them and let the entire batch be
retried and deduplicated downstream.

`mark_published` is an unconditional `UPDATE ... WHERE id = :id` (a dict lookup
on `InMemory*`), which has two practical consequences. It does not check the
current status, so calling it on an entry you previously passed to `mark_failed`
will quietly resurrect it as `published`. And it does not check that the row
exists: an `outbox_id` that was never enqueued, or one already removed by
retention, updates zero rows and raises nothing. Only ever pass an `entry.id`
that came out of `get_pending_events`.

Marking is also what starts the retention clock. `cleanup_published(days=...)`
deletes on `status = 'published' AND published_at < now - days`, so an entry
that is published but never marked is never pruned either -- it sits in the
pending set being redelivered forever, which is what the `pending_count` and
`oldest_pending` alerts in the monitoring section are there to catch.

### Step 4: Handle transient failures with `increment_retry(outbox_id, error)`

```python
await outbox.increment_retry(entry.id, str(exc))
```

`increment_retry` bumps `retry_count` by one and writes `last_error` (the
`error` argument is optional and defaults to `None`). It leaves `status` alone,
so the entry stays pending and the next pass picks it up again.

There is no backoff and no retry cap inside the repository. It does not stop
serving an entry after N attempts, and it will not sleep between attempts --
both are your loop's job. Read `entry.retry_count` from the batch you already
fetched to decide:

```python
if entry.retry_count + 1 >= MAX_RETRIES:
    await outbox.mark_failed(entry.id, str(exc))
else:
    await outbox.increment_retry(entry.id, str(exc))
```

Use this path for failures that a later attempt could plausibly survive: the
broker is down, a connection reset, a timeout. The stored `last_error` is
overwritten on every attempt, so it reflects the most recent failure only --
log the full exception if you need history.

### Step 5: Park poison entries with `mark_failed(outbox_id, error)`

```python
await outbox.mark_failed(entry.id, "unknown event type: WidgetRenamed")
```

`mark_failed` sets `status = 'failed'` and writes `last_error`. It does not
touch `published_at` or `retry_count`. The entry is now out of the pending set
permanently -- no future `get_pending_events` call will return it, and no
repository method moves it back. Requeuing means either an `UPDATE` you write
yourself or calling `add_event` again with the rehydrated event.

Reach for it in two situations:

- **The retry budget is exhausted.** As in step 4.
- **The failure is not going to fix itself.** An `EventTypeNotFoundError` (a
  subclass of `KeyError`) because the event class is no longer registered, or a validation error
  because the stored payload no longer matches the model, will fail identically
  on every attempt. Retrying just burns the budget and keeps a poison entry at
  the head of a FIFO queue.

Failed entries are never deleted by `cleanup_published`, so the count from
`get_stats()` is a durable, alertable signal that something needs a human.

### Idempotency and at-least-once delivery semantics

The outbox guarantees that a committed event is published *at least* once. It
cannot guarantee exactly once, because publishing to the bus and marking the
row are two separate operations against two separate systems, and any crash
between them replays the publish.

#### Why consumers must deduplicate on `event_id`

The stored `event_id` is the one the event was created with, and rehydrating
the payload reproduces it verbatim -- a replayed publish carries the same
`event_id`, not a new one. That makes it the deduplication key.

Every consumer that does something non-idempotent -- incrementing a counter,
charging a card, sending mail -- must record the `event_id` values it has
processed and skip repeats, ideally in the same transaction as the effect
itself. Consumers whose work is naturally idempotent (setting a column to a
value from the payload, upserting a row keyed by `aggregate_id`) need nothing
extra.

Do not use the outbox `id` for this. It is a fresh `uuid4()` per outbox row, so
enqueuing the same event twice produces two different `id` values for one
`event_id`.

#### Why `mark_published` and `mark_failed` remove an entry from the pending set

`get_pending_events` filters on `status = 'pending'`, and exactly two methods
change `status`: `mark_published` (to `published`) and `mark_failed` (to
`failed`). `increment_retry` does not. That is the whole state machine, and it
has a deliberate bias: an entry stays pending until you positively assert it is
done, so every failure mode that is not "you called one of those two methods"
resolves to redelivery rather than loss.

The consequence to keep in mind is that a drain loop that forgets to call
either method on some path will spin on the same batch forever, and the
`pending_count` alert in the monitoring section is how you find out.

### Running the drain as a background loop

Wrap `drain_once` in a task that sleeps when there is nothing to do and never
dies on an unhandled exception:

```python
import asyncio
import logging

logger = logging.getLogger(__name__)


async def drain_forever(outbox, bus, idle_seconds: float = 1.0) -> None:
    while True:
        try:
            processed = await drain_once(outbox, bus)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("outbox drain pass failed")
            await asyncio.sleep(idle_seconds)
        else:
            # Full batch means more work is waiting; poll again immediately.
            if processed == 0:
                await asyncio.sleep(idle_seconds)
```

Points worth copying:

- **Use the engine-backed repository here**, not a connection-backed one. Each
  `mark_published` should commit on its own; a shared connection would hold one
  long transaction across the whole batch.
- **Let `CancelledError` propagate** so the task shuts down when you cancel it.
  Swallowing it in the bare `except` makes the worker unkillable.
- **Sleep only when idle.** A pass that returned a full batch means the backlog
  is longer than `limit`; going straight back around drains it faster.
- **Run exactly one.** Two loops against the same table double-publish, per
  step 2.

Start it with the rest of your application and cancel it on shutdown:

```python
task = asyncio.create_task(drain_forever(outbox, bus))
...
task.cancel()
await asyncio.gather(task, return_exceptions=True)
```

If you would rather not own a long-lived task, `drain_once` is equally happy
called from a scheduler -- see the maintenance job section at the end of this
guide for cadences.
