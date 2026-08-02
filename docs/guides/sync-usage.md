# Using the event store from synchronous code

This guide shows you how to call an event store from code that cannot `await` — Celery tasks, Django management commands and synchronous views, RQ workers, notebooks, and one-off scripts.

`eventsource` is async-first: every store port method is a coroutine. When your calling code is synchronous, wrap the store in `SyncEventStoreAdapter`, which exposes a synchronous method with the *same name* as each async port method and drives the coroutine to completion for you.

```python
from uuid import UUID

from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.domain import StreamId
from eventsource.adapters.sync import SyncEventStoreAdapter

async_store = PostgreSQLEventStore(engine)
sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)

stream = StreamId(aggregate_id=UUID(order_id), category="Order")
envelopes = sync_store.read_stream(stream)
```

The adapter is thread-safe, has a default per-operation timeout you can override on any call, and picks its execution strategy based on whether an event loop is already running in the calling thread.

Two things to know before you start:

- The adapter is for code that is genuinely synchronous. If you are already inside an async framework, use the async store directly — calling the adapter from a running event loop logs a warning and takes a slower path (see [Warning: do not call the adapter from inside a running event loop](#warning-do-not-call-the-adapter-from-inside-a-running-event-loop)).
- Only the event store has a sync adapter. Buses, projections, and subscriptions remain async-only.

## When to use SyncEventStoreAdapter (and when not to)

Reach for the adapter when **the calling thread has no event loop of its own** and you cannot introduce one. That is the situation in:

- Celery tasks running under the default prefork/threads pool.
- Django management commands, synchronous views, and signal receivers.
- RQ, Huey, and similar fork-per-job workers.
- Notebooks, REPL sessions, and one-off scripts where `asyncio.run()` per call is acceptable overhead.
- Existing synchronous service code you are incrementally migrating.

In these contexts each call takes the fast path: `asyncio.run(asyncio.wait_for(coro, timeout))`. A fresh loop is created, the coroutine runs, and the loop is torn down.

### Do not use it when

**You are already in async code.** Inside FastAPI/Starlette async endpoints, `asyncio.run()`-driven scripts, or any coroutine, `await` the store directly. The adapter detects the running loop and raises `RuntimeError` — there is no fallback, because every scheme for running the work on the caller's own loop deadlocks it. See [Warning: do not call the adapter from inside a running event loop](#warning-do-not-call-the-adapter-from-inside-a-running-event-loop).

**You are bridging from async to sync-only third-party code.** Use your framework's bridge (`asgiref.sync.sync_to_async` in Django, `anyio.to_thread.run_sync`, `loop.run_in_executor`) so the adapter runs on a worker thread with no loop of its own — then it takes the fast path again.

**You need a component other than the event store.** `SyncEventStoreAdapter` is the only sync wrapper in the library. `EventBus`, `Projection`, `SubscriptionManager`, snapshot stores, and the checkpoint/DLQ/outbox repositories have no sync surface. If a sync job needs to publish events or drive a projection, either restructure that work as an async entrypoint or wrap your own coroutine in `asyncio.run()`.

**You need `AggregateRepository`.** It takes an async store and its methods are coroutines, so it cannot be driven through the adapter. Sync callers work with the port-level API — `append`, `read_stream`, and friends — and rehydrate aggregates themselves, or move aggregate work into an async entrypoint.

**Per-call loop setup is on your hot path.** Every fast-path call builds and tears down an event loop, and with `PostgreSQLEventStore` that generally means the connection pool cannot be reused across calls the way it is in a long-lived async process. For high-throughput work, batch operations into one coroutine and run that coroutine once, rather than making many sync calls in a loop.

### A note on `read_all`

The async `read_all` is an async iterator; the sync version cannot be. `sync_store.read_all(...)` drains it into a `list[EventEnvelope]` before returning, so bound it with `FeedReadOptions.limit` and paginate rather than reading an entire store into memory. See [`read_all`](#read_all--current_position).

## Wrapping an async event store

Construct your async store as usual, then hand it to `SyncEventStoreAdapter`. The adapter does not open connections or start threads of its own at construction time — it only holds the store and a default timeout.

```python
from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.adapters.sync import SyncEventStoreAdapter

async_store = PostgreSQLEventStore(engine)
sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
```

Build the adapter once at module or application scope and reuse it. It is thread-safe, so a single instance can be shared across Celery worker threads, Django request threads, or any other pool.

For tests and local development, wrap `InMemoryEventStore` the same way:

```python
from eventsource.adapters.memory import InMemoryEventStore

sync_store = SyncEventStoreAdapter(InMemoryEventStore())
```

### Constructor arguments: `store` and `timeout`

`SyncEventStoreAdapter(store, timeout=30.0)` takes two arguments:

- **`store`** — the async, port-shaped event store to wrap (typed as `FullEventStore`). Required and positional; it is exposed afterwards as the read-only `wrapped_store` property.
- **`timeout`** — the default number of seconds any single operation may take, defaulting to `30.0`. Every sync method also accepts a keyword-only `timeout=` that overrides this for one call. The value is exposed as the read-only `timeout` property.

```python
adapter = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)

adapter.wrapped_store  # the InMemoryEventStore instance
adapter.timeout        # 5.0
repr(adapter)          # 'SyncEventStoreAdapter(InMemoryEventStore, timeout=5.0)'
```

Pick the default to match your slowest routine operation, and reserve per-call overrides for the outliers — a bulk `read_all`, say. See [Controlling timeouts](#controlling-timeouts).

Neither property has a setter. To change the timeout, construct another adapter; wrapping the same store twice is cheap and safe.

The constructor does not validate `store` at runtime — it is a structural (`Protocol`-shaped) port, not an ABC, so any object with the right methods is accepted. A backend that is missing a method surfaces as an `AttributeError` on first call rather than a `TypeError` at construction, so exercise a new backend against the conformance suites (see [Validate a custom backend](validate-custom-backend.md)) before wiring it into a sync worker.

## The sync method surface

The adapter exposes one method per store port operation, all sharing the async method's name (no `_sync` suffix), taking the same positional arguments as the coroutine it wraps, and adding a keyword-only `timeout: float | None = None` at the end:

| Sync method | Wraps | Returns |
| --- | --- | --- |
| `append(stream, events, expected, *, timeout=None)` | `append` | `AppendResult` |
| `read_stream(stream, options=None, *, timeout=None)` | `read_stream` | `list[EventEnvelope]` |
| `get_stream_version(stream, *, timeout=None)` | `get_stream_version` | `int` |
| `event_exists(event_id, *, timeout=None)` | `event_exists` | `bool` |
| `read_all(from_position=None, options=None, *, timeout=None)` | `read_all` | `list[EventEnvelope]` |
| `read_category(category, options=None, *, timeout=None)` | `read_category` | `list[EventEnvelope]` |
| `current_position(*, timeout=None)` | `current_position` | `Position \| None` |

Every one of them routes through the same private `_run_sync` helper, so they share identical behaviour in three respects:

- **Timeouts.** The effective timeout is the per-call `timeout=` if you pass one, otherwise the adapter's default. Exceeding it raises `TimeoutError`. See [Controlling timeouts](#controlling-timeouts).
- **Exceptions.** Anything the coroutine raises propagates unchanged to the sync caller — `OptimisticLockError` from a version conflict, `EventStoreError` from the backend, `ValidationError` from pydantic. There is no wrapping or swallowing, so `try`/`except` in sync code looks exactly like it would in async code.
- **Execution strategy.** No running loop in the calling thread is the only supported case (`asyncio.run` per call); a running loop raises `RuntimeError`. See [How the adapter picks an execution strategy](#how-the-adapter-picks-an-execution-strategy).

`read_stream`, `read_all`, and `read_category` wrap async iterators, so the sync adapter drains each into a plain `list[EventEnvelope]` before returning — nothing is converted to dicts or simplified on the way out:

```python
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion

stream = StreamId(aggregate_id=UUID(order_id), category="Order")
envelopes = sync_store.read_stream(stream)
[e.event for e in envelopes]          # list[DomainEvent], chronological
[e.stream_version for e in envelopes] # per-envelope version

result = sync_store.append(
    stream,
    [OrderShipped(...)],
    ExpectedVersion.exact(len(envelopes)),
)
result.new_version   # int
result.position       # Position | None
```

### What is not on the sync surface

The adapter mirrors the whole `FullEventStore` port surface — there is nothing left over on the async store's public API. What it does *not* cover is anything that is not an event store method: snapshot stores, buses, projections, and `AggregateRepository` have no sync wrappers at all — see [When to use SyncEventStoreAdapter (and when not to)](#when-to-use-synceventstoreadapter-and-when-not-to).

If you need the async surface directly — to build a coroutine that batches several operations into one `asyncio.run()` — reach for `sync_store.wrapped_store`, which hands back the store you passed in.

### `append` (optimistic locking with `ExpectedVersion`)

`append` writes events to one stream and enforces optimistic concurrency using an `ExpectedVersion`:

```python
result = sync_store.append(
    stream,                         # StreamId
    [OrderShipped(...)],            # Sequence[DomainEvent]
    ExpectedVersion.exact(current_version),
    timeout=None,                   # keyword-only override
)
```

The return value is an `AppendResult` with `stream` (the `StreamId` written to), `new_version`, and `position` (`None` for feedless stores).

#### Read the version, then write it back

The normal shape is read-then-write:

```python
version = sync_store.get_stream_version(stream)

result = sync_store.append(
    stream,
    [OrderShipped(aggregate_id=order_id, ...)],
    ExpectedVersion.exact(version),
)
# result.new_version == version + 1
```

If you do not need the events themselves — you are appending a fact that does not depend on prior state — use `get_stream_version(stream)` instead and skip materializing the stream.

For a brand-new aggregate the current version is `0`, so a first append passes `ExpectedVersion.exact(0)` (equivalently `ExpectedVersion.no_stream()`) and comes back with `new_version == 1` (or `new_version == len(events)` when you append several at once).

#### The `ExpectedVersion` sentinels

`ExpectedVersion` (`eventsource.ports.ExpectedVersion`) is a small value object with four classmethod constructors instead of magic integers:

| Constructor | Meaning |
| --- | --- |
| `ExpectedVersion.any_()` | Skip the version check entirely — no optimistic locking. |
| `ExpectedVersion.no_stream()` | The stream must not exist yet (zero events). |
| `ExpectedVersion.stream_exists()` | The stream must already have at least one event; any version is accepted. |
| `ExpectedVersion.exact(n)` | The stream must currently have exactly `n` events. |

```python
from eventsource.ports import ExpectedVersion

sync_store.append(stream, [OrderCreated(...)], ExpectedVersion.no_stream())
```

Reserve `ExpectedVersion.any_()` for append-only streams where concurrent writers genuinely cannot conflict; it disables the protection this parameter exists to provide.

#### Handling `OptimisticLockError`

When the stream's actual version does not match the expected version, the store raises `OptimisticLockError` and the adapter propagates it unchanged to your synchronous caller. The exception carries the details you need to decide what to do:

```python
from eventsource.domain.exceptions import OptimisticLockError

try:
    sync_store.append(stream, [event], ExpectedVersion.exact(3))
except OptimisticLockError as exc:
    exc.aggregate_id      # UUID of the contended aggregate
    exc.expected_version  # 3 — what you passed
    exc.actual_version    # what the store found
```

In a sync worker the fix is a bounded retry that re-reads the stream each time, because the events you want to append may depend on state another writer just added:

```python
from eventsource.domain.exceptions import OptimisticLockError

def ship_order(order_id, max_attempts: int = 3):
    stream_id = StreamId(aggregate_id=order_id, category="Order")
    for attempt in range(max_attempts):
        envelopes = sync_store.read_stream(stream_id)
        order = Order.from_events([e.event for e in envelopes])  # your own rehydration
        if order.shipped:
            return                                                # someone else got there first
        event = OrderShipped(aggregate_id=order_id, ...)
        try:
            return sync_store.append(
                stream_id, [event], ExpectedVersion.exact(len(envelopes))
            )
        except OptimisticLockError:
            if attempt == max_attempts - 1:
                raise
```

Do not retry with the same expected version, and do not retry by bumping the version blindly — both defeat the check. Re-read, re-decide, re-append. On Celery you can also let the task's own `autoretry_for=(OptimisticLockError,)` handle it, which spreads retries out under contention instead of hammering in a tight loop.

Concurrency here is real, not theoretical: the adapter is safe to call from many threads at once, and concurrent `append` calls against the same stream with the same expected version result in exactly one winner and `OptimisticLockError` for every loser.

#### Other exceptions

Everything else propagates unchanged too: `EventStoreError` (and its backend-specific subclasses) if the write fails, pydantic's `ValidationError` if an event is malformed, and `TimeoutError` if the operation exceeds the effective timeout — the per-call `timeout=` if given, otherwise the adapter's default. A `TimeoutError` on an append is ambiguous: the write may or may not have committed. Re-read the stream version before retrying rather than assuming it failed.

### `read_stream` / `get_stream_version`

These are the two read paths for a single stream. Use `read_stream` when you need the events themselves; use `get_stream_version` when all you need is the number to pass to `ExpectedVersion.exact()`.

```python
from uuid import UUID

from eventsource.domain import StreamId

stream = StreamId(aggregate_id=UUID(order_id), category="Order")
envelopes = sync_store.read_stream(stream)
version = sync_store.get_stream_version(stream)
```

#### `read_stream`

```python
envelopes = sync_store.read_stream(
    stream,               # StreamId, required
    options=None,          # StreamReadOptions | None — direction, version range, limit
    timeout=None,          # keyword-only override
)
```

It returns a `list[EventEnvelope]`, each carrying `event`, `stream_id`, `stream_version`, `position`, and `stored_at`:

```python
envelopes[0].event             # DomainEvent
envelopes[0].stream_version    # int
envelopes[-1].event            # latest event, if any
```

A stream that has never been written is not an error — you get an empty list. Rehydration code can therefore treat "no events" as "aggregate does not exist yet" without a separate existence check.

The typical sync worker shape is read, rebuild, decide, append:

```python
envelopes = sync_store.read_stream(stream)
if not envelopes:
    raise LookupError(f"no such order: {order_id}")

order = Order.from_events([e.event for e in envelopes])   # your own rehydration
...
sync_store.append(stream, [new_event], ExpectedVersion.exact(len(envelopes)))
```

**Bound the read with `StreamReadOptions` when you already have part of the history.** `StreamReadOptions(from_version=n)` returns only events after version `n`, which is how you top up an aggregate you rehydrated from a snapshot or a cached read model:

```python
from eventsource.ports import StreamReadOptions

snapshot = load_snapshot(order_id)              # your own snapshot cache
tail = sync_store.read_stream(
    stream, StreamReadOptions(from_version=snapshot.version)
)
order = Order.from_snapshot(snapshot)
order.load_from_history([e.event for e in tail])
```

`StreamReadOptions` also accepts `direction` (`ReadDirection.FORWARD`/`BACKWARD`), `to_version`, and `limit` for narrower reads.

> **Do not reuse a filtered read's length as the next expected version.** When you pass `from_version`, `to_version`, or `limit`, the number of envelopes returned describes the filtered result, not the stream's true current version. For a write after a partial read, call `get_stream_version` and use that.

#### `get_stream_version`

```python
version = sync_store.get_stream_version(stream, timeout=None)
```

The argument is a single `StreamId`, and the return is a plain `int` — the current version of the stream, or `0` if no events exist for it. After appending three events to a fresh stream, it returns `3`.

Prefer it over `len(sync_store.read_stream(stream))` whenever you do not need the event payloads — the SQL backends resolve it with a direct `MAX(version)`-style query rather than deserializing every event.

Good uses:

- Appending a fact that does not depend on current state: read the version, append with `ExpectedVersion.exact(version)`.
- Cheap existence checks: `get_stream_version(stream) == 0` means the stream is empty. (For a *specific event*, use `event_exists` instead.)
- Progress and monitoring: reporting how far a long-lived aggregate has advanced without pulling its history.

It is not a substitute for reading when your next write depends on prior state. If a decision needs the aggregate's state — has this order already shipped? — read the events, rehydrate, and decide; a version number alone cannot tell you.

#### Shared behaviour

Both methods go through `_run_sync` like every other adapter method: the effective timeout is the per-call `timeout=` if given and the adapter default otherwise, exceeding it raises `TimeoutError`, and any store exception (`EventStoreError` and its subclasses, deserialization failures) propagates unchanged. Neither raises for a missing stream.

Both are safe to call concurrently from many threads against the same adapter — reads issued while another thread is appending see a consistent stream, not a partial one.

Unbounded reads are the thing to watch. `read_stream` without a `limit` materializes the entire filtered history into memory, so for streams with very long histories either snapshot and use `from_version`, or bound the read with `StreamReadOptions.limit`. If you need to page across *all* streams rather than one, that is `read_all` with `FeedReadOptions.limit`.

### `read_all` / `current_position`

`read_all` reads the store's global, ordered feed rather than a single stream; `current_position` reports the feed's current position without materializing any events. Both are `None`/empty for a feedless store.

```python
from eventsource.ports import FeedReadOptions

position = sync_store.current_position()
page = sync_store.read_all(position, FeedReadOptions(limit=500))
```

`read_all` drains the async iterator into a `list[EventEnvelope]`, so page through the feed with `from_position` (exclusive) plus `FeedReadOptions.limit` rather than calling it unbounded against a store with real history.

### `read_category`

`read_category` reads every stream in one category (aggregate type) rather than one stream or the whole feed — the sync counterpart to a fan-out read across all `Order` streams, for example:

```python
from eventsource.ports import CategoryReadOptions

envelopes = sync_store.read_category("Order", CategoryReadOptions(limit=500))
```

Like `read_all`, it drains the async iterator into a list, so bound it with `CategoryReadOptions.limit` and paginate for categories with a lot of history.

### `event_exists`

```python
sync_store.event_exists(event_id)   # bool
```

Cheap existence check for one specific event id, distinct from `get_stream_version(stream) == 0` which answers "does this stream have any events at all."

## How the adapter picks an execution strategy

Every call goes through `_run_sync`, which checks `asyncio.get_running_loop()` first:

- **No running loop (the common case in Celery/Django/RQ/scripts).** `asyncio.run(asyncio.wait_for(coro, timeout))` — a fresh loop per call, torn down afterward.
- **A running loop on the calling thread.** The coroutine is closed and `RuntimeError` is raised immediately. Nothing is scheduled and nothing blocks.

### Warning: do not call the adapter from inside a running event loop

If your calling thread already has a running loop — an async view, a coroutine, anything under `asyncio.run()` already — calling a sync adapter method raises:

```
SyncEventStoreAdapter was called from a thread with a running event loop.
Blocking that loop on its own work would deadlock. Await the async EventStore
directly, or run this call in a worker thread (e.g. await asyncio.to_thread(...)).
```

Earlier versions accepted the call, scheduled the coroutine onto that same loop, and blocked the loop's only thread waiting for it — so the loop could never run the work, and the call hung until the timeout expired. Prefer awaiting the async store directly; if you must reach the sync API, put it on a worker thread (`await asyncio.to_thread(...)`, `asgiref.sync.sync_to_async`, `anyio.to_thread.run_sync`), where no loop is running and the ordinary path applies.

## Controlling timeouts

Every sync method accepts `timeout: float | None = None` as its last, keyword-only argument. Passing `None` (the default) uses the adapter's own `timeout` (set at construction, default `30.0`). Passing a number overrides it for that one call:

```python
sync_store.read_all(options=FeedReadOptions(limit=10_000), timeout=120.0)
```

Exceeding the effective timeout raises `TimeoutError`, enforced inside the call's own loop by `asyncio.wait_for` so the coroutine is cancelled rather than abandoned.
