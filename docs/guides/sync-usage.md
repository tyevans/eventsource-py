# Using the event store from synchronous code

This guide shows you how to call an `EventStore` from code that cannot `await` — Celery tasks, Django management commands and synchronous views, RQ workers, notebooks, and one-off scripts.

`eventsource` is async-first: every `EventStore` method is a coroutine. When your calling code is synchronous, wrap the store in `SyncEventStoreAdapter`, which exposes a `*_sync` method for each store operation and drives the coroutine to completion for you.

```python
from uuid import UUID

from eventsource.stores import PostgreSQLEventStore
from eventsource.sync import SyncEventStoreAdapter

async_store = PostgreSQLEventStore(database_url)
sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)

stream = sync_store.get_events_sync(UUID(order_id), "Order")
```

The adapter is thread-safe, has a default per-operation timeout you can override on any call, and picks its execution strategy based on whether an event loop is already running in the calling thread.

Two things to know before you start:

- The adapter is for code that is genuinely synchronous. If you are already inside an async framework, use the async `EventStore` directly — calling the adapter from a running event loop logs a warning and takes a slower path (see [Warning: do not call the adapter from inside a running event loop](#warning-do-not-call-the-adapter-from-inside-a-running-event-loop)).
- Only `EventStore` has a sync adapter. Buses, projections, and subscriptions remain async-only.

## When to use SyncEventStoreAdapter (and when not to)

Reach for the adapter when **the calling thread has no event loop of its own** and you cannot introduce one. That is the situation in:

- Celery tasks running under the default prefork/threads pool.
- Django management commands, synchronous views, and signal receivers.
- RQ, Huey, and similar fork-per-job workers.
- Notebooks, REPL sessions, and one-off scripts where `asyncio.run()` per call is acceptable overhead.
- Existing synchronous service code you are incrementally migrating.

In these contexts each `*_sync` call takes the fast path: `asyncio.run(asyncio.wait_for(coro, timeout))`. A fresh loop is created, the coroutine runs, and the loop is torn down.

### Do not use it when

**You are already in async code.** Inside FastAPI/Starlette async endpoints, `asyncio.run()`-driven scripts, or any coroutine, `await` the `EventStore` directly. The adapter detects the running loop, emits a warning through the `eventsource.sync.adapter` logger, and falls back to `run_coroutine_threadsafe` plus a shared thread pool — more overhead, and a deadlock risk if the calling thread *is* the loop thread. See [Warning: do not call the adapter from inside a running event loop](#warning-do-not-call-the-adapter-from-inside-a-running-event-loop).

**You are bridging from async to sync-only third-party code.** Use your framework's bridge (`asgiref.sync.sync_to_async` in Django, `anyio.to_thread.run_sync`, `loop.run_in_executor`) so the adapter runs on a worker thread with no loop of its own — then it takes the fast path again.

**You need a component other than `EventStore`.** `SyncEventStoreAdapter` is the only sync wrapper in the library. `EventBus`, `Projection`, `SubscriptionManager`, snapshot stores, and the checkpoint/DLQ/outbox repositories have no sync surface. If a sync job needs to publish events or drive a projection, either restructure that work as an async entrypoint or wrap your own coroutine in `asyncio.run()`.

**You need `AggregateRepository`.** It takes an async `EventStore` and its methods are coroutines, so it cannot be driven through the adapter. Sync callers work with the event-level API — `append_events_sync`, `get_events_sync`, and friends — and rehydrate aggregates themselves, or move aggregate work into an async entrypoint.

**Per-call loop setup is on your hot path.** Every fast-path call builds and tears down an event loop, and with `PostgreSQLEventStore` that generally means the connection pool cannot be reused across calls the way it is in a long-lived async process. For high-throughput work, batch operations into one coroutine and run that coroutine once, rather than making many `*_sync` calls in a loop.

### A note on `read_all_sync`

The async `read_all` is an async iterator; the sync version cannot be. `read_all_sync` materializes the whole result into a `list[StoredEvent]` before returning, so bound it with `ReadOptions.limit` and paginate rather than reading an entire store into memory. See [`read_all_sync`](#read_all_sync--materializes-the-async-iterator-paginate-with-readoptionslimit).

## Wrapping an async EventStore

Construct your async store as usual, then hand it to `SyncEventStoreAdapter`. The adapter does not open connections or start threads of its own at construction time — it only holds the store and a default timeout.

```python
from eventsource.stores import PostgreSQLEventStore
from eventsource.sync import SyncEventStoreAdapter

async_store = PostgreSQLEventStore(database_url)
sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
```

Build the adapter once at module or application scope and reuse it. It is thread-safe, so a single instance can be shared across Celery worker threads, Django request threads, or any other pool.

For tests and local development, wrap `InMemoryEventStore` the same way:

```python
from eventsource.stores import InMemoryEventStore

sync_store = SyncEventStoreAdapter(InMemoryEventStore())
```

### Constructor arguments: `event_store` and `timeout`

`SyncEventStoreAdapter(event_store, timeout=30.0)` takes exactly two arguments:

- **`event_store`** — the async `EventStore` to wrap. Required and positional; it is exposed afterwards as the read-only `wrapped_store` property.
- **`timeout`** — the default number of seconds any single `*_sync` operation may take, defaulting to `30.0`. Every sync method also accepts a keyword-only `timeout=` that overrides this for one call. The value is exposed as the read-only `timeout` property.

```python
adapter = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)

adapter.wrapped_store  # the InMemoryEventStore instance
adapter.timeout        # 5.0
repr(adapter)          # 'SyncEventStoreAdapter(InMemoryEventStore, timeout=5.0)'
```

Pick the default to match your slowest routine operation, and reserve per-call overrides for the outliers — a bulk `read_all_sync`, say. See [Controlling timeouts](#controlling-timeouts).

Neither property has a setter. To change the timeout, construct another adapter; wrapping the same store twice is cheap and safe.

### The `TypeError` you get when the wrapped object isn't an `EventStore`

The constructor validates its argument immediately with `isinstance(event_store, EventStore)` and raises `TypeError` if the check fails:

```python
>>> SyncEventStoreAdapter("not a store")
TypeError: event_store must be an EventStore instance, got str
```

The message always names the offending type, which makes the usual causes easy to spot:

- **A connection string, engine, or DSN passed instead of a store.** `SyncEventStoreAdapter(database_url)` fails; construct `PostgreSQLEventStore(database_url)` first and wrap that.
- **A class instead of an instance.** `SyncEventStoreAdapter(PostgreSQLEventStore)` reports `got ABCMeta` (the metaclass of the store's ABC) — add the call parentheses.
- **An already-wrapped adapter.** `SyncEventStoreAdapter(sync_store)` reports `got SyncEventStoreAdapter`; the adapter is not itself an `EventStore` and cannot be nested.
- **A custom backend that does not subclass `EventStore`.** `eventsource.stores.interface.EventStore` is an ABC, not a `Protocol`, so structural compatibility is not enough — a class that merely implements all the right methods still fails the check. Inherit from `EventStore` (or register it as a virtual subclass) and the adapter will accept it.

Because the check happens in `__init__`, the failure surfaces at wiring time rather than on the first call from inside a task. Construct your adapters at import or startup so a misconfiguration fails fast instead of at the first job.

## The sync method surface

The adapter exposes seven methods, one per async `EventStore` operation it covers. Each is the async name with a `_sync` suffix, takes the same positional arguments as the coroutine it wraps, and adds a keyword-only `timeout: float | None = None` at the end:

| Sync method | Wraps | Returns |
| --- | --- | --- |
| `append_events_sync(aggregate_id, aggregate_type, events, expected_version, *, timeout=None)` | `append_events` | `AppendResult` |
| `get_events_sync(aggregate_id, aggregate_type=None, from_version=0, from_timestamp=None, to_timestamp=None, *, timeout=None)` | `get_events` | `EventStream` |
| `get_events_by_type_sync(aggregate_type, tenant_id=None, from_timestamp=None, *, timeout=None)` | `get_events_by_type` | `list[DomainEvent]` |
| `get_stream_version_sync(aggregate_id, aggregate_type, *, timeout=None)` | `get_stream_version` | `int` |
| `event_exists_sync(event_id, *, timeout=None)` | `event_exists` | `bool` |
| `read_all_sync(options=None, *, timeout=None)` | `read_all` | `list[StoredEvent]` |
| `get_global_position_sync(*, timeout=None)` | `get_global_position` | `int` |

Every one of them routes through the same private `_run_sync` helper, so they share identical behaviour in three respects:

- **Timeouts.** The effective timeout is the per-call `timeout=` if you pass one, otherwise the adapter's default. Exceeding it raises `TimeoutError`. See [Controlling timeouts](#controlling-timeouts).
- **Exceptions.** Anything the coroutine raises propagates unchanged to the sync caller — `OptimisticLockError` from a version conflict, `EventStoreError` from the backend, `ValidationError` from pydantic. There is no wrapping or swallowing, so `try`/`except` in sync code looks exactly like it would in async code.
- **Execution strategy.** No running loop in the calling thread is the fast path (`asyncio.run`); a running loop triggers a logged warning and the thread-pool path. See [How the adapter picks an execution strategy](#how-the-adapter-picks-an-execution-strategy).

The return types are the same objects the async API returns — `AppendResult`, `EventStream`, `StoredEvent`, `DomainEvent` — imported from `eventsource.stores`. Nothing is converted to dicts or simplified on the way out:

```python
from uuid import UUID

stream = sync_store.get_events_sync(UUID(order_id), "Order")
stream.version        # int, current stream version
stream.events         # list[DomainEvent]
stream.is_empty       # True when no events matched

result = sync_store.append_events_sync(
    aggregate_id=UUID(order_id),
    aggregate_type="Order",
    events=[OrderShipped(aggregate_id=UUID(order_id), ...)],
    expected_version=stream.version,
)
result.success        # bool
result.new_version    # int
result.global_position
```

### What is not on the sync surface

The adapter does not mirror the whole `EventStore` ABC. Two things are absent:

- **`read_stream`.** The per-stream async iterator has no `_sync` counterpart. For a single aggregate use `get_events_sync`, which returns the full `EventStream`; if you specifically need `StoredEvent` position metadata for one stream, filter a `read_all_sync` result or drive `read_stream` yourself inside a coroutine you run with `asyncio.run()`.
- **Anything that is not an `EventStore` method.** Snapshot stores, buses, projections, and `AggregateRepository` have no sync wrappers at all — see [When to use SyncEventStoreAdapter (and when not to)](#when-to-use-synceventstoreadapter-and-when-not-to).

If you need the async surface directly — to build a coroutine that batches several operations into one `asyncio.run()` — reach for `sync_store.wrapped_store`, which hands back the store you passed in.

### `append_events_sync` (optimistic locking with `expected_version`)

`append_events_sync` writes events to one aggregate stream and enforces optimistic concurrency using `expected_version`:

```python
result = sync_store.append_events_sync(
    aggregate_id=order_id,          # UUID
    aggregate_type="Order",         # str
    events=[OrderShipped(...)],     # Sequence[DomainEvent]
    expected_version=stream.version,
    timeout=None,                   # keyword-only override
)
```

`events` is a `Sequence[DomainEvent]` — the adapter copies it with `list(events)` before handing it to the store, so tuples and other sequences are fine. The return value is an `AppendResult` with `success`, `new_version`, `global_position`, and `conflict`.

#### Read the version, then write it back

`expected_version` is the version you believe the stream is at *before* your append. The normal shape is read-then-write:

```python
stream = sync_store.get_events_sync(order_id, "Order")

result = sync_store.append_events_sync(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[OrderShipped(aggregate_id=order_id, aggregate_type="Order", aggregate_version=stream.version + 1)],
    expected_version=stream.version,
)
# result.new_version == stream.version + 1
```

If you do not need the events themselves — you are appending a fact that does not depend on prior state — use `get_stream_version_sync(order_id, "Order")` instead and skip materializing the stream.

For a brand-new aggregate the current version is `0`, so a first append passes `expected_version=0` and comes back with `new_version == 1` (or `new_version == len(events)` when you append several at once).

#### The special `ExpectedVersion` constants

`expected_version` also accepts the sentinels from `eventsource.stores.interface.ExpectedVersion`:

| Constant | Value | Meaning |
| --- | --- | --- |
| `ExpectedVersion.ANY` | `-1` | Skip the version check entirely — no optimistic locking. |
| `ExpectedVersion.NO_STREAM` | `0` | The stream must not exist yet (zero events for this aggregate type). |
| `ExpectedVersion.STREAM_EXISTS` | `-2` | The stream must already have at least one event; any version is accepted. |

```python
from eventsource.stores.interface import ExpectedVersion

sync_store.append_events_sync(
    order_id, "Order", [OrderCreated(...)], ExpectedVersion.NO_STREAM
)
```

`NO_STREAM` is `0`, so creating a new aggregate with the literal `0` and with the constant are the same call — the constant just says what you mean. Reserve `ExpectedVersion.ANY` for append-only streams where concurrent writers genuinely cannot conflict; it disables the protection this parameter exists to provide.

#### Handling `OptimisticLockError`

When the stream's actual version does not match `expected_version`, the store raises `OptimisticLockError` and the adapter propagates it unchanged to your synchronous caller — nothing is wrapped, and `AppendResult.conflict` is *not* how conflicts reach you on the write path. The exception carries the details you need to decide what to do:

```python
from eventsource.exceptions import OptimisticLockError

try:
    sync_store.append_events_sync(order_id, "Order", [event], expected_version=3)
except OptimisticLockError as exc:
    exc.aggregate_id      # UUID of the contended aggregate
    exc.expected_version  # 3 — what you passed
    exc.actual_version    # what the store found
```

In a sync worker the fix is a bounded retry that re-reads the stream each time, because the events you want to append may depend on state another writer just added:

```python
from eventsource.exceptions import OptimisticLockError

def ship_order(order_id, max_attempts: int = 3):
    for attempt in range(max_attempts):
        stream = sync_store.get_events_sync(order_id, "Order")
        order = Order.from_events(stream.events)      # your own rehydration
        if order.shipped:
            return                                     # someone else got there first
        event = OrderShipped(
            aggregate_id=order_id,
            aggregate_type="Order",
            aggregate_version=stream.version + 1,
        )
        try:
            return sync_store.append_events_sync(
                order_id, "Order", [event], expected_version=stream.version
            )
        except OptimisticLockError:
            if attempt == max_attempts - 1:
                raise
```

Do not retry with the same `expected_version`, and do not retry by bumping the version blindly — both defeat the check. Re-read, re-decide, re-append. On Celery you can also let the task's own `autoretry_for=(OptimisticLockError,)` handle it, which spreads retries out under contention instead of hammering in a tight loop.

Concurrency here is real, not theoretical: the adapter is safe to call from many threads at once, and concurrent `append_events_sync` calls against the same stream with the same `expected_version` result in exactly one winner and `OptimisticLockError` for every loser.

#### Two behaviours worth knowing

**An empty `events` list is a no-op that still succeeds.** `append_events_sync(..., events=[], expected_version=n)` returns `AppendResult(success=True, new_version=n)` without touching the store and — importantly — without performing the version check. It will not raise `OptimisticLockError` even if `expected_version` is wrong, so do not use an empty append as a version probe; use `get_stream_version_sync` for that.

**Appends are idempotent by `event_id`.** An event whose `event_id` the store has already seen is skipped rather than written twice, so a retried task that re-submits the *same* event objects will not duplicate them. Note that constructing a fresh event instance generates a new `event_id`, so this protects redelivery of an already-built event, not recomputation of an equivalent one.

#### Other exceptions

Everything else propagates unchanged too: `EventStoreError` (and its backend-specific subclasses) if the write fails, pydantic's `ValidationError` if an event is malformed, and `TimeoutError` if the append exceeds the effective timeout — the per-call `timeout=` if given, otherwise the adapter's default. A `TimeoutError` on an append is ambiguous: the write may or may not have committed. Re-read the stream version before retrying rather than assuming it failed.

### `get_events_sync` / `get_stream_version_sync`

These are the two read paths for a single aggregate stream. Use `get_events_sync` when you need the events themselves; use `get_stream_version_sync` when all you need is the number to pass as `expected_version`.

```python
from uuid import UUID

stream = sync_store.get_events_sync(UUID(order_id), "Order")
version = sync_store.get_stream_version_sync(UUID(order_id), "Order")
```

#### `get_events_sync`

```python
stream = sync_store.get_events_sync(
    aggregate_id,               # UUID, required
    aggregate_type=None,        # str | None — filter by type
    from_version=0,             # int — skip events at or below this version
    from_timestamp=None,        # datetime | None
    to_timestamp=None,          # datetime | None
    timeout=None,               # keyword-only override
)
```

It returns an `EventStream`, the same dataclass the async `get_events` returns:

```python
stream.aggregate_id     # UUID
stream.aggregate_type   # str
stream.events           # list[DomainEvent], chronological (oldest first)
stream.version          # int
stream.is_empty         # True when events is empty
stream.latest_event     # DomainEvent | None
```

A stream that has never been written is not an error — you get an empty `EventStream` with `version == 0`, `events == []`, and `is_empty is True`. Rehydration code can therefore treat "no events" as "aggregate does not exist yet" without a separate existence check.

The typical sync worker shape is read, rebuild, decide, append:

```python
stream = sync_store.get_events_sync(order_id, "Order")
if stream.is_empty:
    raise LookupError(f"no such order: {order_id}")

order = Order.from_events(stream.events)   # your own rehydration
...
sync_store.append_events_sync(
    order_id, "Order", [new_event], expected_version=stream.version
)
```

**`aggregate_type` is optional but should almost always be passed.** Omitting it returns every event recorded under that `aggregate_id` regardless of type. Aggregate IDs are UUIDs and rarely collide across types, so the unfiltered form works — but the filtered form is what the backends index on, and it keeps the returned `aggregate_type` meaningful rather than resolved from whatever happened to be stored.

**`from_version` skips the events you already have.** `from_version=0` (the default) returns the whole history; `from_version=n` returns only events after version `n`, which is how you top up an aggregate you rehydrated from a snapshot or a cached read model:

```python
snapshot = load_snapshot(order_id)              # your own snapshot cache
tail = sync_store.get_events_sync(
    order_id, "Order", from_version=snapshot.version
)
order = Order.from_snapshot(snapshot)
order.load_from_history(tail.events)
```

The timestamp filters narrow the same query: `from_timestamp` keeps events at or after the given moment, `to_timestamp` keeps events at or before it. Both are most useful for point-in-time reconstruction and, on partitioned Postgres deployments, for partition pruning. Pass timezone-aware `datetime` objects.

> **Do not reuse `stream.version` from a filtered read as `expected_version`.** When you pass `from_version`, `from_timestamp`, or `to_timestamp`, the `version` on the returned stream describes the filtered result, and backends do not agree on what that means — `PostgreSQLEventStore` reports the version of the last event actually returned, while `InMemoryEventStore` reports the *count* of returned events. Either way it is not the stream's true current version. For a write after a partial read, call `get_stream_version_sync` and use that.

#### `get_stream_version_sync`

```python
version = sync_store.get_stream_version_sync(order_id, "Order", timeout=None)
```

Both arguments are required, and the return is a plain `int` — the current version of the stream, or `0` if no events exist for that aggregate and type. After appending three events to a fresh aggregate, it returns `3`.

Prefer it over `get_events_sync(...).version` whenever you do not need the event payloads. `PostgreSQLEventStore` and `SQLiteEventStore` both override it with a `SELECT COALESCE(MAX(version), 0)` query, so it never deserializes a single event; the base-class fallback (used by `InMemoryEventStore` and by custom backends that do not override it) does call `get_events` internally, so on those backends the saving is only in what crosses back into your code.

Good uses:

- Appending a fact that does not depend on current state: read the version, append with it as `expected_version`.
- Cheap existence checks: `get_stream_version_sync(...) == 0` means the stream is empty. (For a *specific event*, use `event_exists_sync` instead.)
- Progress and monitoring: reporting how far a long-lived aggregate has advanced without pulling its history.

It is not a substitute for reading when your next write depends on prior state. If a decision needs the aggregate's state — has this order already shipped? — read the events, rehydrate, and decide; a version number alone cannot tell you.

#### Shared behaviour

Both methods go through `_run_sync` like every other adapter method: the effective timeout is the per-call `timeout=` if given and the adapter default otherwise, exceeding it raises `TimeoutError`, and any store exception (`EventStoreError` and its subclasses, deserialization failures) propagates unchanged. Neither raises for a missing aggregate.

Both are safe to call concurrently from many threads against the same adapter — reads issued while another thread is appending see a consistent stream, not a partial one.

Unbounded reads are the thing to watch. `get_events_sync` materializes the entire filtered history into memory, so for aggregates with very long streams either snapshot and use `from_version`, or bound the read with the timestamp filters. If you need to page across *all* aggregates rather than one, that is `read_all_sync` with `ReadOptions.limit`.
