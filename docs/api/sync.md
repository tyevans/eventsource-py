# Sync API Reference

Reference documentation for `eventsource.sync`, which contains a single public
name: `SyncEventStoreAdapter`. The adapter wraps any async
[`EventStore`](stores.md) and exposes blocking `*_sync` counterparts of its
methods, so event-sourced code can be called from synchronous runtimes such as
Celery tasks, Django management commands, and RQ workers.

Public names covered here:

| Name | Kind | Purpose |
| --- | --- | --- |
| `SyncEventStoreAdapter` | class | Blocking facade over an async `EventStore` |

```python
from eventsource.sync import SyncEventStoreAdapter

# Also available from the package root:
from eventsource import SyncEventStoreAdapter
```

The adapter delegates every call to the wrapped store — it adds no storage,
caching, or retry behaviour of its own. What it adds is event-loop management
(`asyncio.run()` when no loop is running, a shared thread pool when one is) and
a default timeout applied to each operation, overridable per call. Return types,
argument semantics, and exceptions are those of the underlying `EventStore`;
this page documents only how the sync wrapper changes their shape.

## Overview

`SyncEventStoreAdapter` holds a reference to an async `EventStore` and, for each
call, drives that store's coroutine to completion on an event loop it manages
itself. Every method mirrors an `EventStore` method with a `_sync` suffix, the
same positional arguments, and one extra keyword-only `timeout` parameter.
There is no sync bus, sync repository, or sync projection runner — the adapter
covers the event store surface only.

### When to use the sync adapter

Use it when the calling frame cannot be `async def` and you have no way to make
it one: Celery tasks, Django management commands and sync views, RQ workers,
one-off scripts, and pytest tests written without `asyncio` support.

Do not use it inside an already-async call stack. The adapter detects a running
loop via `asyncio.get_running_loop()` and, rather than failing, schedules the
coroutine back onto that loop with `asyncio.run_coroutine_threadsafe()` and
blocks on the resulting future, while emitting a `logging.WARNING` on the
`eventsource.sync.adapter` logger:

```
SyncEventStoreAdapter called from running event loop.
Consider using async EventStore directly for better performance.
```

This path is only safe when the caller is a *different* thread from the one
running the loop — for example, sync code executed via `run_in_executor`. If
you call a `*_sync` method directly from a coroutine on the loop thread, the
call blocks that thread while the work it is waiting on is queued on the same
thread, and the call cannot progress until the timeout expires and
`TimeoutError` is raised. In async code, await the `EventStore` directly.

Two further constraints follow from how the no-loop path works. Each call in
that path runs `asyncio.run()`, which creates and closes a fresh event loop, so
any connection pool or resource the wrapped store binds to a loop is torn down
between calls — stores that cache loop-bound state across calls will not
benefit from pooling here. And because every call blocks the calling thread
until it completes or times out, throughput is bounded by your worker
concurrency, not by the store's async concurrency.

### Import path

The adapter is exported from both its own module and the package root; the two
names are the same object.

```python
from eventsource.sync import SyncEventStoreAdapter
from eventsource import SyncEventStoreAdapter  # equivalent
```

`eventsource.sync.__all__` is `["SyncEventStoreAdapter"]`, and the package root
re-exports the same object, so `eventsource.sync.SyncEventStoreAdapter is
eventsource.SyncEventStoreAdapter`. The implementation lives in
`eventsource.sync.adapter`; importing it from there works but is not part of
the documented surface.

The module has no optional dependencies of its own. It imports only standard
library modules (`asyncio`, `logging`, `threading`, `concurrent.futures`,
`collections.abc`, `datetime`, `typing`, `uuid`) plus
[`DomainEvent`](events.md) and the `AppendResult`, `EventStore`, `EventStream`,
`ReadOptions`, and `StoredEvent` types from
[`eventsource.stores.interface`](stores.md). Importing `eventsource.sync` is
therefore always safe; whatever extras the *wrapped* store needs (for example
`asyncpg` for `PostgreSQLEventStore`) still apply at construction time.

## SyncEventStoreAdapter

```python
class SyncEventStoreAdapter:
    def __init__(self, event_store: EventStore, timeout: float = 30.0) -> None: ...
```

A blocking facade over one async `EventStore`. The instance is stateless apart
from the wrapped store and the default timeout; all event-loop machinery is
created per call (or, for the shared executor, at class level).

`__repr__` renders as `SyncEventStoreAdapter(<StoreClassName>, timeout=<float>)`
— for example `SyncEventStoreAdapter(InMemoryEventStore, timeout=30.0)`.

### Constructor

```python
SyncEventStoreAdapter(event_store: EventStore, timeout: float = 30.0)
```

#### Parameters (`event_store`, `timeout=30.0`)

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `event_store` | `EventStore` | required | The async store to wrap. Retained as-is and exposed via `wrapped_store`. |
| `timeout` | `float` | `30.0` | Default timeout in seconds applied to every operation, overridable per call. |

Both parameters are positional-or-keyword. The constructor does no I/O and does
not touch an event loop: it validates the type, stores the two values, and
returns. Constructing an adapter never connects to the backing store — any
connection setup remains the wrapped store's responsibility and happens on the
first call that reaches it.

`timeout` is not validated. Passing `0` or a negative value produces an
immediately-expiring deadline rather than an error, and `float("inf")` disables
the timeout entirely.

```python
from eventsource.stores import InMemoryEventStore
from eventsource.sync import SyncEventStoreAdapter

sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
```

#### Raises: `TypeError` for non-`EventStore` arguments

Before storing anything, the constructor runs `isinstance(event_store, EventStore)`
and raises the builtin `TypeError` if the check fails:

```python
SyncEventStoreAdapter("not a store")
# TypeError: event_store must be an EventStore instance, got str
```

The message is
`f"event_store must be an EventStore instance, got {type(event_store).__name__}"`,
so the trailing word is the offending object's class name (`str`, `dict`,
`Mock`, …). This is the constructor's only validation — `timeout` is accepted
unchecked.

`EventStore` (`eventsource.stores.interface.EventStore`) is an ABC, not a
`@runtime_checkable` Protocol, so the check is nominal rather than structural.
An object that implements every `EventStore` method but does not inherit from
it is still rejected; so is an unwrapped `unittest.mock.Mock`. Two ways to
satisfy the check:

- subclass `EventStore` (the normal route for custom backends and test doubles), or
- register an existing class as a virtual subclass with `EventStore.register(MyStore)`,
  which makes `isinstance` succeed without inheritance.

For tests, prefer `InMemoryEventStore` — it is a real `EventStore` subclass and
needs no external services:

```python
from eventsource.stores import InMemoryEventStore
from eventsource.sync import SyncEventStoreAdapter

sync_store = SyncEventStoreAdapter(InMemoryEventStore())
```

Because the check happens at construction, this failure surfaces at wiring time
(module import, Celery app setup) rather than on the first store call.

### Event loop handling

Every `*_sync` method builds a coroutine from the wrapped store and hands it to
the private `_run_sync` helper, which resolves the default-vs-override timeout
and then picks one of two execution paths based on `asyncio.get_running_loop()`.

> The class docstring describes three scenarios, including a
> `loop.run_until_complete()` path for a loop that exists but is not running.
> That path is not implemented; the two paths below are the actual behaviour.

#### No running loop -> `asyncio.run()` with `asyncio.wait_for`

`asyncio.get_running_loop()` raises `RuntimeError`, which the adapter swallows,
and the call becomes:

```python
asyncio.run(asyncio.wait_for(coro, timeout=effective_timeout))
```

This is the ordinary path for Celery tasks, management commands, and scripts.
`asyncio.run()` creates a fresh event loop for the call and closes it on the way
out, so no loop-bound state survives between calls. `asyncio.wait_for` enforces
the deadline *inside* that loop, which means a timeout cancels the coroutine
rather than abandoning it.

#### Running loop -> `run_coroutine_threadsafe()`

When a loop is already running, the adapter logs the warning shown under
[When to use the sync adapter](#when-to-use-the-sync-adapter) and then submits
the coroutine to the caller's own loop:

```python
future = asyncio.run_coroutine_threadsafe(coro, loop)
return future.result(timeout=effective_timeout)
```

Note what this does **not** do, despite the class docstring: it does not run the
work on the adapter's `ThreadPoolExecutor`. The pool exists
(`_get_executor()`, `max_workers=4`, `thread_name_prefix="sync_adapter"`) and is
reachable through [`shutdown_executor()`](#shutdown_executor--releasing-the-shared-threadpoolexecutor-at-application-shutdown),
but no code path in `_run_sync` submits to it — the coroutine runs on the
existing loop and the *calling* thread blocks on the concurrent future.

The consequence is the deadlock hazard described earlier: this path only makes
progress when the caller is on a different thread from the loop. On expiry the
adapter calls `future.cancel()` before raising, so the scheduled work is
cancelled rather than left running.

#### Timeout semantics and the keyword-only `timeout` override

Every method accepts a keyword-only `timeout: float | None = None`. `None`
means "use the adapter default"; any other value applies to that call only and
does not mutate the `timeout` property.

```python
# Adapter default of 30s for most work…
sync_store.get_stream_version_sync(order_id, "Order")

# …but allow a long replay to run for five minutes.
events = sync_store.read_all_sync(timeout=300.0)
```

The deadline covers the whole operation, including connection acquisition inside
the wrapped store — it is not a per-network-round-trip timeout. On expiry both
paths raise the builtin `TimeoutError`, with path-specific messages:

- no running loop: `Sync operation timed out after 30.0s`
- running loop: `Sync operation timed out after 30.0s (called from running event loop)`

The `asyncio.TimeoutError` raised by `wait_for` is an alias of the builtin
`TimeoutError` on Python 3.11+, so `except TimeoutError:` catches both variants.
A timeout says nothing about whether a write landed: for
`append_events_sync`, re-read the stream version before retrying.

#### Thread safety

The adapter is safe to call concurrently from multiple threads. It holds no
mutable per-call state — `_run_sync` is re-entrant, and each call gets its own
loop (no-loop path) or its own future (running-loop path). The one piece of
shared mutable state is the class-level `_executor`, guarded by
`_executor_lock`, so `_get_executor()` and `shutdown_executor()` are safe under
concurrency.

Two caveats sit below the adapter. First, thread safety of the *wrapped* store
is its own concern: a store holding a connection pool bound to one loop may not
tolerate being driven from many short-lived loops at once, so consult that
backend's documentation. Second, a single adapter instance may safely be shared
as a module-level singleton across Celery worker threads — this is the intended
usage — but forked worker processes should construct their own store rather than
inheriting one across the fork.
