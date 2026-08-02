# Sync API Reference

Reference documentation for `eventsource.adapters.sync`, which contains a single public
name: `SyncEventStoreAdapter`. The adapter wraps any async, port-shaped event
store (typed as `FullEventStore` from `eventsource.ports` — the union of the
`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, and
`CategoryQuery` ports) and exposes blocking counterparts of its methods, so
event-sourced code can be called from synchronous runtimes such as Celery
tasks, Django management commands, and RQ workers.

Public names covered here:

| Name | Kind | Purpose |
| --- | --- | --- |
| `SyncEventStoreAdapter` | class | Blocking facade over an async `FullEventStore` |

```python
from eventsource.adapters.sync import SyncEventStoreAdapter

# Also available from the package root:
from eventsource import SyncEventStoreAdapter
```

The adapter delegates every call to the wrapped store — it adds no storage,
caching, or retry behaviour of its own. What it adds is event-loop management
(`asyncio.run()` when no loop is running, a shared thread pool when one is) and
a default timeout applied to each operation, overridable per call. Return types,
argument semantics, and exceptions are those of the underlying store's ports;
this page documents only how the sync wrapper changes their shape.

## Overview

`SyncEventStoreAdapter` holds a reference to an async, port-shaped event store
(`FullEventStore`) and, for each call, drives that store's coroutine to
completion on an event loop it manages itself. Every method mirrors one of the
store's port methods **under the same name** (there is no `_sync` suffix — the
adapter's methods are named `append`, `read_stream`, `get_stream_version`,
`event_exists`, `read_all`, `read_category`, and `current_position`), taking
the same positional arguments plus one extra keyword-only `timeout` parameter.
Methods that return an async iterator on the underlying port (`read_stream`,
`read_all`, `read_category`) return a plain `list[EventEnvelope]` here — the
adapter collects the iterator before returning. There is no sync bus, sync
repository, or sync projection runner — the adapter covers the event store
surface only.

### When to use the sync adapter

Use it when the calling frame cannot be `async def` and you have no way to make
it one: Celery tasks, Django management commands and sync views, RQ workers,
one-off scripts, and pytest tests written without `asyncio` support.

Do not use it inside an already-async call stack. The adapter detects a running
loop via `asyncio.get_running_loop()` and raises `RuntimeError`:

```
SyncEventStoreAdapter was called from a thread with a running event loop.
Blocking that loop on its own work would deadlock. Await the async EventStore
directly, or run this call in a worker thread (e.g. await asyncio.to_thread(...)).
```

Earlier versions accepted this call, handing the coroutine back to the caller's
own loop and then blocking that loop's thread waiting for it — which the loop
could never satisfy, so the call hung until the timeout expired. There is no
variant of that scheme that works, so the adapter refuses the call instead. In
async code, await the wrapped store's async methods directly; if you must reach
a sync API, put it on a worker thread with `asyncio.to_thread`, where no loop is
running and the ordinary path applies.

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
from eventsource.adapters.sync import SyncEventStoreAdapter
from eventsource import SyncEventStoreAdapter  # equivalent
```

`eventsource.adapters.sync.__all__` is `["SyncEventStoreAdapter"]`, and the package root
re-exports the same object, so `eventsource.adapters.sync.SyncEventStoreAdapter is
eventsource.SyncEventStoreAdapter`. The implementation lives in
`eventsource.adapters.sync.adapter`; importing it from there works but is not part of
the documented surface.

The module has no optional dependencies of its own. It imports only standard
library modules (`asyncio`, `logging`, `threading`, `concurrent.futures`,
`collections.abc`, `typing`, `uuid`) plus [`DomainEvent`](events.md),
[`StreamId`](types.md), and the `AppendResult`, `CategoryReadOptions`,
`EventEnvelope`, `ExpectedVersion`, `FeedReadOptions`, `FullEventStore`,
`Position`, `StreamReadOptions`, and `collect` names from
`eventsource.ports`. Importing `eventsource.adapters.sync` is therefore always safe;
whatever extras the *wrapped* store needs (for example `asyncpg` for
`PostgreSQLEventStore`) still apply at construction time.

## SyncEventStoreAdapter

```python
class SyncEventStoreAdapter:
    def __init__(self, store: FullEventStore, timeout: float = 30.0) -> None: ...
```

A blocking facade over one async, port-shaped event store. The instance is
stateless apart from the wrapped store and the default timeout; all event-loop
machinery is created per call.

`__repr__` renders as `SyncEventStoreAdapter(<StoreClassName>, timeout=<float>)`
— for example `SyncEventStoreAdapter(InMemoryEventStore, timeout=30.0)`.

### Constructor

```python
SyncEventStoreAdapter(store: FullEventStore, timeout: float = 30.0)
```

#### Parameters (`store`, `timeout=30.0`)

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `store` | `FullEventStore` | required | The async, port-shaped store to wrap. Retained as-is and exposed via `wrapped_store`. |
| `timeout` | `float` | `30.0` | Default timeout in seconds applied to every operation, overridable per call. |

Both parameters are positional-or-keyword. The constructor does no I/O and does
not touch an event loop: it stores the two values and returns — nothing else.
Constructing an adapter never connects to the backing store — any connection
setup remains the wrapped store's responsibility and happens on the first call
that reaches it.

`timeout` is not validated. Passing `0` or a negative value produces an
immediately-expiring deadline rather than an error, and `float("inf")` disables
the timeout entirely.

```python
from eventsource.adapters.memory import InMemoryEventStore
from eventsource.adapters.sync import SyncEventStoreAdapter

sync_store = SyncEventStoreAdapter(InMemoryEventStore(), timeout=5.0)
```

#### No type validation of `store`

The constructor does not check its `store` argument at all — there is no
`isinstance` check, no `TypeError`, and (since `FullEventStore` is a plain type
alias / Protocol union, not an ABC to register against) nothing to register a
virtual subclass with. `SyncEventStoreAdapter("not a store")` succeeds at
construction time; the resulting adapter will only fail once a method is
called and the wrapped object turns out not to implement the port methods
being driven (an `AttributeError` from `self._store.append(...)` and similar,
surfaced from inside `_run_sync`). `FullEventStore` is a typing-only contract
— mypy will flag a mismatched argument, but nothing enforces it at runtime.

For tests, prefer `InMemoryEventStore` — it implements every port and needs no
external services:

```python
from eventsource.adapters.memory import InMemoryEventStore
from eventsource.adapters.sync import SyncEventStoreAdapter

sync_store = SyncEventStoreAdapter(InMemoryEventStore())
```

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

#### Running loop -> `RuntimeError`

When a loop is already running on the calling thread, the adapter closes the
coroutine (so it raises no "never awaited" warning) and raises `RuntimeError`.
No work is scheduled and nothing blocks.

There is no correct alternative. Submitting the coroutine to the caller's own
loop and blocking that loop's thread for the result deadlocks by construction:
the loop cannot execute the work it was just handed while its only thread waits
on it. Running it on a second loop would break any loop-bound state the wrapped
store holds. Refusing the call is the only option that fails fast and points at
the fix.

If the sync API is unavoidable in an async caller, move it off the loop thread:

```python
version = await asyncio.to_thread(sync_store.get_stream_version, stream_id)
```

Inside that worker thread no loop is running, so the ordinary `asyncio.run()`
path applies.

#### Timeout semantics and the keyword-only `timeout` override

Every method accepts a keyword-only `timeout: float | None = None`. `None`
means "use the adapter default"; any other value applies to that call only and
does not mutate the `timeout` property.

```python
# Adapter default of 30s for most work…
sync_store.get_stream_version(stream_id)

# …but allow a long replay to run for five minutes.
events = sync_store.read_all(timeout=300.0)
```

The deadline covers the whole operation, including connection acquisition inside
the wrapped store — it is not a per-network-round-trip timeout. On expiry the
adapter raises the builtin `TimeoutError`: `Sync operation timed out after 30.0s`.

The `asyncio.TimeoutError` raised by `wait_for` is an alias of the builtin
`TimeoutError` on Python 3.11+, so `except TimeoutError:` catches both variants.
A timeout says nothing about whether a write landed: for `append`, re-read the
stream version before retrying.

#### Thread safety

The adapter is safe to call concurrently from multiple threads. It holds no
mutable per-call state — `_run_sync` is re-entrant and each call gets its own
event loop. There is no class-level shared state.

Two caveats sit below the adapter. First, thread safety of the *wrapped* store
is its own concern: a store holding a connection pool bound to one loop may not
tolerate being driven from many short-lived loops at once, so consult that
backend's documentation. Second, a single adapter instance may safely be shared
as a module-level singleton across Celery worker threads — this is the intended
usage — but forked worker processes should construct their own store rather than
inheriting one across the fork.

### Releasing the wrapped store: `close()`

```python
def close(self, *, timeout: float | None = None) -> None: ...
```

Synchronously releases resources owned by the wrapped store, delegating to its
async `close()` when it implements
[`SupportsClose`](../architecture.md) and doing nothing when it does not. The
delegated call goes through `_run_sync`, so the same timeout rules apply.

Without this, a sync caller had no way to release a store that owns a
connection — a `SQLiteEventStore` holding an open `aiosqlite` connection, or a
`PostgreSQLEventStore` constructed with `owns_engine=True` — and the process
could hang at interpreter exit waiting on it. The ownership contract is the
port's: `close()` never tears down a resource the caller injected and still
owns.

The adapter is also a context manager, which closes on exit:

```python
with SyncEventStoreAdapter(SQLiteEventStore(conn)) as sync_store:
    version = sync_store.get_stream_version(stream_id)
# the store's connection is released here
```

`close()` is idempotent, as the port requires — calling it twice is safe.
