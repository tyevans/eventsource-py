# ADR-0001: Async-First Design

**Status:** Accepted

**Amended by [0048 - Failure Paths Report Honestly and Retain What They Cannot
Handle](0048-failure-paths-report-and-retain.md)**, scoped to
`SyncEventStoreAdapter`'s running-loop branch. That branch scheduled the
coroutine onto the caller's own loop and then blocked that loop's thread
waiting for it -- a guaranteed self-deadlock -- so it now raises
`RuntimeError` instead. The shared `ThreadPoolExecutor` (`_executor`,
`_get_executor()`, `shutdown_executor()`), which no code path ever dispatched
to, is removed. The async-first decision and the `asyncio.run` /
`asyncio.wait_for` no-loop path recorded below are unchanged. The addendum's
discussion of the running-loop case describes the superseded behavior.

Every I/O-bound interface in `eventsource` is defined as a coroutine function. `EventStore.append_events`, `EventStore.get_events`, `EventStore.read_all` (an async iterator), `EventBus.publish`, the snapshot stores, the checkpoint/outbox/DLQ repositories, `AggregateRepository`, and the projection and subscription runners are all `async def`. There is no parallel synchronous hierarchy behind them: the async definition is the interface, and the PostgreSQL, SQLite, Redis, RabbitMQ, Kafka, and in-memory backends implement it directly on top of asyncpg, aiosqlite, redis-py's asyncio client, aio-pika, and aiokafka.

This ADR records why the library commits to that shape rather than offering sync and async APIs side by side, and what the commitment costs. The short version: an event-sourced system spends nearly all of its wall-clock time waiting on a database, a broker, or another service, and a subscription runner is by nature a set of long-lived concurrent consumers. `asyncio` expresses that workload with one execution model, one cancellation model, and one timeout model, where a thread-per-consumer design would need its own.

The one deliberate exception is `SyncEventStoreAdapter` in `eventsource.sync`, a thin edge adapter that lets Celery tasks, RQ workers, and Django management commands reach an async store without adopting an event loop. It wraps a store and nothing else, and it is not a second API. The addendum below covers what it does, the event-loop cases its `_run_sync` helper handles, and why the library stops there. ADR-0007 records the related removal of the `SyncEventStore` abstract class in 0.2.0.

The sections that follow set out the context that made this choice reasonable, the decision itself, and the consequences the project has since had to live with.

## Context

Event sourcing is an I/O-bound workload almost end to end. Appending events is a round trip to PostgreSQL or SQLite; loading an aggregate is a read of its stream plus, optionally, a snapshot; publishing is a round trip to Redis, RabbitMQ, or Kafka; a projection catch-up is a long sequence of reads interleaved with writes to a read model and checkpoint table. Between those calls the process does very little arithmetic. The scarce resource is not CPU but the ability to have many operations outstanding at once without paying for a thread per operation.

The subscription machinery makes that concrete. A deployment typically runs several long-lived subscriptions side by side, each consuming its own stream, each with its own checkpoint, retry policy, and health state. `subscriptions/lifecycle.py` starts them with `asyncio.create_task` and stops them with `asyncio.gather`; `subscriptions/flow_control.py` bounds in-flight work with an `asyncio.Semaphore`; `subscriptions/retry.py` backs off with `asyncio.sleep`; `subscriptions/shutdown.py` bounds every drain, checkpoint, and close step with `asyncio.wait_for` and treats `asyncio.CancelledError` as the normal path out of a running consumer. That is one coherent set of primitives for concurrency, backpressure, timeout, and cancellation. A synchronous design would need a thread pool, a bounded queue or counting semaphore, per-call socket timeouts, and a cooperative stop flag -- four mechanisms that do not compose as cleanly, because a blocked thread cannot be cancelled.

### Python Async Ecosystem Maturity

The decision would have been harder to defend a few years earlier. By the time this library targeted Python 3.11+, every backend it needed had a first-class asyncio driver: `asyncpg` for PostgreSQL, `aiosqlite` for SQLite, the asyncio client shipped inside `redis-py` 5.x, `aio-pika` for RabbitMQ, and `aiokafka` for Kafka. These are the actual optional extras declared in `pyproject.toml`. SQLAlchemy 2.0, a required dependency, ships async engine and session support rather than leaving it to a third-party fork. Pydantic v2, the other required dependency, is agnostic -- validation and serialization are synchronous and cheap, so they sit comfortably inside coroutines.

The language runtime had also caught up. Python 3.11 brought `asyncio.TaskGroup` and materially faster coroutine dispatch, and `pytest-asyncio` made testing coroutine-based interfaces routine rather than an exercise in bespoke event-loop fixtures. Choosing async no longer meant choosing a driver ecosystem one notch behind the synchronous one.

### Forces at Play

Several pressures pulled in different directions:

- **One interface or two.** Offering both `EventStore` and `SyncEventStore` doubles the surface every backend must implement, every conformance suite must cover, and every reader must navigate. It also invites drift, where a feature lands on one hierarchy and not the other.
- **Async cannot be retrofitted from below.** A synchronous interface can be driven from async code only by pushing it to a thread; an async interface can be driven from sync code by an adapter at the edge. The async-first direction is the one that keeps both callers reachable, which is why the exception the library does make -- `SyncEventStoreAdapter` -- is possible at all.
- **Existing sync deployments are real.** Celery tasks, RQ workers, and Django management commands are where a lot of event-sourced write paths actually live. Refusing them entirely would have been a real adoption cost, which is what the edge adapter answers.
- **Colored functions.** Async is viral: callers of a coroutine must themselves be coroutines. Committing to async means accepting that cost throughout, and accepting that a user who blocks inside a handler stalls the loop for every other subscription sharing it.
- **Long-lived consumers need cancellation.** Graceful shutdown -- drain, checkpoint, close, each under a deadline -- is a first-class requirement for a subscription runner, and it is far easier to express against tasks that can be cancelled than against threads that cannot.

The decision below resolves these in favor of a single async hierarchy, with one narrowly scoped adapter at the boundary.

## Decision

`eventsource` defines every I/O-bound abstraction as a coroutine interface and ships no parallel synchronous hierarchy. `EventStore` and `EventBus` in `stores/interface.py` and `bus/interface.py` are ABCs whose abstract methods are all `async def` -- `append_events`, `get_events`, `get_events_by_type`, `event_exists`, `get_global_position` on the store; `publish` and the subscribe/lifecycle methods on the bus. `EventStore.read_all` is an async iterator, with a base implementation that raises `NotImplementedError` so backends opt in rather than inherit a silently wrong default. `AggregateRepository` follows suit: `load`, `load_or_create`, `save`, `exists`, `get_version`, `get_or_raise`, and `create_snapshot` are all coroutines. The one synchronous entry point in the library is `SyncEventStoreAdapter`, and it wraps a store only.

### Core Choices

- **The async signature is the interface, not a wrapper over a sync core.** Backends implement the coroutine directly against an async driver -- asyncpg, aiosqlite, redis-py asyncio, aio-pika, aiokafka -- rather than delegating to blocking calls in a thread. There is no hidden `run_in_executor` layer to reason about.
- **No `SyncEventStore` abstract class.** A second hierarchy would double what backends implement and what the conformance suites in `testing/conformance.py` must cover. ADR-0007 records the removal of the class that once existed.
- **Only I/O is coloured async; registration stays synchronous.** The line is drawn at the operation that touches a socket or a file. `EventBus.publish` is `async def`, but `subscribe`, `unsubscribe`, `subscribe_all`, `subscribe_to_all_events`, and `unsubscribe_from_all_events` are ordinary methods, because wiring a handler into a registry is in-process bookkeeping. The same applies on the store side: `AggregateRepository.create_new` and the property accessors are sync; `load`, `save`, and friends are not. Making everything a coroutine for uniformity would have forced an event loop on module-level wiring code that has no reason to want one.
- **`asyncio` primitives are the concurrency, backpressure, timeout, and cancellation model.** The subscription layer uses `asyncio.create_task` and `asyncio.gather(..., return_exceptions=True)` for lifecycle (`subscriptions/lifecycle.py`, `subscriptions/manager.py`), `asyncio.Semaphore` for flow control, `asyncio.sleep` for retry backoff, `asyncio.wait_for` and `asyncio.timeout` for deadlines, `asyncio.Event` for drain and shutdown signalling, `asyncio.Lock` for shared mutable state in the registry, retry policy, error handler, and health provider, and `asyncio.CancelledError` as the ordinary exit path of a running consumer. Loop signal handlers (`register_signals` in `subscriptions/shutdown.py`) hang graceful shutdown off the same machinery via `loop.add_signal_handler`.
- **Sync interop is an adapter at the edge, not an API tier.** `SyncEventStoreAdapter` exposes `*_sync` counterparts (`append_events_sync`, `get_events_sync`, `get_events_by_type_sync`, `get_stream_version_sync`, `event_exists_sync`, `read_all_sync`, `get_global_position_sync`) and nothing beyond the store. No sync repository, projection, or bus facade exists, and the adapter keeps a `wrapped_store` property so a caller that later grows an event loop can drop straight to the async object it already has.
- **Every sync call is bounded.** The adapter takes a `timeout` (default 30.0s) at construction and every `*_sync` method takes a keyword-only per-call override, so a blocking caller can never wait indefinitely. Both paths through `_run_sync` raise `TimeoutError` with the effective timeout in the message, and the running-loop path cancels the underlying future before raising rather than leaving orphaned work on the loop.

### Implementation Patterns

The patterns that fall out of the choice above are consistent across the codebase:

- **Coroutines all the way down the I/O path.** A command handler awaits `AggregateRepository.save`, which awaits `EventStore.append_events`, which awaits its driver. Nothing in that chain steps outside the loop, so cancellation propagates from the runner to the driver without a bespoke stop flag.
- **Async iteration for unbounded reads.** `read_all` yields `StoredEvent` values as they arrive rather than materializing a list, which is what lets a projection catch-up stream a large store under constant memory.
- **Deadlines expressed as `asyncio.wait_for`, not socket options.** Drain, checkpoint, and close steps each get their own bound, so a slow backend degrades one phase instead of hanging shutdown.
- **The adapter detects its context rather than assuming one.** `_run_sync` calls `asyncio.get_running_loop()` first; a `RuntimeError` means the ordinary sync case and it falls through to `asyncio.run(asyncio.wait_for(coro, timeout))`. A running loop instead logs a warning and hands the coroutine to `asyncio.run_coroutine_threadsafe` against a class-level `ThreadPoolExecutor` (four workers, `sync_adapter` thread prefix, guarded by a lock, released via `shutdown_executor()`). The addendum walks through all three cases.
- **The same deadline, enforced by two different mechanisms.** On the no-loop path the bound is `asyncio.wait_for` inside `asyncio.run`, so the coroutine is cancelled at the source; on the running-loop path it is `future.result(timeout=...)` followed by an explicit `future.cancel()`. Both re-raise as `TimeoutError` carrying the effective timeout, so callers see one failure mode regardless of which branch ran.
- **The adapter validates what it wraps.** `__init__` raises `TypeError` unless the argument is an `EventStore` instance, keeping the sync surface from being pointed at something that only partially resembles a store.
- **Streaming collapses to a list at the sync boundary.** `read_all_sync` drives the async iterator to exhaustion inside a helper coroutine and returns a `list[StoredEvent]`, because a synchronous caller cannot consume an async generator. This is a real behavioral difference from the async path, and the reason the docstring points large-store callers at `ReadOptions.limit`.

## Consequences

The commitment has paid off where the workload is genuinely concurrent I/O and cost the most where a caller does not already own an event loop. What follows is what the choice actually produced, not what it promised.

### Positive

- **One implementation per backend, one conformance suite.** `testing/conformance.py` defines a single set of behavioral tests that every `EventStore` and `EventBus` implementation is held to. Because there is no second hierarchy, PostgreSQL, SQLite, and in-memory stores cannot drift apart on the sync side while the async side stays correct -- there is no sync side.
- **Cancellation is free and uniform.** A subscription runner stops because its task is cancelled; `asyncio.CancelledError` propagates from `subscriptions/shutdown.py` down through the runner, the handler, and the driver without any cooperative stop flag being threaded through the call chain. The thread-based equivalent has no way to interrupt a blocked consumer at all.
- **Backpressure and deadlines use the same vocabulary as everything else.** `asyncio.Semaphore` in `subscriptions/flow_control.py` bounds in-flight work, `asyncio.wait_for` bounds each shutdown phase, `asyncio.sleep` implements retry backoff. A reader who understands one of these understands all of them; there is no separate thread-pool sizing model to reason about alongside them.
- **Many concurrent subscriptions cost little.** Several long-lived consumers, each awaiting its own store and bus, are several tasks on one loop rather than several OS threads with their own stacks and their own connection pool slots.
- **Streaming reads stay constant-memory.** `read_all` is an async iterator, so a projection catch-up over a large store yields `StoredEvent` values as they arrive. This is the single most load-bearing consequence of the async interface for operational behavior.
- **Registration remained cheap.** Because only I/O is coloured -- `EventBus.subscribe` and `AggregateRepository.create_new` are ordinary methods -- module-level wiring code composes a system without needing a loop to do it.

### Negative

- **Async is viral, and the library cannot contain it.** Any caller that wants to append an event must itself be a coroutine, all the way up to whatever owns the loop. For a codebase that is not already async, this is not a small integration -- it is a structural change to the call graph.
- **A blocking handler stalls every subscription sharing the loop.** Nothing in the library prevents a user's projection handler from making a blocking `requests` call or a CPU-heavy computation. When it does, every other consumer on that loop stops, and the failure presents as unexplained latency rather than as an error.
- **The sync adapter is genuinely worse than the async path, on both branches.** In the ordinary no-loop case, every `*_sync` call spins up a fresh event loop via `asyncio.run` and tears it down when the call returns, which means per-call loop setup and, for connection-pooling backends, no reuse across calls. In the running-loop case it pays a thread hop plus cross-thread future signalling. Neither is a performance-neutral escape hatch.
- **The shared executor is process-global state with a manual lifecycle.** `SyncEventStoreAdapter._executor` is a class attribute -- four workers, guarded by `_executor_lock` -- shared by every adapter instance in the process. It is created lazily on first running-loop call and only released by an explicit `shutdown_executor()`, which application code has to remember to call. Four workers is also a fixed ceiling, not a tunable.
- **`read_all_sync` breaks the streaming guarantee.** Its `collect_events()` helper drives the async iterator to exhaustion into a `list[StoredEvent]` before returning, because a sync caller cannot consume an async generator. The docstring points large-store callers at `ReadOptions.limit`, but the memory profile of the sync path is fundamentally different from the async one and there is no way to fix that without giving up the sync signature.
- **The class docstring's three scenarios are two branches in the code.** `_run_sync` documents "event loop exists but not running -> `loop.run_until_complete()`" as case 2, but the implementation only calls `asyncio.get_running_loop()`; a `RuntimeError` -- which covers both "no loop" and "loop exists but is not running" -- falls through to `asyncio.run`. The behavior is correct for both cases, but the documented three-way split does not correspond to three code paths.
- **The running-loop branch's `except RuntimeError` is broader than intended.** The `try` in `_run_sync` encloses not just `get_running_loop()` but also `run_coroutine_threadsafe` and `future.result()`. A `RuntimeError` raised by the wrapped coroutine itself on the running-loop path is therefore caught by the no-loop handler, and the already-consumed coroutine is then passed to `asyncio.run` -- surfacing as "cannot reuse already awaited coroutine" rather than the original error. This is a latent sharp edge on a path the library already discourages.
- **Sync interop stops at the store.** There is no sync `AggregateRepository`, projection, or bus. A Celery task can append and read events, but it cannot load an aggregate, apply a command, and save through the repository without writing that orchestration itself against the raw store API.

### Neutral

- **Python 3.11+ is a floor, not a preference.** The subscription layer's use of `asyncio.timeout` and the runtime's coroutine performance improvements make the version requirement in `pyproject.toml` load-bearing rather than incidental.
- **Every test is an async test.** `asyncio_mode = "auto"` in `pyproject.toml` means `pytest-asyncio` drives coroutine tests without per-test decoration. This is convenient, and it also means the test suite exercises the library only under a loop that the harness owns -- the sync adapter's own behavior needs its own tests, which is what `tests/unit/sync/test_adapter.py` and `tests/unit/sync/test_concurrency.py` are for.
- **Optional backends are optional in the usual way.** Each async driver is an extra guarded by an `ImportError` and an `*_AVAILABLE` flag; async-first did not change how optional dependencies are handled, only which packages they are.
- **The escape hatch runs in both directions.** `SyncEventStoreAdapter.wrapped_store` returns the underlying async store, so a codebase that starts sync and later adopts a loop unwraps rather than rewires. The adapter is a migration aid as much as an interop layer.
- **ADR-0007 closed the other direction.** Removing the `SyncEventStore` abstract class in 0.2.0 made the "one hierarchy" claim literally true; before that, the codebase carried an unimplemented second interface that suggested a symmetry it never delivered.
