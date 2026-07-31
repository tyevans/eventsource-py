# Error Handling

This guide shows you how to react to the exceptions `eventsource` raises: which ones to retry, which ones mean you have a bug to fix, and which ones should surface to the caller.

Use it when you are writing the code *around* the library — command handlers, projection runners, HTTP endpoints — and you need to decide what belongs in an `except` block.

Most of what you need comes from two places:

```python
from eventsource import (
    EventSourceError,        # catch-all base for the core exceptions
    OptimisticLockError,     # concurrent write to the same aggregate
    AggregateNotFoundError,
    AggregateNotCreatedError,
    EventVersionError,
    EventNotFoundError,
    ProjectionError,
)
from eventsource.exceptions import (
    UnhandledEventError,
    SerializationError,
    EventStoreError,
    EventBusError,
    CheckpointError,
)
```

Four things are worth knowing before you write your first handler:

- **Not every exception inherits from `EventSourceError`.** The subscription, snapshot, and read-model subsystems each define their own root that derives directly from `Exception`. A bare `except EventSourceError` will not catch them — see [Catching broadly at the application boundary](#catching-broadly-at-the-application-boundary).
- **There are two `OptimisticLockError` classes.** `eventsource.exceptions.OptimisticLockError` is the aggregate/event-store version conflict; `eventsource.readmodels.exceptions.OptimisticLockError` is the read-model one. They are unrelated classes and neither catches the other.
- **Retryability is a property of the exception type, not of the operation.** `OptimisticLockError` is almost always worth retrying; `UnhandledEventError` never is. The sections below group the exceptions by that distinction so you can map each one to an action.
- **Projections and subscriptions handle failure differently, and the difference decides where your `except` block goes.** A projection owns its own failures: `CheckpointTrackingProjection` retries under its `retry_policy` (by default `ExponentialBackoffRetryPolicy` with `max_retries=2`, so three attempts), writes the event to the DLQ, then re-raises. A subscription escalates instead: `SubscriptionManager` routes every handler failure through a per-subscription `SubscriptionErrorHandler`, which classifies the error and decides — via `ErrorHandlingStrategy` — whether to retry, continue, or route to the DLQ. Configure the projection's policy for projection failures; configure the subscription's handler for everything dispatched by the manager.

If you only want the mapping, jump to the [quick reference table](#quick-reference-table-exception---cause---action) at the end.

## The exception hierarchy at a glance

There is no single root exception in this library. There are four independent trees, and knowing which one an exception belongs to tells you what your `except` clauses have to look like.

```
Exception
├── EventSourceError                    (eventsource.exceptions)
│   ├── OptimisticLockError             aggregate_id, expected_version, actual_version
│   ├── EventVersionError               aggregate_id, event_id, expected_version, actual_version
│   ├── UnhandledEventError             event_type, event_id, handler_class, available_handlers
│   ├── AggregateNotCreatedError        aggregate_class, suggestion
│   ├── AggregateNotFoundError          aggregate_id, aggregate_type
│   ├── EventNotFoundError              event_id
│   ├── ProjectionError                 projection_name, event_id
│   ├── SerializationError              event_type
│   ├── EventStoreError
│   ├── EventBusError
│   ├── CheckpointError
│   ├── TenantContextNotSetError        (eventsource.multitenancy.exceptions)
│   └── TenantMismatchError             (eventsource.multitenancy.exceptions)
├── SubscriptionError                   (eventsource.subscriptions.exceptions)
├── SnapshotError                       (eventsource.exceptions -- not actually a subclass of EventSourceError)
└── ReadModelError                      (eventsource.readmodels.exceptions)
```

Write your handlers against this shape: catch the specific type when you can act on it, fall back to the subsystem root, and only then to `EventSourceError`.

### `EventSourceError` as the single catch-all base

`EventSourceError` is a bare subclass of `Exception` with no added behaviour — it exists purely so you can write one `except` clause for the core event-sourcing surface:

```python
from eventsource import EventSourceError

try:
    await repository.save(order)
except EventSourceError as exc:
    logger.exception("event sourcing failure", extra={"error": type(exc).__name__})
    raise
```

Everything in `eventsource/exceptions.py` inherits from it, and so do the two multi-tenancy exceptions (`TenantContextNotSetError`, `TenantMismatchError`) even though they live in `eventsource/multitenancy/exceptions.py`. That is the full set — nothing else in the library is guaranteed to be caught by it.

The exceptions with structured attributes carry them as plain instance attributes set in `__init__`, so you can read them directly without parsing the message:

```python
except OptimisticLockError as exc:
    metrics.increment(
        "aggregate.conflict",
        tags={"aggregate": str(exc.aggregate_id), "expected": exc.expected_version},
    )
```

Import the common ones from the package root; the rest come from `eventsource.exceptions`:

```python
# available from the top-level package
from eventsource import (
    EventSourceError,
    OptimisticLockError,
    AggregateNotFoundError,
    AggregateNotCreatedError,
    EventNotFoundError,
    EventVersionError,
    ProjectionError,
)

# only from the submodule
from eventsource.exceptions import (
    EventStoreError,
    EventBusError,
    CheckpointError,
    SerializationError,
    UnhandledEventError,
)
```

`EventSourceError` is never raised on its own — it is always one of the concrete subclasses above. Because it adds nothing, you can also subclass it for your own domain errors when you want them swept up by the same boundary handler:

```python
class InsufficientFundsError(EventSourceError):
    """Raised by the Account aggregate when a debit would overdraw."""
```

### Per-subsystem hierarchies that do NOT inherit from it (`SubscriptionError`, `SnapshotError`, `ReadModelError`, `readmodels.OptimisticLockError`)

Three subsystems define their own root, each deriving straight from `Exception`. If your boundary handler only catches `EventSourceError`, these will fly past it and hit whatever generic 500 handler sits above you.

**`SubscriptionError`** — `eventsource.subscriptions.exceptions`. Raised by the subscription manager and runners: `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `CheckpointNotFoundError` (has `.projection_name`), `EventStoreConnectionError`, `EventBusConnectionError`, `TransitionError`. Note that `CheckpointNotFoundError` here is unrelated to `eventsource.exceptions.CheckpointError`, which *is* an `EventSourceError`.

**`SnapshotError`** — `eventsource.exceptions` (moved here from its own `snapshots` module in the ring migration; still not part of the `EventSourceError` tree), with `SnapshotDeserializationError` (`aggregate_id`, `aggregate_type`, `original_error`), `SnapshotSchemaVersionError` (adds `snapshot_schema_version` and `expected_schema_version`), and `SnapshotNotFoundError`. These are largely internal: the snapshot path is designed so that a failed load degrades to a full event replay rather than surfacing to you — a *missing* snapshot is not an error at all, `get_snapshot()` simply returns `None`. Catch `SnapshotError` if you want to log the degradation; do not treat it as a request failure.

```python
from eventsource.exceptions import SnapshotError

try:
    snapshot = await snapshot_store.get_snapshot(aggregate_id, "Order")
except SnapshotError:
    snapshot = None  # fall back to replaying the full stream
```

**`ReadModelError`** — `eventsource.readmodels.exceptions`, with `ReadModelNotFoundError` (`model_id`) and its own `OptimisticLockError`. Both are re-exported from `eventsource.readmodels`.

That last one is the trap. `eventsource.readmodels.exceptions.OptimisticLockError` inherits from `ReadModelError`, **not** from `EventSourceError`, and its attributes are different: `model_id`, `expected_version`, and `actual_version` (which may be `None` when the row was missing at check time), versus the aggregate version's `aggregate_id`, `expected_version`, `actual_version`. Neither class catches the other, so alias them on import whenever both are in scope:

```python
from eventsource import OptimisticLockError as AggregateConflict
from eventsource.readmodels.exceptions import OptimisticLockError as ReadModelConflict

try:
    await repo.save_with_version_check(summary)
except ReadModelConflict as exc:
    logger.warning("read model %s conflicted at v%s", exc.model_id, exc.expected_version)
```

Two more trees exist outside all of this and are only relevant if you use those features: `MigrationError` (`eventsource.migration.exceptions`, for the live-migration tooling) and the event registry's `EventTypeNotFoundError` (a `KeyError`) and `DuplicateEventTypeError` (a `ValueError`), neither of which is an `EventSourceError`.

## Deciding how to react: retryable vs. bug vs. fatal

Before writing an `except` block, sort the exception into one of four buckets. The bucket determines the action; the specific type only determines what you log.

| Bucket | What it means | Action |
| --- | --- | --- |
| **Retryable** | Two writers raced, or infrastructure blipped | Reload state, replay the command, bounded retries |
| **Programming error** | Your code or wiring is wrong | Let it crash, fix the code — never retry |
| **Lookup failure** | The caller asked for something that does not exist | Map to a 404 / domain-level "not found" |
| **Fatal / infrastructure** | The store, bus, or checkpoint backend is unavailable | Fail the request, alert, retry at a higher level |

Sorting the concrete exceptions into those buckets:

- **Retryable** — `OptimisticLockError` (the aggregate one). This is the only core exception that is *expected* under normal load: two commands touched the same aggregate concurrently and one lost. Reload and replay.
- **Programming errors** — `UnhandledEventError`, `AggregateNotCreatedError`, `EventVersionError`, `SerializationError`, `TenantContextNotSetError`, `TenantMismatchError`. Each of these means a decorator is missing, a guard was skipped, an event type was never registered, or a scope was not entered. Retrying reproduces the failure identically. The messages are written to be actionable — `UnhandledEventError` even lists `available_handlers` so you can spot the typo — so log the exception and let it propagate.
- **Lookup failures** — `AggregateNotFoundError`, `EventNotFoundError`, `ReadModelNotFoundError`. Not bugs and not transient: the identifier does not exist. Translate at the boundary rather than letting a 500 escape.
- **Fatal / infrastructure** — `EventStoreError`, `EventBusError`, `CheckpointError`, and the subscription tree's `EventStoreConnectionError` / `EventBusConnectionError`. These are wrappers around a backend that is unreachable or misbehaving. A short retry at the connection layer is reasonable; a retry loop in your command handler is not, because the failure will persist for as long as the outage does.

Two exceptions sit deliberately outside the table:

- **`SnapshotError`** is neither: the snapshot path degrades to a full replay on its own, so the correct reaction is to log and continue, not to fail the request.
- **`ProjectionError`** is *not* raised by the library on your behalf, and it does not wrap anything. `CheckpointTrackingProjection` re-raises whatever your `_process_event()` raised, unchanged, after its `RetryPolicy` is exhausted and the event has gone to the DLQ. So the bucket you land in is the bucket of your own exception. Construct `ProjectionError(projection_name, event_id, message)` yourself when you want a failure to carry the projection name and event ID. This is also why projections take a `RetryPolicy` rather than a fixed retry count: retry the transient causes, send the rest to the DLQ.

The mechanical form of this decision, in a command handler:

```python
from eventsource import (
    AggregateNotFoundError,
    EventStoreError,
    OptimisticLockError,
)

async def place_order(command: PlaceOrder) -> None:
    for attempt in range(3):
        try:
            order = await repository.get(command.order_id)   # AggregateNotFoundError
            order.place(command)                             # domain / programming errors
            await repository.save(order)                     # OptimisticLockError
            return
        except OptimisticLockError:
            continue                    # retryable: reload and replay
        except AggregateNotFoundError:
            raise HTTPException(404)    # lookup failure
        except EventStoreError:
            raise HTTPException(503)    # infrastructure
    raise HTTPException(409)            # retries exhausted
```

Notice what is *not* caught: `UnhandledEventError`, `AggregateNotCreatedError`, and `EventVersionError` fall straight through. That is intentional. Swallowing them hides the bug and, worse, leaves the aggregate in a half-applied state that the next command will inherit.

One rule of thumb covers most cases: **retry only when a second attempt runs against different state.** `OptimisticLockError` qualifies — the reload sees the winning writer's events. A missing `@handles` decorator does not, no matter how many times you try.

If you retry a bucket you should not have, the failure mode is specific and worth recognising: a programming error retried in a subscription runner becomes a poison-pill loop that stalls the projection at one checkpoint until the retry budget is spent and the event lands in the DLQ. That is the system telling you the bucket was wrong.

## Retryable: concurrency and transient failures

Two kinds of failure are worth retrying in place: a *version conflict*, where another writer committed to the same aggregate between your load and your save, and a *transient infrastructure blip*, where the connection dropped or a call timed out. Everything else in this guide is either a bug or a genuine failure to report.

The version conflict is the one you will actually see under load. Every `EventStore` implementation — PostgreSQL, SQLite, and in-memory alike — re-checks the aggregate's current version inside `append()` and raises `eventsource.exceptions.OptimisticLockError(aggregate_id, expected_version, actual_version)` if it moved. `AggregateRepository.save()` computes `expected_version` as `aggregate.version - len(uncommitted_events)` and passes it straight through, so the conflict surfaces from `save()`:

```python
from eventsource import OptimisticLockError

order = await repo.load(order_id)   # loaded at version 7
order.ship(tracking_number="TRACK123")
await repo.save(order)              # another writer got to version 8 first
# -> OptimisticLockError: expected version 7, but current version is 8
```

**Retry by discarding the aggregate instance, not by re-saving it.** `save()` only calls `mark_events_as_committed()` when the append succeeds, so after a conflict your in-memory aggregate still holds its uncommitted events *and* the state they produced. Calling `save()` again re-sends the same events with the same stale `expected_version` and fails identically; mutating and re-saving compounds decisions made against state that is now known to be out of date. Load a fresh aggregate and re-run the command against it:

```python
async def ship_order(order_id: UUID, tracking_number: str) -> None:
    for attempt in range(3):
        order = await repo.load(order_id)      # fresh state, includes the winner's events
        order.ship(tracking_number=tracking_number)
        try:
            await repo.save(order)
            return
        except OptimisticLockError as exc:
            logger.info(
                "conflict on %s: expected v%s, actual v%s (attempt %s)",
                exc.aggregate_id, exc.expected_version, exc.actual_version, attempt + 1,
            )
    raise ConflictError(order_id)
```

The `load()` call belongs *inside* the loop. That is the whole point: the retry only makes progress because the second attempt runs the command against different state — and because the command re-runs, its invariant checks get a chance to reject it. If shipping an already-shipped order is illegal, the reload is what lets your domain say so instead of silently double-writing.

Bound the loop. Conflicts on the same aggregate mean contention, and unbounded retries under contention turn into a livelock that keeps the store busy while no writer wins. Three attempts is a reasonable default for interactive commands; add a short randomised sleep between attempts if you see the same two writers repeatedly colliding. When the budget is exhausted, that is a real answer — return 409 to the caller rather than looping harder.

Two ways to avoid the conflict rather than absorb it:

- **Serialize the writers.** `PostgreSQLLockManager` (`eventsource.locks`) gives you advisory locks keyed by aggregate id, so a second writer waits instead of racing. Worth it when conflicts are frequent and the command is expensive to replay; see [Distributed locks](distributed-locks.md).
- **Opt out of the check.** `ExpectedVersion.ANY` (`-1`) tells `append()` to skip the version check entirely. That disables optimistic locking for that append, so reserve it for genuinely order-independent streams — it does not make concurrent writes safe, it makes them silent.

Transient infrastructure failures are the second retryable class, and they are *not* `EventSourceError` subclasses — they arrive as `ConnectionError`, `TimeoutError`, `asyncio.TimeoutError`, or `OSError` from the driver, sometimes wrapped in `EventStoreError` or `EventBusError`. The library ships a helper for exactly this shape, `retry_async()` from `eventsource.subscriptions`:

```python
from eventsource.subscriptions import RetryConfig, RetryError, retry_async

try:
    events = await retry_async(
        lambda: store.get_events(order_id),
        config=RetryConfig(max_retries=3, initial_delay=0.1, max_delay=2.0),
        operation_name="get_events",
    )
except RetryError as exc:
    logger.error("store unreachable after %s attempts: %s", exc.attempts, exc.last_error)
    raise
```

`RetryConfig` defaults to `max_retries=5`, `initial_delay=1.0`, `max_delay=60.0`, `exponential_base=2.0`, and `jitter=0.1` (a fraction of the delay added as randomness, so a fleet of clients does not retry in lockstep). It validates its arguments at construction: `max_delay` must be at least `initial_delay`, `exponential_base` must exceed `1.0`, and `jitter` must be within `0.0`–`1.0`. When every attempt fails, `retry_async` raises `RetryError` carrying `attempts` and `last_error` — not the original exception, so catch `RetryError` and unwrap it for logging.

By default `retry_async` retries only `TRANSIENT_EXCEPTIONS` (`ConnectionError`, `TimeoutError`, `asyncio.TimeoutError`, `OSError`) and re-raises anything else immediately. Do **not** widen `retryable_exceptions` to include `OptimisticLockError`: the helper re-invokes the same callable, and a conflict needs a reload between attempts, which only your loop can do.

For a backend that is down rather than flaky, wrap the retry in `CircuitBreaker` (also in `eventsource.subscriptions`, configured via `CircuitBreakerConfig`: `failure_threshold=5`, `recovery_timeout=30.0`, `half_open_max_calls=1`). Retrying into a dead dependency just multiplies the load on it; the breaker opens after the threshold and fails fast until the recovery window elapses.

### Handle `OptimisticLockError` by reloading the aggregate and replaying the command

The recovery has exactly three moving parts, and getting them in the right order is the whole technique:

1. **Throw the aggregate instance away.** It is unusable after a conflict.
2. **`load()` a fresh one** so the winning writer's events are part of your state.
3. **Re-run the command method** against that fresh instance, then `save()` again.

Step 1 is the one people skip. `AggregateRepository.save()` calls `aggregate.mark_events_as_committed()` *only* inside the `result.success` branch, so a conflict leaves `aggregate.uncommitted_events` fully populated. The instance is now carrying events that were never persisted, plus the state those events produced. Re-saving it re-sends the same list with the same `expected_version` (`aggregate.version - len(uncommitted_events)`) and fails identically, forever. Mutating it further is worse: you are appending decisions made on top of a version the store has already rejected.

Wrap it as a function that takes the command's inputs, not the loaded aggregate — that way the reload is structurally inside the retry:

```python
from uuid import UUID

from eventsource import OptimisticLockError

MAX_ATTEMPTS = 3

async def ship_order(order_id: UUID, tracking_number: str) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        order = await repo.load(order_id)          # (2) fresh state each attempt
        order.ship(tracking_number=tracking_number)  # (3) command re-decides
        try:
            await repo.save(order)
            return
        except OptimisticLockError as exc:
            logger.info(
                "conflict on %s: expected v%s, actual v%s (attempt %s/%s)",
                exc.aggregate_id, exc.expected_version, exc.actual_version,
                attempt, MAX_ATTEMPTS,
            )
            # (1) `order` is discarded here — the next iteration loads a new one
    raise ConflictError(order_id)
```

Contrast that with the shape that looks equivalent and is not:

```python
order = await repo.load(order_id)        # WRONG: load is outside the loop
order.ship(tracking_number=tracking_number)
for _ in range(3):
    try:
        await repo.save(order)           # same events, same stale expected_version
        return
    except OptimisticLockError:
        continue                         # spins three times, fails three times
```

Re-running the command is not merely a way to rebuild the event list — it is what makes the retry *correct*. The reloaded aggregate has the other writer's events applied, so `ship()` re-evaluates its invariants against the real current state. If the competing command already shipped the order, your domain method gets to reject it instead of writing a second `OrderShipped`. A retry that skipped the command call would happily double-write.

Two consequences worth planning for:

- **The command may now legitimately fail.** After the reload, `ship()` might raise your own domain error, or `AggregateNotCreatedError` if the state is not what the command assumed. Do not catch those in the retry loop — they are the reload doing its job. Only `OptimisticLockError` should route back to the top.
- **The command must be safe to run more than once.** Keep side effects (charging a card, sending mail) out of the aggregate method and behind the successful `save()`, or driven by a projection off the committed events. Anything the command does before `save()` will happen once per attempt.

A conflict costs one extra load and one extra command evaluation, so under normal contention this is cheap. If a specific aggregate conflicts constantly, the fix is upstream — serialize the writers with an advisory lock, or split the aggregate — not a longer retry loop. Bounding and backoff are covered in [Bound your retries and pick a backoff](#bound-your-retries-and-pick-a-backoff).

### Read the `aggregate_id` / `expected_version` / `actual_version` attributes for logging and metrics

`OptimisticLockError` sets all three in `__init__` before building its message, so read them as plain attributes instead of parsing `str(exc)`:

```python
class OptimisticLockError(EventSourceError):
    def __init__(self, aggregate_id: UUID, expected_version: int, actual_version: int) -> None:
        self.aggregate_id = aggregate_id
        self.expected_version = expected_version
        self.actual_version = actual_version
```

- `aggregate_id` — a `UUID`, the stream that conflicted. Convert it with `str()` before putting it in a log field or metric tag.
- `expected_version` — exactly the value your caller passed to `append()`. For a `repo.save()` this is `aggregate.version - len(uncommitted_events)`, i.e. the version you loaded at.
- `actual_version` — the store's current version for that stream: the number of events already persisted for the `(aggregate_id, aggregate_type)` pair.

The gap between them is the useful signal. `actual_version - expected_version` is how many events the winning writer(s) committed while you held your copy — `1` is a normal two-writer race, consistently larger values mean a hot aggregate with several concurrent writers, and that is a design problem no retry loop fixes:

```python
except OptimisticLockError as exc:
    drift = exc.actual_version - exc.expected_version
    logger.info(
        "aggregate conflict",
        extra={
            "aggregate_id": str(exc.aggregate_id),
            "expected_version": exc.expected_version,
            "actual_version": exc.actual_version,
            "version_drift": drift,
            "attempt": attempt,
        },
    )
    metrics.increment("aggregate.conflict", tags={"aggregate_type": "Order"})
    metrics.histogram("aggregate.conflict.drift", drift)
```

Note what the exception does *not* carry: there is no `aggregate_type` attribute, even though the store checked the version per `(aggregate_id, aggregate_type)` pair. Tag your metrics with the type from your own call site — `aggregate_id` alone is unbounded cardinality and makes a poor metric dimension. Use it as a log field, aggregate on the type.

Two cases will make `expected_version` look wrong if you treat it as a version number unconditionally. It is whatever was passed in, and `ExpectedVersion` sentinels are negative or zero:

| `expected_version` | Meaning | What a conflict means |
| --- | --- | --- |
| `0` (`NO_STREAM`) | "this aggregate must not exist yet" | someone already created it; `actual_version` > 0 |
| `-2` (`STREAM_EXISTS`) | "this aggregate must already exist" | the stream is empty; `actual_version` is `0` |
| `-1` (`ANY`) | check disabled | never raised |
| any other int | exact version match | `actual_version` moved |

So guard the drift calculation, or you will emit a `drift` of `7` for a duplicate-creation conflict that has nothing to do with contention:

```python
if exc.expected_version >= 0:
    drift = exc.actual_version - exc.expected_version
else:
    drift = None  # STREAM_EXISTS sentinel — not a version gap
```

An `expected_version` of `0` with a non-zero `actual_version` is worth alerting on separately: it is a duplicate-creation attempt, usually a retried request or a non-idempotent command path, and reloading-and-replaying is the wrong recovery for it.

`actual_version` is always meaningful and never negative — every backend derives it from the stream itself. The in-memory and PostgreSQL/SQLite stores compute it as the stored event count for the pair, and on the race where the database's unique constraint fires rather than the pre-check, the backend re-queries `COALESCE(MAX(version), 0)` before raising, so the number is the freshly observed truth in both paths. That makes it safe to log as "the version you would have gotten had you reloaded" — though you still have to actually reload, because the events themselves are what your command needs.

For tracing, attach the three attributes as span attributes rather than recording the exception and letting the message be re-parsed downstream:

```python
except OptimisticLockError as exc:
    span.set_attribute("eventsource.aggregate_id", str(exc.aggregate_id))
    span.set_attribute("eventsource.expected_version", exc.expected_version)
    span.set_attribute("eventsource.actual_version", exc.actual_version)
```

Finally, log conflicts at `INFO` or `DEBUG` while the retry loop is still running, and only escalate to `WARNING`/`ERROR` when the budget is exhausted. A conflict that the next attempt resolves is the system working as designed; logging each one at `ERROR` buries the retries that genuinely failed.
