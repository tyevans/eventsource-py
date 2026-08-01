# Exceptions

Reference for the error types raised by `eventsource`.

The core hierarchy lives in `eventsource.domain.exceptions` and is rooted at a single
base class, `EventSourceError`. It contains thirteen types:

| Exception | Structured attributes |
| --- | --- |
| `EventSourceError` | — (base class) |
| `OptimisticLockError` | `aggregate_id`, `expected_version`, `actual_version` |
| `EventNotFoundError` | `event_id` |
| `ProjectionError` | `projection_name`, `event_id` |
| `AggregateNotFoundError` | `aggregate_id`, `aggregate_type` |
| `EventStoreError` | — |
| `EventBusError` | — |
| `CheckpointError` | — |
| `SerializationError` | `event_type` |
| `EventVersionError` | `expected_version`, `actual_version`, `event_id`, `aggregate_id` |
| `UnhandledEventError` | `event_type`, `event_id`, `handler_class`, `available_handlers` |
| `AggregateNotCreatedError` | `aggregate_class`, `suggestion` |
| `LockAcquisitionError` | `key`, `reason`, `timeout` (ADR 0029: rebased onto `EventSourceError`) |
| `LockNotHeldError` | `key` (ADR 0029: rebased onto `EventSourceError`) |

Every one of these accepts its attributes as constructor arguments and builds a
human-readable message from them, so the attributes are always populated when
the library raises the error — you can branch on them rather than parsing
`str(exc)`.

Six of these are re-exported from the package root (`AggregateNotCreatedError`,
`AggregateNotFoundError`, `EventNotFoundError`, `EventSourceError`,
`EventVersionError`, `OptimisticLockError`, `ProjectionError`); the rest must be
imported from `eventsource.domain.exceptions`. See
[Import paths and public exports](#import-paths-and-public-exports).

Other subsystems — multi-tenancy, snapshots, migration, the read-model layer,
and the optional bus backends — define their own error types in their own
modules. (`SubscriptionError` used to be one of these; ADR 0031 rebased it
onto `EventSourceError` -- see below.) Those are catalogued under
[Related exceptions outside the core hierarchy](#related-exceptions-outside-the-core-hierarchy);
note in particular that `eventsource.ports.readmodels.exceptions` defines a
*second*, unrelated `OptimisticLockError`.

## Overview

Errors in `eventsource` fall into two groups.

**The core hierarchy** in `eventsource.domain.exceptions` covers the failures that
arise from the fundamental event-sourcing operations: appending events under a
version expectation, loading an aggregate, applying an event to aggregate state,
serializing an event, and running a projection. Everything there derives from
`EventSourceError`, so a single `except EventSourceError:` catches the whole
group.

**Subsystem errors** live beside the code that raises them (or, for
`SnapshotError`, alongside the core hierarchy in `eventsource.domain.exceptions`
despite not deriving from it) — `eventsource.migration`,
`eventsource.multitenancy`, `eventsource.ports.readmodels.exceptions`, and the
optional bus backends each define their own families. These are *not*
subclasses of `EventSourceError`; catching the core base class will not catch
them. (`LockAcquisitionError` and `LockNotHeldError` moved onto
`EventSourceError` under ADR 0029, and `SubscriptionError` moved the same way
under ADR 0031 -- all three used to be bare-`Exception` families and are now
part of the core hierarchy; see the diagram below.)

Three properties of the core hierarchy are worth knowing before you write
handling code:

- **Errors carry structured data, not just text.** Where an error has
  attributes, they are constructor arguments assigned to `self` and then
  formatted into the message. `OptimisticLockError` tells you the
  `expected_version` and `actual_version`, which is what makes the
  reload-and-retry loop possible without string parsing.
- **The hierarchy is flat.** Every core error derives directly from
  `EventSourceError`; there are no intermediate base classes. So you can catch
  everything, or catch one specific type, but there is no middle tier such as
  "all storage errors".
- **Four types are markers with no attributes.** `EventStoreError`,
  `EventBusError`, `CheckpointError`, and `EventSourceError` itself are plain
  `pass` subclasses; they carry only whatever message the raiser passes to
  `Exception.__init__`.

Two of the errors are configuration-sensitive rather than unconditional.
`UnhandledEventError` is raised only when a `DeclarativeAggregate` or
`DeclarativeProjection` sets `unregistered_event_handling = "error"`; under
`"warn"` or `"ignore"` the same situation produces no exception.
`AggregateNotCreatedError` is raised only when an aggregate sets
`requires_creation_event = True` and `.state` is read before any event has been
applied — `state_or_none` and `is_created` are the non-raising alternatives.

The sections below document each core error's attributes and the code paths that
raise it, then catalogue the subsystem families and their import paths.

## Exception hierarchy

`eventsource.domain.exceptions` defines fourteen classes: `EventSourceError` and
thirteen direct subclasses (including `LockAcquisitionError` and
`LockNotHeldError`, moved here from a standalone `Exception` base under ADR
0029). There are no intermediate base classes inside the module — every error
is exactly one level below the root.

Two classes defined elsewhere also derive from `EventSourceError`:
`TenantContextNotSetError` and `TenantMismatchError`, both in
`eventsource.multitenancy.exceptions`. They are the only members of the
`EventSourceError` tree that live outside `eventsource/exceptions.py`, so
`except EventSourceError:` catches tenant-context failures as well as the core
eleven.

Every other error family in the library is rooted at a plain Python builtin,
not at `EventSourceError`:

| Family | Root | Base class |
| --- | --- | --- |
| Snapshots | `SnapshotError` | `Exception` (defined in `eventsource.domain.exceptions`, alongside but not part of the core hierarchy) |
| Read models | `ReadModelError` | `Exception` |
| Subscriptions | `SubscriptionError` | `Exception` |
| Migration | `MigrationError` | `Exception` |
| Event registry | `EventTypeNotFoundError` | `KeyError` |
| Event registry | `DuplicateEventTypeError` | `ValueError` |
| Optional backends | `RedisNotAvailableError`, `RabbitMQNotAvailableError`, `KafkaNotAvailableError`, `SQLiteNotAvailableError` | `ImportError` |

Catching `EventSourceError` will not catch any of these.

### Hierarchy diagram

The tree below shows every named exception class in the library, annotated with
the module that defines it. Annotations are given once per family; unannotated
entries live in the same module as their nearest annotated ancestor.

```text
Exception
├── EventSourceError                          eventsource.domain.exceptions
│   ├── OptimisticLockError
│   ├── EventNotFoundError
│   ├── ProjectionError
│   ├── AggregateNotFoundError
│   ├── EventStoreError
│   ├── EventBusError
│   ├── CheckpointError
│   ├── SerializationError
│   ├── EventVersionError
│   ├── UnhandledEventError
│   ├── AggregateNotCreatedError
│   ├── TenantContextNotSetError               eventsource.multitenancy.exceptions
│   ├── TenantMismatchError                    eventsource.multitenancy.exceptions
│   ├── LockAcquisitionError                   (ADR 0029: rebased here, was a bare Exception)
│   ├── LockNotHeldError                       (ADR 0029: rebased here, was a bare Exception)
│   └── SubscriptionError                      (ADR 0031: rebased here, was a bare Exception)
│       ├── SubscriptionConfigError
│       ├── SubscriptionStateError
│       ├── SubscriptionAlreadyExistsError
│       ├── CheckpointNotFoundError
│       ├── EventStoreConnectionError
│       ├── EventBusConnectionError
│       └── TransitionError
│
├── SnapshotError                             eventsource.domain.exceptions
│   │                                          (not a subclass of EventSourceError)
│   ├── SnapshotDeserializationError
│   ├── SnapshotSchemaVersionError
│   └── SnapshotNotFoundError
│
├── ReadModelError                            eventsource.ports.readmodels.exceptions
│   ├── OptimisticLockError                    (distinct from the core one)
│   └── ReadModelNotFoundError
│
├── MigrationError                            eventsource.migration.exceptions
│   ├── MigrationNotFoundError
│   ├── MigrationAlreadyExistsError
│   ├── MigrationStateError
│   │   └── InvalidPhaseTransitionError
│   ├── CutoverError
│   │   ├── CutoverTimeoutError
│   │   └── CutoverLagError
│   ├── ConsistencyError
│   ├── BulkCopyError
│   ├── DualWriteError
│   ├── PositionMappingError
│   ├── RoutingError
│   ├── CircuitBreakerOpenError
│   └── SubscriptionMigrationError             eventsource.migration.subscription_migrator
│
├── (standalone, direct Exception subclasses)
│   ├── RetryError                            eventsource.application.subscriptions.retry
│   ├── CircuitBreakerOpenError               eventsource.application.subscriptions.retry
│   │                                          (distinct from the migration one)
│   ├── StoreNotFoundError                    eventsource.migration.router
│   ├── WritePausedError                      eventsource.migration.write_pause
│   ├── ShutdownError                         eventsource.bus.rabbitmq
│   └── BatchPublishError                     eventsource.bus.rabbitmq
│
├── KeyError
│   └── EventTypeNotFoundError                eventsource.events.registry
├── ValueError
│   ├── DuplicateEventTypeError               eventsource.events.registry
│   ├── HandlerSignatureError                 eventsource.handlers.registry
│   └── DeserializationError                  eventsource.bus.kafka
└── ImportError
    ├── RedisNotAvailableError                eventsource.bus.redis
    ├── RabbitMQNotAvailableError             eventsource.bus.rabbitmq
    ├── KafkaNotAvailableError                eventsource.bus.kafka
    └── SQLiteNotAvailableError               eventsource.adapters.sqlite.snapshots
```

Only the migration family has depth beyond one level; everywhere else the
hierarchy is flat, so specificity comes from the attributes an error carries
rather than from its position in the tree.

Two names appear twice in the tree. `OptimisticLockError` is defined both in
`eventsource.domain.exceptions` (aggregate append conflicts) and in
`eventsource.ports.readmodels.exceptions` (read-model row conflicts), and the
two are unrelated classes — catching one will not catch the other. (This
collision predates ADR 0029 and is tracked in `BACKLOG.md`.) `CircuitBreakerOpenError`
is likewise defined twice, in `eventsource.migration.exceptions` (a
`MigrationError`) and in `eventsource.application.subscriptions.retry` (a bare `Exception`).
Import these by module rather than pulling both into one namespace.

## EventSourceError

```python
class EventSourceError(Exception): ...
```

Base class for the core exception hierarchy. Defined in
`eventsource.domain.exceptions` and re-exported from the package root.

**Import**

```python
from eventsource import EventSourceError
# or
from eventsource.domain.exceptions import EventSourceError
```

**Constructor** — none of its own. The class body is `pass`, so it inherits
`Exception.__init__` and accepts any arguments:

```python
raise EventSourceError("Test error")   # str(exc) == "Test error"
```

**Attributes** — none. Unlike most of its subclasses, `EventSourceError` carries
no structured fields; only `args` and whatever message was passed.

### The library never raises it directly

No code path in `src/eventsource/` raises `EventSourceError` itself — it exists
purely as the root of the hierarchy. Every error the library raises from this
tree is one of the concrete subclasses. Treat a bare `EventSourceError` in a
traceback as coming from application code, not from the library.

### What catching it covers

`except EventSourceError:` catches the eleven concrete subclasses in
`eventsource.domain.exceptions` plus `TenantContextNotSetError` and
`TenantMismatchError` from `eventsource.multitenancy.exceptions`, which also
derive from it. It does **not** catch the snapshot, read-model, subscription,
migration, event-registry, or optional-backend families — those are rooted at
`Exception`, `KeyError`, `ValueError`, and `ImportError` respectively. See
[Exception hierarchy](#exception-hierarchy).

### When to catch it

Use it at boundaries where you want to convert any event-sourcing failure into
something else — an HTTP response, a log line, a dead-letter record — and the
specific cause does not change what you do:

```python
try:
    await repository.save(order)
except EventSourceError as exc:
    logger.exception("event sourcing operation failed")
    raise HTTPException(status_code=500, detail=str(exc)) from exc
```

Do not use it where the recovery depends on the cause. `OptimisticLockError`
means "reload and retry" and `AggregateNotFoundError` means "404"; both are
`EventSourceError`, so catching the base class collapses two very different
outcomes into one. Catch the specific type first and let the base class be the
fallback:

```python
try:
    order = await repository.get(order_id)
except AggregateNotFoundError:
    return None
except EventSourceError:
    raise
```

Because it carries no attributes, code that catches `EventSourceError` can only
rely on `str(exc)` and `type(exc)`. If you need to branch on data — a version
number, an aggregate id, an event type — catch the concrete subclass that
defines the attribute you need. See
[Attributes safe to branch on](#attributes-safe-to-branch-on).

## OptimisticLockError

```python
class OptimisticLockError(EventSourceError):
    def __init__(
        self,
        aggregate_id: UUID,
        expected_version: int,
        actual_version: int,
    ) -> None: ...
```

Raised when a version conflict is detected while appending events to a stream.
Defined in `eventsource.domain.exceptions` and re-exported from the package root.

**Import**

```python
from eventsource import OptimisticLockError
# or
from eventsource.domain.exceptions import OptimisticLockError
```

All three constructor arguments are required and positional-friendly; the
message is built from them:

```text
Optimistic lock error for aggregate <aggregate_id>: expected version
<expected_version>, but current version is <actual_version>
```

Note that `eventsource.ports.readmodels.exceptions` defines an unrelated class with
the same name. See
[Name collision: eventsource.ports.readmodels.exceptions.OptimisticLockError](#name-collision-eventsourceportsreadmodelsexceptionsoptimisticlockerror).

### Attributes: aggregate_id, expected_version, actual_version

| Attribute | Type | Meaning |
| --- | --- | --- |
| `aggregate_id` | `UUID` | The aggregate whose stream the append targeted. |
| `expected_version` | `int` | The version the caller asserted, derived from the `ExpectedVersion` passed to `append`. |
| `actual_version` | `int` | The stream's current version at conflict-detection time. |

All three are always populated: the constructor requires them, and every raise
site in the library (`adapters/memory/store.py`, `adapters/postgresql/store.py`,
`adapters/sqlite/store.py`) passes all three positionally. Branching on them is
safe without `getattr` guards.

```python
from eventsource.ports.positions import ExpectedVersion

try:
    await store.append(stream, events, ExpectedVersion.exact(4))
except OptimisticLockError as exc:
    exc.aggregate_id      # UUID('...')
    exc.expected_version  # 4
    exc.actual_version    # 7
    str(exc)
    # "Optimistic lock error for aggregate ...: expected version 4,
    #  but current version is 7"
```

The attributes are assigned before `super().__init__` formats the message, so
the message is always consistent with them — never parse `str(exc)` to recover
the numbers.

One subtlety: `expected_version` is a plain `int` sentinel derived from the
`ExpectedVersion` dataclass the caller passed to `append` (defined in
`eventsource.ports.positions`), not the `ExpectedVersion` object itself. Each
backend module keeps a small internal `_NO_STREAM_SENTINEL` /
`_STREAM_EXISTS_SENTINEL` mapping to preserve this field's historical int
shape: `ExpectedVersion.no_stream()` reports as `0`, `ExpectedVersion.stream_exists()`
reports as `-2`, and `ExpectedVersion.exact(n)` reports as `n`. When a
`no_stream`/`stream_exists` append conflicts, `expected_version` holds the
sentinel value, so `exc.expected_version == -2` means "the caller required an
existing stream", not "version negative two". `ExpectedVersion.any_()` disables
the check and therefore never produces this error.

`actual_version` is a real version number in every case: the count of events
already in the stream, `0` for a stream that does not exist.

`aggregate_id` is annotated `UUID` but not validated at runtime — it is stored
verbatim from the `StreamId` passed to `append`.

One attribute is *not* on the class but is worth knowing about: `__cause__`. On
the PostgreSQL and SQLite unique-constraint path the error is raised with
`raise ... from e`, so `exc.__cause__` holds the underlying `IntegrityError`.
On the pre-check path (and on `InMemoryEventStore` always) `__cause__` is
`None`. A non-`None` `__cause__` therefore distinguishes "lost a genuine race
with a concurrent writer" from "the version you asked for was already stale when
we read it".

### Raised by

Every raise site in the library is inside an event store's `append`.
Everything else in this list is a caller that lets the store's error propagate
unchanged — no layer wraps, re-raises, or translates it.

**`EventAppender.append`** — the version check itself, and the only origin.
All three shipped backends (`InMemoryEventStore`, `PostgreSQLEventStore`,
`SQLiteEventStore`) implement the same four-branch logic against
`current_version`, the number of events already stored for the stream:

| `expected.kind` | Conflict condition |
| --- | --- |
| `"any"` | never — check skipped |
| `"stream_exists"` | `current_version == 0` |
| `"no_stream"` | `current_version != 0` |
| `"exact"` | `current_version != expected.version` |

`InMemoryEventStore` performs the check under an `asyncio.Lock`, so the
pre-check is authoritative there.

`PostgreSQLEventStore` and `SQLiteEventStore` run the same read-then-check
inside the write transaction and have a *second* raise site: a unique-constraint
violation on the `(aggregate_id, aggregate_type, version)` index is caught and
translated into `OptimisticLockError`. That path is what catches a genuinely
concurrent writer that slipped past the read check; it re-queries the stream to
fill in `actual_version` and chains the database error with `raise ... from e`.
Correctness under concurrency rests on the constraint, not on the pre-check —
which is also why `exc.__cause__` is the reliable signal for distinguishing the
two, as described above.

**`AggregateRepository.save`** — propagates the store's error; it never
constructs one. The repository derives
`expected_version = aggregate.version - len(uncommitted_events)`, wraps it in
`ExpectedVersion.exact(expected_version)`, and passes that straight to
`append`, so a conflict here means another writer advanced the stream between
your `load` and your `save`. Two consequences worth knowing:

- A `save` on an aggregate with no uncommitted events returns early and cannot
  raise.
- The aggregate is *not* rolled back by the failure. `mark_events_as_committed`
  only runs on success, so the uncommitted events are still attached to the
  stale instance — which is exactly why the retry pattern below reloads rather
  than re-saving.

`AggregateRepository.load` and `load_or_create` never raise this error; they
only read.

**`SyncEventStoreAdapter.append`** — runs the wrapped async store's
`append` on the shared executor and re-raises whatever it produced, so
the same conflict surfaces identically to synchronous callers.

**`DualWriteEventStore.append`** — writes to the source store first and
lets its failures propagate, so a conflict on the *source* store reaches the
caller unchanged. The target write is best-effort and wrapped in
`except Exception`: a conflict against the migration target is logged and
recorded as a sync failure, never raised.

Note that the read-model backends (`readmodels/postgresql.py`,
`readmodels/sqlite.py`, `readmodels/in_memory.py`) also raise an
`OptimisticLockError` — but the one from `eventsource.ports.readmodels.exceptions`,
which is a different class. See
[Name collision](#name-collision-eventsourcereadmodelsexceptionsoptimisticlockerror).

### Handling: retry-on-conflict pattern

This is the one core exception that is *expected* under normal load: two commands
touched the same aggregate concurrently and one lost. The recovery is always the
same — discard the stale in-memory aggregate, reload it, re-apply the command
against fresh state, and save again.

The reload is not optional. `AggregateRepository.save` only calls
`mark_events_as_committed()` on success, so after a conflict the stale instance
still carries its uncommitted events and its unchanged `version`. Calling `save`
again on that same object recomputes the identical
`expected_version = aggregate.version - len(uncommitted_events)` and fails the
same way.

```python
import asyncio
import random

from eventsource import OptimisticLockError

MAX_ATTEMPTS = 3


async def ship_order(repository, order_id, tracking_number):
    for attempt in range(MAX_ATTEMPTS):
        order = await repository.load(order_id)
        order.ship(tracking_number=tracking_number)
        try:
            await repository.save(order)
            return order
        except OptimisticLockError as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            logger.info(
                "conflict on %s: expected v%s, actual v%s — retrying",
                exc.aggregate_id,
                exc.expected_version,
                exc.actual_version,
            )
            await asyncio.sleep(random.uniform(0, 0.05 * 2**attempt))
    raise AssertionError("unreachable")
```

Points worth keeping:

- **Put the load *and* the command inside the loop.** Only the `save` belongs in
  the `try`. Reloading gives you fresh state; re-running `ship` re-derives the
  decision from that state, which is the entire point of the retry. A loop that
  retries `save` alone spins forever.
- **`repository.load` raises `AggregateNotFoundError`, not this error.** If the
  aggregate may legitimately not exist, use `load_or_create` — it swallows
  `AggregateNotFoundError` and hands back a fresh instance — and keep that call
  inside the loop too.
- **The command must be safe to re-apply.** Events emitted onto the stale
  instance are discarded along with it, but side effects outside the aggregate
  (emails, payments, outbound HTTP) are not. Keep them out of the retried
  region, or move them behind the outbox.
- **Cap the attempts and back off.** An unbounded loop turns a hot aggregate
  into a livelock. A small bound with jittered sleep is usually enough;
  persistent conflict on one aggregate is an aggregate-boundary design problem,
  not a retry-count problem.
- **`exc.expected_version` and `exc.actual_version` are the useful telemetry.**
  A gap that grows across retries — actual moving several versions per attempt —
  says the aggregate is a write hotspot rather than an unlucky collision.
- **`exc.__cause__` distinguishes the two conflict paths.** `None` means the
  pre-check caught a version you already knew was stale; a chained
  `IntegrityError` means you lost a genuine race inside the write transaction.
  Both retry identically, but the ratio is worth a metric.

There is no purpose-built retry helper for command conflicts in the library. The
generic `retry_async` in `eventsource.application.subscriptions.retry` will work if — and
only if — the closure you hand it performs the reload itself:

```python
from eventsource import OptimisticLockError
from eventsource.application.subscriptions.retry import RetryConfig, retry_async


async def attempt():
    order = await repository.load(order_id)
    order.ship(tracking_number=tracking_number)
    await repository.save(order)
    return order


order = await retry_async(
    attempt,
    config=RetryConfig(max_retries=2),
    retryable_exceptions=(OptimisticLockError,),
    operation_name="ship_order",
)
```

Note the differences from the hand-written loop: you must pass
`retryable_exceptions` explicitly (the default `TRANSIENT_EXCEPTIONS` does not
include `OptimisticLockError`), and exhausting the retries raises `RetryError`
wrapping the last failure rather than the `OptimisticLockError` itself. The
retry policies in `eventsource.application.projections.retry` are for
projection delivery and do not apply to the command side at all.

If you deliberately do not want the version check — bulk import, replay into a
fresh store — pass `ExpectedVersion.any_()` to `append` rather than catching
and ignoring the error.
