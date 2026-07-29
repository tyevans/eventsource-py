# Projections API Reference

Technical reference for the `eventsource.projections` package: the projection base
classes, the checkpoint/retry/DLQ pipeline they build on, the retry policies, and the
registries and coordinators that drive them.

The package is organized into five source modules:

| Module | Contains |
| --- | --- |
| `eventsource.projections.base` | `Projection`, `SyncProjection`, `EventHandlerBase`, `CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection`, `TenantFilter` |
| `eventsource.projections.coordinator` | `ProjectionRegistry`, `ProjectionCoordinator`, `SubscriberRegistry` |
| `eventsource.projections.retry` | `RetryPolicy`, `ExponentialBackoffRetryPolicy`, `NoRetryPolicy`, `FilteredRetryPolicy`, `DEFAULT_RETRY_POLICY` |
| `eventsource.projections.checkpoint_manager` | `ProjectionCheckpointManager` |
| `eventsource.projections.dlq_manager` | `ProjectionDLQManager` |
| `eventsource.projections.protocols` | `AsyncEventHandler` |

Only the first two modules plus the protocols and the `@handles` helpers are re-exported
from the `eventsource.projections` barrel; the retry policies and the two managers must
be imported from their own modules. The `handles`, `get_handled_event_type`, and
`is_event_handler` names re-exported here are aliases for the canonical definitions in
`eventsource.handlers`, kept for backward compatibility — new code should import them
from `eventsource.handlers`. The `EventHandler`, `SyncEventHandler`, and
`EventSubscriber` names are likewise re-exports from `eventsource.protocols`.

The class hierarchy is linear: `CheckpointTrackingProjection` extends `Projection` and
adds checkpointing, retry, and dead-letter handling; `DeclarativeProjection` extends
that with `@handles`-based dispatch and tenant filtering; `DatabaseProjection` extends
`DeclarativeProjection` with transactional handlers that receive a database connection.
Each level narrows what a subclass has to implement, at the cost of a fixed set of
constructor parameters — the constructors are not uniform down the chain, and the
asymmetries are documented explicitly in the sections for each class.

Everything below describes the current source. Members with a leading underscore
(`_process_event`, `_should_process_event`, `_execute_in_transaction`, and similar) are
documented where subclasses are expected to override or rely on them; they are subclass
hooks, not stable public API for application code.

## Overview

A projection in this package is an object that consumes `DomainEvent` instances one at
a time and updates a read model. `Projection` and `SyncProjection` define nothing more
than that: an abstract `handle(event)` plus an abstract `reset()`. Everything else in
the package exists to supply the machinery around those two methods.

`CheckpointTrackingProjection` is where that machinery lives, and it is the base every
practical projection inherits from. It subclasses `EventSubscriber` (so it must also
declare `subscribed_to()`) and implements `handle()` as a fixed pipeline:

1. Open a `eventsource.projection.handle` span (a no-op unless tracing is enabled).
2. Loop for `retry_policy.max_retries + 1` attempts, calling the subclass hook
   `_process_event(event)` on each.
3. On success, record the event in the checkpoint via `ProjectionCheckpointManager` and
   return.
4. On failure, consult `retry_policy.should_retry(attempt, exception)`. If it says
   retry, `asyncio.sleep(retry_policy.get_backoff(attempt))` and try again. If not,
   write the event to the dead-letter queue via `ProjectionDLQManager` and **re-raise**
   the original exception.

Two consequences of step 4 are worth stating up front: a permanently failing event is
sent to the DLQ *and* propagated to the caller, and the checkpoint is not advanced for
it. The projection does not silently skip past poison events on its own.

Subclasses choose their level by how much of `_process_event` they want to write:

| Base class | You implement | You get |
| --- | --- | --- |
| `CheckpointTrackingProjection` | `subscribed_to()`, `_process_event()`, optionally `_truncate_read_models()` | checkpointing, retry, DLQ, tracing hooks |
| `DeclarativeProjection` | `@handles(EventType)` methods | the above, plus auto-generated `subscribed_to()`, dispatch through `HandlerRegistry`, and tenant filtering |
| `DatabaseProjection` | `@handles` methods taking `(self, conn, event)` | the above, plus a fresh SQLAlchemy session and transaction per attempt, committed on success and rolled back on error |

`DatabaseProjection` reimplements the retry loop rather than reusing the parent's, so
that each attempt runs `_execute_in_transaction()` against a brand-new session — a
PostgreSQL transaction is unusable after any error, so retrying inside the failed
transaction would fail unconditionally.

Both `DeclarativeProjection` and `DatabaseProjection` narrow the constructor: they
accept `checkpoint_repo`, `dlq_repo`, `enable_tracing`, and (keyword-only)
`tenant_filter`, but **not** `retry_policy` or `tracer`, and they do not forward those
to `CheckpointTrackingProjection.__init__`. Any projection built on those two classes
therefore always runs the inline default policy — `ExponentialBackoffRetryPolicy` with
`max_retries=2` (three total attempts), `initial_delay=2.0`, `exponential_base=2.0`,
`jitter=0.1` — regardless of what is in `eventsource.projections.retry`. This is called
out again under each class because it is the most common surprise in the package.

Storage defaults are in-memory. Omitting `checkpoint_repo` yields an
`InMemoryCheckpointRepository` and omitting `dlq_repo` yields an
`InMemoryDLQRepository`, so checkpoints and dead-lettered events vanish with the
process unless durable repositories are passed in. Tracing is likewise off by default
(`enable_tracing=False`), a deliberate choice given how frequently projections run.

Nothing in these classes drives itself. Something else must call `handle()` — a
subscription runner, or the `ProjectionCoordinator`/`ProjectionRegistry` pair in
`eventsource.projections.coordinator`, which fan a single event out to the registered
projections that subscribe to its type.

## Import Surface (`eventsource.projections`)

The barrel module `eventsource/projections/__init__.py` re-exports 15 names. This is the
complete `__all__`, grouped as the source groups it:

| Group | Names | Defined in |
| --- | --- | --- |
| Base classes | `Projection`, `SyncProjection`, `EventHandlerBase`, `CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection` | `eventsource.projections.base` |
| Type aliases | `TenantFilter` | `eventsource.projections.base` |
| Decorators | `handles`, `get_handled_event_type`, `is_event_handler` | `eventsource.handlers` (re-export) |
| Coordinators and registries | `ProjectionRegistry`, `ProjectionCoordinator`, `SubscriberRegistry` | `eventsource.projections.coordinator` |
| Protocols | `EventHandler`, `SyncEventHandler`, `EventSubscriber` | `eventsource.protocols` (re-export) |
| Protocols | `AsyncEventHandler` | `eventsource.projections.protocols`, itself a re-export of `eventsource.protocols` |

```python
from eventsource.projections import (
    CheckpointTrackingProjection,
    DatabaseProjection,
    DeclarativeProjection,
    ProjectionCoordinator,
    ProjectionRegistry,
    TenantFilter,
    handles,
)
```

### What is *not* in the barrel

Three modules in the package are not re-exported at all and must be imported by their
full path:

```python
from eventsource.projections.retry import (
    DEFAULT_RETRY_POLICY,
    ExponentialBackoffRetryPolicy,
    FilteredRetryPolicy,
    NoRetryPolicy,
    RetryPolicy,
)
from eventsource.projections.checkpoint_manager import ProjectionCheckpointManager
from eventsource.projections.dlq_manager import ProjectionDLQManager
```

`from eventsource.projections import ExponentialBackoffRetryPolicy` raises
`ImportError`. Since `retry_policy` is the one constructor parameter that materially
changes failure behavior, the deep import is the normal case, not an edge case.

### Relationship to the top-level `eventsource` package

The top-level `eventsource/__init__.py` re-exports a strict subset: `Projection`,
`CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection`, and
`handles`. It does **not** export `SyncProjection`, `EventHandlerBase`, `TenantFilter`,
`get_handled_event_type`, `is_event_handler`, or any of the coordinator classes — those
require `eventsource.projections`. The protocols (`EventHandler`, `SyncEventHandler`,
`EventSubscriber`, `AsyncEventHandler`) are available from both places, plus
`eventsource.protocols`, because all three paths resolve to the same objects.

### Aliases, not duplicates

`handles`, `get_handled_event_type`, and `is_event_handler` are bound directly from
`eventsource.handlers`; `EventHandler`, `SyncEventHandler`, and `EventSubscriber` from
`eventsource.protocols`; and `eventsource.projections.protocols` contains nothing but a
re-export of `AsyncEventHandler` from `eventsource.protocols`. Identity comparisons and
`isinstance`/`issubclass` checks behave identically whichever path you import through.
The projections-package copies exist for backward compatibility. New code should prefer
the canonical modules: `eventsource.handlers` for the decorator helpers and
`eventsource.protocols` for the protocols.

## Abstract Base Classes

Three ABCs in `eventsource.projections.base` define the minimal contracts. They contain
no implementation at all — every method is `@abstractmethod` with a `pass` body — so
instantiating any of them, or a subclass that leaves a method unimplemented, raises
`TypeError` from `abc`.

They are also unrelated to each other: `Projection`, `SyncProjection`, and
`EventHandlerBase` each inherit directly from `ABC`, share no common base, and are not
`Protocol`s, so structural compatibility is not enough — a class must explicitly
subclass to satisfy an `isinstance` check.

| Class | Async | Methods | Registered with |
| --- | --- | --- | --- |
| `Projection` | yes | `handle(event)`, `reset()` | `ProjectionRegistry.register_projection()` |
| `SyncProjection` | no | `handle(event)`, `reset()` | nothing — no registry accepts it |
| `EventHandlerBase` | `handle` only | `can_handle(event)`, `handle(event)` | `ProjectionRegistry.register_handler()` |

### `Projection`

```python
class Projection(ABC):
    @abstractmethod
    async def handle(self, event: DomainEvent) -> None: ...
    @abstractmethod
    async def reset(self) -> None: ...
```

The root contract for a read-model builder: consume one event, and be able to throw the
read model away so it can be rebuilt from the log. `handle()` receives every event the
caller routes to it — plain `Projection` has no subscription declaration, so filtering
by type is the subclass's job, conventionally with `isinstance`.

`reset()` is not decoration. `ProjectionRegistry.reset_all()` awaits it on every
registered projection, and `ProjectionCoordinator.rebuild_projection()` awaits it before
replaying events into the projection — so an implementation that leaves stale rows behind
produces a corrupt rebuild rather than a clean one. It must clear everything `handle()`
writes.

Note what `Projection` is *not*. It is not an `EventSubscriber`, has no
`subscribed_to()`, no checkpointing, no retry, and no dead-letter handling. It is the
type `ProjectionRegistry` stores and the type `ProjectionCoordinator` fans events out
to, and it is the right base for a projection you drive yourself. Anything that needs
to resume after a restart should start from `CheckpointTrackingProjection` instead,
which subclasses `EventSubscriber` rather than `Projection` — the two hierarchies are
separate, and a `CheckpointTrackingProjection` is therefore **not** an instance of
`Projection`. This matters when registering: `register_projection()` is annotated
`Projection`, so passing a checkpoint-tracking projection is a type error even though
the duck-typed `handle()`/`reset()` calls would work at runtime.

### `SyncProjection`

```python
class SyncProjection(ABC):
    @abstractmethod
    def handle(self, event: DomainEvent) -> None: ...
    @abstractmethod
    def reset(self) -> None: ...
```

The same two methods without `async`, for projections that do no I/O — in-memory
counters, test doubles, and anything driven from synchronous code. It inherits from
`ABC` directly, not from `Projection`, so the two are unrelated types.

Both methods are abstract with `pass` bodies. Instantiating `SyncProjection` itself, or
a subclass that implements `handle()` but omits `reset()` (or vice versa), raises
`TypeError` from `abc`.

```python
from eventsource.projections import SyncProjection

class OrderCountProjection(SyncProjection):
    def __init__(self) -> None:
        self.count = 0

    def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self.count += 1

    def reset(self) -> None:
        self.count = 0
```

As with `Projection`, there is no `subscribed_to()` and no dispatch on event type —
`handle()` sees whatever the caller passes it, so filter with `isinstance` yourself.

`SyncProjection` is a standalone contract with no consumers inside the library. No
registry, coordinator, or subscription runner accepts one; nothing bridges it to the
async `Projection` path, and `await projection.handle(event)` fails because `handle()`
returns `None` rather than an awaitable. (`SyncEventStoreAdapter` in `eventsource.sync`
bridges in the opposite direction — a sync caller over an async store — and has nothing
to do with this class.) If you need a synchronous projection driven by async
infrastructure, wrap it yourself: have an async `Projection.handle()` call the sync one,
using `asyncio.to_thread` if the work could block.

It is also not exported from the top-level `eventsource` package; import it from
`eventsource.projections`.

### `EventHandlerBase`

```python
class EventHandlerBase(ABC):
    @abstractmethod
    def can_handle(self, event: DomainEvent) -> bool: ...
    @abstractmethod
    async def handle(self, event: DomainEvent) -> None: ...
```

A reactive handler rather than a read-model builder: send a notification, kick off a
workflow, call an external system. Like the other two ABCs it inherits from `ABC`
directly and both methods are abstract with `pass` bodies, so `EventHandlerBase()` and
any subclass missing either method raise `TypeError`.

It has no `reset()` — side effects on other systems are not rewindable, so there is
nothing to clear. That absence is visible in the registry too: `reset_all()` iterates
`self._projections` only, so registered handlers are untouched by a projection rebuild.

The distinguishing method is `can_handle()`, a synchronous predicate the caller checks
*before* dispatching. `ProjectionRegistry` keeps handlers in `self._handlers`, a list
separate from `self._projections`, with its own `register_handler()`,
`unregister_handler()`, `handlers` property, and `get_handler_count()`. Interest is
declared imperatively — any `bool` expression over the event instance — rather than as a
list of types the way `EventSubscriber.subscribed_to()` does, so filters can depend on
the event payload, not just the event class:

```python
from eventsource.projections import EventHandlerBase, ProjectionRegistry

class LargeOrderAlertHandler(EventHandlerBase):
    def can_handle(self, event: DomainEvent) -> bool:
        return isinstance(event, OrderPlaced) and event.total > 10_000

    async def handle(self, event: DomainEvent) -> None:
        await alerting.notify(f"Large order: {event.aggregate_id}")

registry = ProjectionRegistry()
registry.register_handler(LargeOrderAlertHandler())
```

`ProjectionRegistry.dispatch()` builds one coroutine per registered projection, then one
per handler whose `can_handle()` returned `True`, and awaits them all under a single
`asyncio.gather(..., return_exceptions=True)`. Two behaviors follow:

- **Failures inside `handle()` are logged, not raised.** An exception from a handler is
  captured by `gather` and logged with the handler class name; other projections and
  handlers still run, and `dispatch()` returns normally. Unlike
  `CheckpointTrackingProjection`, there is no retry, no dead-letter queue, and no
  checkpoint — if the handler's side effect must survive failure, it has to arrange that
  itself.
- **Failures inside `can_handle()` are not contained.** It is called synchronously while
  the task list is being assembled, before `gather` is reached, so a raising
  `can_handle()` propagates straight out of `dispatch()` and prevents the remaining
  projections and handlers from running at all.

So `can_handle()` must not be async and must not raise for event types it does not
recognize — it is called for every event reaching the registry, including ones the
handler knows nothing about. Guard with `isinstance` before touching any type-specific
attribute, as in the example above.

`EventHandlerBase` is not exported from the top-level `eventsource` package; import it
from `eventsource.projections`. It is also unrelated to the `EventHandler` and
`AsyncEventHandler` names in `eventsource.protocols`, despite the similar spelling —
those describe bus-level callables and subscribers, not registry-dispatched handlers.

## `CheckpointTrackingProjection`

```python
class CheckpointTrackingProjection(EventSubscriber, ABC):
    def __init__(
        self,
        checkpoint_repo: CheckpointRepository | None = None,
        dlq_repo: DLQRepository | None = None,
        retry_policy: RetryPolicy | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None: ...
```

Defined in `eventsource.projections.base`; exported from both `eventsource` and
`eventsource.projections`. This is the base every durable projection in the library is
built on — `DeclarativeProjection` and `DatabaseProjection` both descend from it — and
the only one that supplies checkpointing, retry, dead-lettering, and tracing.

It inherits from `EventSubscriber` (the ABC in `eventsource.protocols`), **not** from
`Projection`. The two hierarchies are disjoint, so `isinstance(p, Projection)` is
`False` for a checkpoint-tracking projection and `ProjectionRegistry.register_projection()`,
annotated `Projection`, rejects one under a type checker even though the runtime
`handle()`/`reset()` calls would succeed.

### What a subclass must supply

`EventSubscriber` contributes abstract `subscribed_to()` and `handle()`;
`CheckpointTrackingProjection` implements `handle()` itself and adds abstract
`_process_event()`. A concrete subclass therefore implements exactly two methods:

| Member | Required | Purpose |
| --- | --- | --- |
| `subscribed_to() -> list[type[DomainEvent]]` | yes | event types this projection consumes; also feeds `get_lag_metrics()` |
| `_process_event(event) -> None` | yes | the projection logic; called once per attempt |
| `_truncate_read_models() -> None` | no | clear read-model state on `reset()`; base implementation is a no-op |

Leaving either abstract method unimplemented makes the class uninstantiable
(`TypeError` from `abc`).

```python
from eventsource.projections import CheckpointTrackingProjection

class OrderProjection(CheckpointTrackingProjection):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def _process_event(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self._create_row(event)

    async def _truncate_read_models(self) -> None:
        await self._db.execute("DELETE FROM order_summaries")
```

Note that `_process_event()` takes `(self, event)`. The class docstring's example shows
a `(self, conn, event)` signature, which is wrong for this class — the connection
parameter belongs to `@handles` handler methods on `DatabaseProjection`, not to the
`_process_event` hook. Same for `_truncate_read_models()`: no `conn` is passed.

### What it provides

- **`handle(event)`** — a fixed pipeline, not an override point: open a span, attempt
  `_process_event()` up to `retry_policy.max_retries + 1` times, update the checkpoint on
  success, dead-letter and re-raise on final failure. The retry loop lives in
  `_handle_with_retry()`, which `DatabaseProjection` overrides so each attempt gets a
  fresh transaction.
- **Checkpointing** via a `ProjectionCheckpointManager` keyed on the projection name,
  advanced only after `_process_event()` returns without raising.
- **Dead-lettering** via a `ProjectionDLQManager`, invoked when the policy declines a
  further retry.
- **`get_checkpoint()`, `get_lag_metrics()`, `reset()`** — read and rewind checkpoint
  state.
- **Tracing** through a composed `Tracer` (`self._tracer`), off unless `enable_tracing=True`
  or an explicit `tracer` is passed.

Nothing here starts itself. Some caller — a subscription runner, or your own replay loop
— must invoke `handle()` per event.

### Naming and identity

`self._projection_name` is set to `self.__class__.__name__` and exposed read-only as the
`projection_name` property. It is the checkpoint key and the DLQ partition key, so
**renaming the class orphans its checkpoint**: the new name has no checkpoint row, and
the projection restarts from the beginning of whatever the runner feeds it. There is no
constructor parameter to override the name; if you need a stable identifier across
renames, override the `projection_name` property and set `_projection_name` yourself
after calling `super().__init__()`.

Two projections whose classes share a name — for example the same class name in two
modules — share a checkpoint key and will corrupt each other's position.

### Attributes set by the constructor

| Attribute | Value |
| --- | --- |
| `_projection_name` | `self.__class__.__name__` |
| `_tracer` | the `tracer` argument, else `create_tracer(__name__, enable_tracing)` |
| `_enable_tracing` | `self._tracer.enabled` — the *effective* state, which is `False` when tracing was requested but OpenTelemetry is not installed |
| `_checkpoint_manager` | `ProjectionCheckpointManager(projection_name, checkpoint_repo or InMemoryCheckpointRepository(), enable_tracing=enable_tracing)` |
| `_dlq_manager` | `ProjectionDLQManager(projection_name, dlq_repo or InMemoryDLQRepository(), enable_tracing=enable_tracing)` |
| `_retry_policy` | the `retry_policy` argument, else the inline default described below |
| `_checkpoint_repo` / `_dlq_repo` | the repositories held by the two managers, re-exposed for convenience |

One asymmetry to be aware of: an explicitly supplied `tracer` is used for the
projection's own spans but is **not** passed to the two managers — they are constructed
with the raw `enable_tracing` flag and build their own tracers. Passing
`tracer=my_tracer, enable_tracing=False` therefore leaves checkpoint and DLQ spans
disabled while projection spans are enabled.

### Defaults are in-memory

Omitting `checkpoint_repo` yields an `InMemoryCheckpointRepository`; omitting `dlq_repo`
yields an `InMemoryDLQRepository`. Both are per-instance and per-process: checkpoints do
not survive a restart, so the projection reprocesses from wherever the caller starts it,
and dead-lettered events are lost entirely. Fine for tests; pass durable repositories in
production.

### Constructor

```python
CheckpointTrackingProjection(
    checkpoint_repo: CheckpointRepository | None = None,
    dlq_repo: DLQRepository | None = None,
    retry_policy: RetryPolicy | None = None,
    tracer: Tracer | None = None,
    enable_tracing: bool = False,
) -> None
```

All five parameters are optional and positional-or-keyword, so
`MyProjection()` is valid and produces a fully functional, entirely in-memory
projection. This is the widest constructor in the hierarchy — the two subclasses
accept strictly fewer parameters.

| Parameter | Type | Default | Effect |
| --- | --- | --- | --- |
| `checkpoint_repo` | `CheckpointRepository \| None` | `None` | Backing store for the projection's checkpoint. `None` → a fresh `InMemoryCheckpointRepository`. |
| `dlq_repo` | `DLQRepository \| None` | `None` | Backing store for dead-lettered events. `None` → a fresh `InMemoryDLQRepository`. |
| `retry_policy` | `RetryPolicy \| None` | `None` | Governs attempt count and backoff. `None` → the inline `ExponentialBackoffRetryPolicy` described in the next section (**not** `DEFAULT_RETRY_POLICY`). |
| `tracer` | `Tracer \| None` | `None` | Custom tracer for the projection's own spans. When given, `enable_tracing` is ignored for those spans. |
| `enable_tracing` | `bool` | `False` | When `True` and OpenTelemetry is installed, builds a live tracer via `create_tracer(__name__, enable_tracing)`. |

Both repository parameters take the interfaces from
`eventsource.repositories.checkpoint` and `eventsource.repositories.dlq`; any
implementation of those (PostgreSQL, SQLite, in-memory) is accepted. The constructor
never validates them beyond typing, and never touches the store — no schema check, no
connection attempt happens until the first `handle()` call.

```python
from eventsource.projections import CheckpointTrackingProjection
from eventsource.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.repositories.checkpoint import PostgresCheckpointRepository
from eventsource.repositories.dlq import PostgresDLQRepository
from eventsource.subscriptions.retry import RetryConfig

projection = OrderProjection(
    checkpoint_repo=PostgresCheckpointRepository(pool),
    dlq_repo=PostgresDLQRepository(pool),
    retry_policy=ExponentialBackoffRetryPolicy(RetryConfig(max_retries=5)),
    enable_tracing=True,
)
```

Note that `ExponentialBackoffRetryPolicy` is configured with a `RetryConfig` from
`eventsource.subscriptions.retry`, not with loose keyword arguments — the retry
configuration type is shared with the subscription machinery.

#### Interactions worth knowing

- **`tracer` wins over `enable_tracing` — but only for projection spans.** `self._tracer`
  is `tracer or create_tracer(__name__, enable_tracing)`, while the checkpoint and DLQ
  managers are constructed with the raw `enable_tracing` value. Passing a `tracer` with
  `enable_tracing=False` gives you projection spans without checkpoint or DLQ spans.
- **`_enable_tracing` reflects reality, not the request.** It is assigned
  `self._tracer.enabled`, so it stays `False` when `enable_tracing=True` was passed but
  the optional OpenTelemetry dependency is missing. Read this attribute, not the
  argument, to know whether spans are actually being emitted.
- **No `projection_name` parameter.** The checkpoint and DLQ keys come from
  `self.__class__.__name__`; see *Naming and identity* above for the consequences.
- **Subclasses must call it.** `DeclarativeProjection` and `DatabaseProjection` both
  invoke `super().__init__()`, but a hand-written subclass that overrides `__init__`
  without chaining leaves `_checkpoint_manager`, `_dlq_manager`, and `_retry_policy`
  unset, and `handle()` fails with `AttributeError` on the first event.
