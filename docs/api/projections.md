# Projections API Reference

Technical reference for the `eventsource.application.projections` package: the projection
base classes, the checkpoint/retry/DLQ pipeline they build on, the retry policies, and
the registries and coordinators that drive them. `DatabaseProjection` is documented here
too, even though it lives in `eventsource.adapters.sql` rather than this package — its
constructor takes a SQLAlchemy `async_sessionmaker`, which makes it an adapter (ADR
0024), but it subclasses `DeclarativeProjection` and is part of the same class hierarchy
a reader of this page needs.

The package is organized into five source modules:

| Module | Contains |
| --- | --- |
| `eventsource.application.projections.base` | `Projection`, `SyncProjection`, `EventHandlerBase`, `CheckpointTrackingProjection`, `DeclarativeProjection`, `TenantFilter` |
| `eventsource.application.projections.store` | `StoreProjection`, `ProjectionOptions` |
| `eventsource.application.projections.coordinator` | `ProjectionRegistry`, `ProjectionCoordinator`, `SubscriberRegistry` |
| `eventsource.application.projections.retry` | `RetryPolicy`, `ExponentialBackoffRetryPolicy`, `NoRetryPolicy`, `FilteredRetryPolicy`, `DEFAULT_RETRY_POLICY` |
| `eventsource.application.projections.checkpoints` | `record_checkpoint`, `read_checkpoint`, `lag_metrics_dict`, `reset_checkpoint` |
| `eventsource.application.projections.dlq` | `send_to_dlq`, `read_failed_events` |
| `eventsource.application.projections.replay` | `replay`, `ReplayReport`, `ReplayFailure`, `ReplayFailedError` |
| `eventsource.ports.handlers` | `AsyncEventHandler` |

`DatabaseProjection` itself lives in `eventsource.adapters.sql.projection`.

The barrel `eventsource.application.projections` re-exports `base`, `coordinator`,
`checkpoints`, and `dlq` — everything except `DatabaseProjection` (which lives in
`eventsource.adapters.sql.projection`, not this package) and the retry policies, which
must still be imported from `eventsource.application.projections.retry`. The `handles`,
`get_handled_event_type`, and `is_event_handler` names re-exported here are aliases for
the canonical definitions in `eventsource.domain.decorators`, kept for backward
compatibility — new code should import them from `eventsource.domain.decorators`. The
`EventHandler`,
`SyncEventHandler`, and `EventSubscriber` names are likewise re-exports from
`eventsource.ports.handlers`.

The class hierarchy is linear: `CheckpointTrackingProjection` extends `Projection` and
adds checkpointing, retry, and dead-letter handling; `DeclarativeProjection` extends
that with `@handles`-based dispatch and tenant filtering; `DatabaseProjection` extends
`DeclarativeProjection` with transactional handlers that receive a database connection.
Each level narrows what a subclass has to implement while widening the constructor: a
subclass adds parameters (`tenant_filter`, `session_factory`, `model_class`) and never
drops one its parent accepts.

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
3. On success, record the event in the checkpoint via `record_checkpoint()` (skipped
   entirely when `checkpoint_repo` is `None`) and return.
4. On failure, consult `retry_policy.should_retry(attempt, exception)`. If it says
   retry, `asyncio.sleep(retry_policy.get_backoff(attempt))` and try again. If not,
   write the event to the dead-letter queue via `send_to_dlq()` (skipped when `dlq_repo`
   is `None`) and **re-raise** the original exception.

Two consequences of step 4 are worth stating up front: a permanently failing event is
sent to the DLQ *and* propagated to the caller, and the checkpoint is not advanced for
it. The projection does not silently skip past poison events on its own.

Subclasses choose their level by how much of `_process_event` they want to write:

| Base class | You implement | You get |
| --- | --- | --- |
| `CheckpointTrackingProjection` | `subscribed_to()`, `_process_event()`, optionally `_truncate_read_models()` | checkpointing, retry, DLQ, tracing hooks |
| `DeclarativeProjection` | `@handles(EventType)` methods | the above, plus auto-generated `subscribed_to()`, dispatch through `HandlerRegistry`, and tenant filtering |
| `StoreProjection[TStore]` | `@handles` methods writing to `self._store` | the above, with one store held for you and the whole parent constructor forwarded |
| `DatabaseProjection` | `@handles` methods taking `(self, conn, event)` | the above, plus a fresh SQLAlchemy session and transaction per attempt, committed on success and rolled back on error |

`DatabaseProjection` reimplements the retry loop rather than reusing the parent's, so
that each attempt runs `_execute_in_transaction()` against a brand-new session — a
PostgreSQL transaction is unusable after any error, so retrying inside the failed
transaction would fail unconditionally.

Every subclass constructor accepts at least what its parent does. `DeclarativeProjection`,
`DatabaseProjection`, and `ReadModelProjection` each take `checkpoint_repo`, `dlq_repo`,
`enable_tracing`, and — keyword-only — `retry_policy`, `tracer`, and `tenant_filter`,
forwarding all of them up to `CheckpointTrackingProjection.__init__`. Omitting
`retry_policy` still means the inline default policy — `ExponentialBackoffRetryPolicy`
with `max_retries=2` (three total attempts), `initial_delay=2.0`, `exponential_base=2.0`,
`jitter=0.1` — which is not `DEFAULT_RETRY_POLICY` from
`eventsource.application.projections.retry`.

This is a contract, not just an observed property: **every projection base's
constructor accepts at least what its parent's accepts, permanently, in every
release.** A parameter may be added; one is never removed. A subclass author
can therefore write a constructor without tracking which release introduced
which parameter — and one forwarding `**options` names no parameter at all, so
the only version floor it needs is the one for the base class it subclasses
(ADR 0055).

That superset property is enforced by
`tests/unit/application/projections/test_constructor_widening.py`: these constructors
restate the same parameter list rather than forwarding `**kwargs`, and each one dropping
a parameter silently is exactly what the test exists to catch. `StoreProjection` is the
one that does not restate it — it forwards `**options: Unpack[ProjectionOptions]`, and
the same test expands that TypedDict's keys to check it by the same rule (ADR 0055).

Storage defaults are disabled, not in-memory (ADR 0024). Omitting `checkpoint_repo`
means no checkpoint is ever written — `get_checkpoint()` / `get_lag_metrics()` return
`None` — and omitting `dlq_repo` means a permanently failed event is logged at
`critical` and re-raised with no DLQ write attempted. Pass
`InMemoryCheckpointRepository()` / `InMemoryDLQRepository()` (from `eventsource`)
explicitly for the old vanish-on-restart behavior; it is suitable for tests, not for
production, which is exactly why it is no longer the default. Tracing is likewise off
by default (`enable_tracing=False`), a deliberate choice given how frequently
projections run.

Nothing in these classes drives itself. Something else must call `handle()` — a
subscription runner, or the `ProjectionCoordinator`/`ProjectionRegistry` pair in
`eventsource.application.projections.coordinator`, which fan a single event out to the
registered projections that subscribe to its type.

To *rebuild* a projection from the log rather than follow it live, use
[`replay()`](#rebuilding-a-projection-replay). `ProjectionCoordinator.rebuild_projection`
takes the events as a list the caller has already read; a live subscription runner polls
forever and stops on a failure, which is the wrong shape for a foreground rebuild —
`replay()` owns the read loop itself and records a failure without stopping instead.

## Import Surface (`eventsource.application.projections`)

The barrel module `eventsource/application/projections/__init__.py` re-exports 27 names.
This is the complete `__all__`, grouped as the source groups it:

| Group | Names | Defined in |
| --- | --- | --- |
| Base classes | `Projection`, `SyncProjection`, `EventHandlerBase`, `CheckpointTrackingProjection`, `DeclarativeProjection` | `eventsource.application.projections.base` |
| Store projections | `StoreProjection`, `ProjectionOptions` | `eventsource.application.projections.store` |
| Type aliases | `TenantFilter` | `eventsource.application.projections.base` |
| Decorators | `handles`, `get_handled_event_type`, `is_event_handler` | `eventsource.domain.decorators` (re-export) |
| Coordinators and registries | `ProjectionRegistry`, `ProjectionCoordinator`, `SubscriberRegistry` | `eventsource.application.projections.coordinator` |
| Checkpoint functions | `record_checkpoint`, `read_checkpoint`, `lag_metrics_dict`, `reset_checkpoint` | `eventsource.application.projections.checkpoints` |
| DLQ functions | `send_to_dlq`, `read_failed_events` | `eventsource.application.projections.dlq` |
| Replay | `replay`, `ReplayReport`, `ReplayFailure`, `ReplayFailedError` | `eventsource.application.projections.replay` |
| Protocols | `EventHandler`, `SyncEventHandler`, `EventSubscriber` | `eventsource.ports.handlers` (re-export) |

`DatabaseProjection` is **not** in this barrel — it lives in
`eventsource.adapters.sql.projection` because its constructor takes a SQLAlchemy
`async_sessionmaker`.

```python
from eventsource.application.projections import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    ProjectionCoordinator,
    ProjectionRegistry,
    TenantFilter,
    handles,
)
from eventsource.adapters.sql import DatabaseProjection
```

### What is *not* in the barrel

The retry policies are not re-exported at all and must be imported by their
full path:

```python
from eventsource.application.projections.retry import (
    DEFAULT_RETRY_POLICY,
    ExponentialBackoffRetryPolicy,
    FilteredRetryPolicy,
    NoRetryPolicy,
    RetryPolicy,
)
```

`from eventsource.application.projections import ExponentialBackoffRetryPolicy` raises
`ImportError`. Since `retry_policy` is the one constructor parameter that materially
changes failure behavior, the deep import is the normal case, not an edge case.

### Relationship to the top-level `eventsource` package

The top-level `eventsource/__init__.py` re-exports a strict subset: `Projection`,
`CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection`,
`handles`, and the four replay names (`replay`, `ReplayReport`, `ReplayFailure`,
`ReplayFailedError`). It does **not** export `SyncProjection`, `EventHandlerBase`, `TenantFilter`,
`get_handled_event_type`, `is_event_handler`, or any of the coordinator classes, the
checkpoint functions, or the DLQ functions — those require
`eventsource.application.projections`. The protocols (`EventHandler`, `SyncEventHandler`,
`EventSubscriber`) are available from both places, plus `eventsource.ports.handlers`,
because all three paths resolve to the same objects.

### Aliases, not duplicates

`handles`, `get_handled_event_type`, and `is_event_handler` are bound directly from
`eventsource.domain.decorators`; `EventHandler`, `SyncEventHandler`, and `EventSubscriber` from
`eventsource.ports.handlers`. Identity comparisons and `isinstance`/`issubclass` checks
behave identically whichever path you import through. The
`application.projections`-package copies exist for backward compatibility. New code
should prefer the canonical modules: `eventsource.domain.decorators` for the decorator helpers and
`eventsource.ports.handlers` for the protocols.

## Abstract Base Classes

Three ABCs in `eventsource.application.projections.base` define the minimal contracts. They contain
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
from eventsource.application.projections import SyncProjection

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
returns `None` rather than an awaitable. (`SyncEventStoreAdapter` in `eventsource.adapters.sync`
bridges in the opposite direction — a sync caller over an async store — and has nothing
to do with this class.) If you need a synchronous projection driven by async
infrastructure, wrap it yourself: have an async `Projection.handle()` call the sync one,
using `asyncio.to_thread` if the work could block.

It is also not exported from the top-level `eventsource` package; import it from
`eventsource.application.projections`.

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
from eventsource.application.projections import EventHandlerBase, ProjectionRegistry

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
from `eventsource.application.projections`. It is also unrelated to the `EventHandler` and
`AsyncEventHandler` names in `eventsource.ports.handlers`, despite the similar spelling —
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

Defined in `eventsource.application.projections.base`; exported from both `eventsource` and
`eventsource.application.projections`. This is the base every durable projection in the library is
built on — `DeclarativeProjection` and `DatabaseProjection` both descend from it — and
the only one that supplies checkpointing, retry, dead-lettering, and tracing.

It inherits from `EventSubscriber` (the ABC in `eventsource.ports.handlers`), **not** from
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
from eventsource.application.projections import CheckpointTrackingProjection

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
- **Checkpointing** via `record_checkpoint()`, called against `self._checkpoint_repo`
  and keyed on the projection name, advanced only after `_process_event()` returns
  without raising -- skipped entirely when `checkpoint_repo` was `None`.
- **Dead-lettering** via `send_to_dlq()`, called against `self._dlq_repo`, invoked when
  the policy declines a further retry -- skipped when `dlq_repo` was `None`.
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
| `_checkpoint_repo` | the `checkpoint_repo` argument, unchanged — `None` means checkpoint tracking is disabled |
| `_dlq_repo` | the `dlq_repo` argument, unchanged — `None` means DLQ capture is disabled |
| `_retry_policy` | the `retry_policy` argument, else the inline default described below |

There is no wrapper object here. `record_checkpoint()` and `send_to_dlq()` are called
directly against `self._checkpoint_repo` / `self._dlq_repo` (and `self._tracer`) at the
call sites in `_handle_with_retry()`, guarded by an `is not None` check — the functions
themselves take the repository and tracer as explicit parameters rather than holding
them (ADR 0024). One consequence of that: there is no separate tracer for checkpoint and
DLQ spans the way the old manager objects had — every span, whichever function opens it,
uses `self._tracer`, so `tracer=my_tracer` covers projection, checkpoint, and DLQ spans
uniformly.

### Defaults disable the concern

Omitting `checkpoint_repo` means checkpoint tracking is off: no checkpoint is ever
written, and `get_checkpoint()` / `get_lag_metrics()` return `None`. Omitting `dlq_repo`
means DLQ capture is off: a permanently failed event is logged at `critical` and
re-raised, with no DLQ write attempted. This changed with ADR 0024 — both used to default
to a fresh in-memory repository, which looked durable (`get_checkpoint()` returned a
value, `get_lag_metrics()` returned real-looking numbers) while silently reprocessing
the entire event stream on every restart. Pass `InMemoryCheckpointRepository()` /
`InMemoryDLQRepository()` (from `eventsource`) explicitly to get that old behavior back;
it remains fine for tests, and remains wrong for production.

### Constructor

```python
CheckpointTrackingProjection(
    checkpoint_repo: ProjectionCheckpoints | None = None,
    dlq_repo: DLQRepository | None = None,
    retry_policy: RetryPolicy | None = None,
    tracer: Tracer | None = None,
    enable_tracing: bool = False,
) -> None
```

All five parameters are optional and positional-or-keyword, so
`MyProjection()` is valid and produces a fully functional projection with checkpoint
tracking and DLQ capture both disabled. Subclasses accept all five as well, though
`retry_policy` and `tracer` become keyword-only from `DeclarativeProjection` down.

| Parameter | Type | Default | Effect |
| --- | --- | --- | --- |
| `checkpoint_repo` | `ProjectionCheckpoints \| None` | `None` | Backing store for the projection's checkpoint. `None` → checkpoint tracking disabled. |
| `dlq_repo` | `DLQRepository \| None` | `None` | Backing store for dead-lettered events. `None` → DLQ capture disabled. |
| `retry_policy` | `RetryPolicy \| None` | `None` | Governs attempt count and backoff. `None` → the inline `ExponentialBackoffRetryPolicy` described in the next section (**not** `DEFAULT_RETRY_POLICY`). |
| `tracer` | `Tracer \| None` | `None` | Custom tracer, shared by the projection's own spans and the checkpoint/DLQ spans. When given, `enable_tracing` is ignored. |
| `enable_tracing` | `bool` | `False` | When `True` and OpenTelemetry is installed, builds a live tracer via `create_tracer(__name__, enable_tracing)`. |

`checkpoint_repo` takes the `ProjectionCheckpoints` interface from
`eventsource.ports.checkpoints`; `dlq_repo` takes `DLQRepository` from
`eventsource.ports.dlq`. Any implementation of those (the dialect-parameterized SQL
adapter, or in-memory) is accepted. The constructor never validates them beyond typing,
and never touches the store — no schema check, no connection attempt happens until the
first `handle()` call.

```python
from eventsource.application.projections import CheckpointTrackingProjection
from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
from eventsource import SQLCheckpointRepository, SQLDLQRepository
from eventsource.application.subscriptions.retry import RetryConfig

projection = OrderProjection(
    checkpoint_repo=SQLCheckpointRepository(pool),
    dlq_repo=SQLDLQRepository(pool),
    retry_policy=ExponentialBackoffRetryPolicy(RetryConfig(max_retries=5)),
    enable_tracing=True,
)
```

Note that `ExponentialBackoffRetryPolicy` is configured with a `RetryConfig` from
`eventsource.application.subscriptions.retry`, not with loose keyword arguments — the retry
configuration type is shared with the subscription machinery.

#### Interactions worth knowing

- **`tracer` covers checkpoint and DLQ spans too.** `self._tracer` is `tracer or
  create_tracer(__name__, enable_tracing)`, and `record_checkpoint()` / `send_to_dlq()`
  are called with that same tracer instance — unlike the old manager objects, there is
  no separate `enable_tracing` value for checkpoint/DLQ spans to disagree with the
  projection's own spans.
- **`_enable_tracing` reflects reality, not the request.** It is assigned
  `self._tracer.enabled`, so it stays `False` when `enable_tracing=True` was passed but
  the optional OpenTelemetry dependency is missing. Read this attribute, not the
  argument, to know whether spans are actually being emitted.
- **No `projection_name` parameter.** The checkpoint and DLQ keys come from
  `self.__class__.__name__`; see *Naming and identity* above for the consequences.
- **Subclasses must call it.** `DeclarativeProjection` and `DatabaseProjection` both
  invoke `super().__init__()`, but a hand-written subclass that overrides `__init__`
  without chaining leaves `_checkpoint_repo`, `_dlq_repo`, and `_retry_policy`
  unset, and `handle()` fails with `AttributeError` on the first event.

## Rebuilding a projection: `replay()`

```python
async def replay(
    feed: GlobalEventFeed,
    projections: Sequence[EventSubscriber],
    *,
    from_position: Position | None = None,
    tenant_id: UUID | None = None,
    aggregate_type: str | None = None,
    strict: bool = False,
    max_events: int = MAX_EVENTS_PER_REPLAY,
    max_failures: int = MAX_FAILURES_PER_REPLAY,
    on_failure: Callable[[ReplayFailure], None] | None = None,
) -> ReplayReport: ...
```

Reads the global feed from `from_position` and folds every event into every projection,
returning a report (ADR 0054). This is the answer to "how do I rebuild a projection from
the log" — a live subscription runner does the other job, catch-up on a timer via
`ProjectionCoordinator`/`ProjectionRegistry`, and the coordinator's `rebuild_projection()`
takes the events as a materialized list you have already read and filtered yourself.

```python
from eventsource.application.projections import replay

report = await replay(event_store, [orders, invoices])
print(f"{report.applied} applied, {report.failed} failed")
```

`feed` is type-hinted `GlobalEventFeed`, the narrowest port that suffices: `replay`
appends nothing, reads no stream, and looks up no event id. Any store adapter satisfies
it, as does a hand-written stand-in.

**A poison event does not stop the rebuild.** A projection that raises has its failure
recorded and the fold continues, because the alternative — stopping — denies the
projection every event after the bad one. This is deliberately the opposite of what a
live subscription does, where re-raising is what prevents checkpointing past a failure.

`from_position` is exclusive, matching `read_all`; `None` starts from the beginning,
which is what a rebuild wants. `replay` does **not** checkpoint: persist
`report.last_position` yourself if the rebuild is to be resumed.

### Scoping the read

`tenant_id=` and `aggregate_type=` are forwarded to the adapter as `FeedReadOptions` and
pushed into its query (ADR 0052), so rebuilding one tenant or one aggregate type out of a
shared store is an indexed read rather than a scan. Naming neither sends no options
object at all.

This is narrower than `tenant_filter` on the projection, which is applied *after*
delivery and therefore pays for the whole read either way.

```python
report = await replay(event_store, [orders], tenant_id=tenant, aggregate_type="Order")
```

### `ReplayReport`

| Field | Meaning |
| --- | --- |
| `applied` | Events delivered that no projection rejected. An event every projection ignores still counts. |
| `last_position` | The last position the (possibly filtered) read reached; `None` for an empty feed. |
| `failures` | One `ReplayFailure` per *rejection*, capped at `max_failures`. |
| `failures_truncated` | Failures that occurred but are not in `failures`. |
| `failed` (property) | Events at least one retained failure names — per *event*, not per rejection. |

`failed` and `len(failures)` legitimately differ: two projections rejecting one event is
one failed event and two failures. `failed` is derived from the distinct `event_id`s
rather than stored, so the two cannot drift apart. It keys on `event_id` and not on
`position` because `position` is `Position | None` — a feedless store sets it on nothing,
and keying on it would report `1` for a rebuild in which every event failed.

`failed` is exact only while `failures_truncated` is zero, and a lower bound otherwise.

### `ReplayFailure`

A frozen dataclass carrying `position`, `event_id`, `event_type`, `projection` (the
rejecting class's name), and `error` — the exception object itself, not a string about
it. A count alone gives an operator no route back to the poison event.

### Bounding the failure list

Every retained failure pins a live exception and, through its traceback, every frame's
locals. `max_failures` (default 1000) caps what the report holds; `failures_truncated`
counts what the cap dropped, so a truncated report says so rather than quietly reading
like a complete one.

`on_failure` is called for **every** failure regardless of the cap, so a caller who needs
all of them can stream them somewhere that is not memory:

```python
report = await replay(event_store, [orders], on_failure=log_replay_failure)
if report.failures_truncated:
    print(f"{report.failures_truncated} more failures -- see the log")
```

### `strict=True` and `ReplayFailedError`

Raises on the first rejection instead of carrying on, carrying the `ReplayFailure` as
`.failure` and the original exception as `__cause__`. Use it in tests and on a first
deployment, where a silent partial rebuild is most costly and least visible.

`ReplayFailedError` subclasses `ProjectionError`, so `except ProjectionError` catches it
alongside a live projection's failure. `on_failure` still fires before the raise.

### `max_events`

The feed is adapter-supplied and the loop's termination depends on it, so a cursor that
failed to advance would hang. `max_events` (default 10,000,000) turns that into a
`RuntimeError` naming the last position reached.
## `StoreProjection`

`eventsource.application.projections.store.StoreProjection[TStore]` is a
`DeclarativeProjection` that holds exactly one store and hands it to your handlers as
`self._store`. Use it when the projection's read model is a single object with its own
port — a graph store, a vector store, a repository — rather than a SQL table, which is
`DatabaseProjection`'s case.

```python
from eventsource.application.projections import StoreProjection, handles


class OrderProjection(StoreProjection[OrderStore]):
    @handles(OrderCreated)
    async def _on_created(self, _context: object, event: OrderCreated) -> None:
        await self._store.upsert(event.order)


projection = OrderProjection(order_store, checkpoint_repo=repo)
```

`TStore` is a type parameter, not a concrete type: the class names no adapter and no
driver, which is what keeps it in the application ring (ADR 0024 sent
`DatabaseProjection` to `adapters/sql` for naming an `async_sessionmaker`).

### Constructor

```python
def __init__(self, store: TStore, **options: Unpack[ProjectionOptions]) -> None
```

`store` is the only parameter this class declares. Everything else is
`ProjectionOptions`, a `TypedDict` naming exactly what `DeclarativeProjection.__init__`
accepts — `checkpoint_repo`, `dlq_repo`, `enable_tracing`, `retry_policy`, `tracer`,
`tenant_filter` — so the options stay individually typed and checked (PEP 692), unlike
`**kwargs: Any`. They are keyword-only here.

### Adding your own parameters

This is the reason the class exists. A subclass that needs constructor parameters of its
own declares only those and forwards the rest as one opaque `**options`, so it never
names — and never drops — a parameter belonging to its parent:

```python
from typing import Unpack

from eventsource.application.projections import ProjectionOptions, StoreProjection


class BatchingProjection(StoreProjection[OrderStore]):
    def __init__(
        self,
        store: OrderStore,
        batch_size: int = 100,
        **options: Unpack[ProjectionOptions],
    ) -> None:
        self._batch_size = batch_size
        super().__init__(store, **options)
```

`retry_policy`, `tracer`, and `tenant_filter` reach the base through that forward, and
keep reaching it when the parent gains options in a future release — which is the failure
0.10.0 fixed inside this tree and this class removes outside it.
