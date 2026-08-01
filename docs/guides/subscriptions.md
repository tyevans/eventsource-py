# Subscription Manager

`SubscriptionManager` drives projections from an event store and an event bus. It replays historical events from the store ("catch-up"), transitions each subscription to live events from the bus without gaps, persists checkpoints so restarts resume where they left off, and adds retries, a circuit breaker, a dead-letter queue, health probes, and graceful shutdown around the whole thing.

These recipes assume you already have an event store, an event bus, and a checkpoint repository wired up, and that you want to run one or more projections against them in production. Each section is independent — jump to the task you have:

- Getting a projection running: [Wire up a SubscriptionManager](#wire-up-a-subscriptionmanager), [Implement the projection protocol](#implement-the-projection-protocol), [Choose where a subscription starts](#choose-where-a-subscription-starts)
- Surviving failure: [Retry transient failures with exponential backoff](#retry-transient-failures-with-exponential-backoff), [Guard external calls with a circuit breaker](#guard-external-calls-with-a-circuit-breaker), [Route permanently failed events to the DLQ](#route-permanently-failed-events-to-the-dlq), [React to errors with callbacks](#react-to-errors-with-callbacks)
- Operating it: [Run multiple projections from one manager](#run-multiple-projections-from-one-manager), [Monitor subscription health](#monitor-subscription-health), [Expose Kubernetes readiness and liveness probes](#expose-kubernetes-readiness-and-liveness-probes), [Shut down gracefully](#shut-down-gracefully)

Import everything from `eventsource.application.subscriptions`:

```python
from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
)

manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
await manager.subscribe(my_projection)
await manager.start()
```

## Before you begin

You need Python 3.11 or later and three collaborators to construct a `SubscriptionManager`: an `EventStore` (historical events), an `EventBus` (live events), and a `CheckpointRepository` (position tracking). A `DLQRepository` is optional and only needed for the DLQ recipes.

Install the extras for the backends you plan to use — the core package depends only on `pydantic` and `sqlalchemy`:

```bash
# PostgreSQL store, checkpoints, and DLQ
pip install "eventsource-py[postgresql]"

# SQLite instead
pip install "eventsource-py[sqlite]"

# Redis event bus (also available: rabbitmq, kafka)
pip install "eventsource-py[redis]"

# Everything, including OpenTelemetry tracing
pip install "eventsource-py[all]"
```

For a database-backed deployment, apply the SQL schema first. The checkpoint recipes need the `projection_checkpoints` table and the DLQ recipes need `dead_letter_queue`; both ship in `src/eventsource/migrations/schemas/` (`checkpoints.sql`, `dlq.sql`, or `all.sql` / `sqlite_all.sql` for the full set).

The SQL repositories are dialect-parameterized -- the same classes serve PostgreSQL and SQLite -- and take a SQLAlchemy `AsyncConnection` or `AsyncEngine`:

```python
from sqlalchemy.ext.asyncio import create_async_engine

from eventsource import SQLCheckpointRepository, SQLDLQRepository

engine = create_async_engine("postgresql+asyncpg://localhost/app")
checkpoint_repo = SQLCheckpointRepository(engine)
dlq_repo = SQLDLQRepository(engine)
```

If you just want to follow along without infrastructure, swap in the in-memory implementations — `InMemoryCheckpointRepository()` and `InMemoryDLQRepository()` take no arguments, and `InMemoryEventStore` / `InMemoryEventBus` need no services. They lose all state when the process exits, so use them for tests and local exploration only.

Everything below also assumes you are working inside an async context (`asyncio.run(...)` or an ASGI app), since every store, bus, and subscription API is async.

## Wire up a SubscriptionManager

Construct the manager with the three required collaborators, register each projection with `subscribe()`, then call `start()`:

```python
import asyncio

from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager


async def main() -> None:
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    await manager.subscribe(
        OrderProjection(),
        SubscriptionConfig(start_from="beginning", batch_size=500),
    )

    results = await manager.start()
    failures = {name: err for name, err in results.items() if err is not None}
    if failures:
        raise RuntimeError(f"subscriptions failed to start: {failures}")

    try:
        await asyncio.Event().wait()  # keep the process alive
    finally:
        await manager.stop()


asyncio.run(main())
```

Three things happen when `start()` runs, per subscription: the manager resolves the starting position (from `config.start_from`), replays historical events out of the event store in batches, and then transitions to live events from the bus. Checkpoints are written along the way, so a restart picks up where the last one left off.

### What `subscribe()` gives you

`subscribe(subscriber, config=None, name=None)` returns the `Subscription` object, which is a live handle you can read for monitoring — `subscription.name`, `.state`, `.last_processed_position`, `.events_processed`, `.events_failed`, `.lag`, and `.is_running`:

```python
subscription = await manager.subscribe(OrderProjection())
print(subscription.name)   # "OrderProjection"
print(subscription.state)  # SubscriptionState.STARTING
```

The name defaults to the subscriber's class name. Pass `name=` when you run two subscriptions from the same projection class, or when the class name is not a stable identifier — the name is the checkpoint key, so changing it makes the subscription replay from its configured start position again:

```python
await manager.subscribe(OrderProjection(), name="orders-read-model")
```

Names must be unique within a manager; registering the same name twice raises `SubscriptionAlreadyExistsError`. Omitting `config` gives you `SubscriptionConfig()` defaults — resume from checkpoint, batch size 100, retries and circuit breaker on.

### Starting and stopping

`start()` returns a `dict[str, Exception | None]` mapping subscription name to `None` on success or the exception that stopped it. Subscriptions are isolated: one failing to start does not prevent the others, so always inspect the result rather than assuming success. Calling `start()` on an already-running manager logs a warning and returns an empty dict.

`stop()` drains in-flight events and saves checkpoints, with a `timeout` (default 30 seconds). Both `start()` and `stop()` accept `subscription_names=` to act on a subset:

```python
await manager.start(subscription_names=["orders-read-model"])
await manager.stop(timeout=15.0, subscription_names=["orders-read-model"])
```

For a long-running daemon, prefer `run_until_shutdown()` over the manual `start()` / sleep / `stop()` dance — see [Shut down gracefully](#shut-down-gracefully).

### Constructor options worth setting early

Beyond the three required arguments, the constructor takes:

| Argument | Default | Why you would set it |
| --- | --- | --- |
| `shutdown_timeout` | `30.0` | Total seconds allowed for a graceful shutdown. |
| `drain_timeout` | `10.0` | Seconds to wait for in-flight events before forcing shutdown. |
| `dlq_repo` | `None` | Enables the dead-letter queue for permanently failed events. |
| `error_handling_config` | `ErrorHandlingConfig()` | Retry, DLQ, and error-classification behavior. |
| `health_check_config` | `HealthCheckConfig()` | Lag and staleness thresholds used by the health probes. |
| `tracer` / `enable_tracing` | `None` / `True` | Supply a custom OpenTelemetry tracer, or pass `enable_tracing=False` to turn spans off. |

A production wiring usually looks like this:

```python
from eventsource.application.subscriptions import (
    ErrorHandlingConfig,
    HealthCheckConfig,
    SubscriptionManager,
)

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    dlq_repo=dlq_repo,
    shutdown_timeout=60.0,
    drain_timeout=20.0,
    error_handling_config=ErrorHandlingConfig(),
    health_check_config=HealthCheckConfig(),
)
```

`error_handling_config` and `health_check_config` are manager-wide; `SubscriptionConfig` is per subscription. The rest of this guide fills in what to put in each.

### Inspecting what is registered

The manager exposes read-only views of its registry, useful in tests and admin endpoints:

```python
manager.subscription_count          # int
manager.subscription_names          # list[str]
manager.subscriptions               # list[Subscription]
manager.get_subscription("orders-read-model")  # Subscription | None
manager.get_all_statuses()          # dict[str, SubscriptionStatus]
manager.is_running                  # bool
```

To remove a subscription at runtime, `await manager.unsubscribe(name)` — it stops the subscription first if it is running, and returns `True` if a subscription by that name existed.

## Implement the projection protocol

Anything you pass to `subscribe()` must satisfy two members: `subscribed_to()`, returning the event classes you want, and `handle()`, processing one event. That is the whole contract — no base class required:

```python
from eventsource.domain.event import DomainEvent
from eventsource.application.subscriptions import Subscriber


class OrderProjection:
    def __init__(self, db) -> None:
        self._db = db

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self._db.insert_order(event.aggregate_id, event.total)
        elif isinstance(event, OrderShipped):
            await self._db.mark_shipped(event.aggregate_id)


assert isinstance(OrderProjection(db), Subscriber)  # runtime-checkable
```

`Subscriber`, `SyncSubscriber`, and `BatchSubscriber` are `@runtime_checkable` Protocols, so the `isinstance()` check above works and is worth asserting in a test — a typo in a method name otherwise surfaces only when the subscription starts.

If you prefer inheritance, `BaseSubscriber` is an ABC with the same two abstract methods plus `can_handle(event)` (defaults to `type(event) in self.subscribed_to()`) and a `__repr__` that lists the subscribed types. `FilteringSubscriber` goes further: override `should_handle(event)` for predicate filtering and implement `_process_event(event)` instead of `handle()`.

```python
from eventsource.application.subscriptions import FilteringSubscriber


class TenantOrderProjection(FilteringSubscriber):
    def __init__(self, tenant_id: UUID) -> None:
        self._tenant_id = tenant_id

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    def should_handle(self, event: DomainEvent) -> bool:
        return event.tenant_id == self._tenant_id

    async def _process_event(self, event: DomainEvent) -> None:
        await self._db.insert_order(event.aggregate_id, event.total)
```

Import all of these from `eventsource.application.subscriptions`. The manager's own type hint is the `EventSubscriber` ABC from `eventsource.ports.handlers`, but the runners only ever call `subscribed_to()` and `handle()`, so a plain duck-typed class works.

### What `subscribed_to()` controls

The returned list does double duty, and the two paths differ:

- **Live path**: the live runner calls `subscribed_to()` once at startup and registers a bus handler per event type. Types missing from the list are never delivered — and adding a type later requires restarting the subscription.
- **Catch-up path**: the runner reads *all* events from the store and filters them in-process through an `EventFilter` built by `EventFilter.from_config_and_subscriber(config, subscriber)`. `SubscriptionConfig.event_types` wins if set; otherwise the filter falls back to `subscribed_to()`. Returning an empty list means no filtering at all during catch-up, so `handle()` sees every event in the store — declare your types explicitly unless that is what you want.

Because `config.event_types` overrides the subscriber during catch-up but the *live* subscription always uses `subscribed_to()`, keep the two consistent. Setting `config.event_types` to a narrower set than `subscribed_to()` means the projection sees fewer events while catching up than it does once live.

### Handle events one at a time

During catch-up, `handle()` is called once per event in global-position order and awaited before the next event is delivered. On the live path the bus invokes the same method per event, guarded by a flow-control slot (`max_in_flight`), after duplicate and filter checks. Design for three things:

**Raising is how you signal failure — but nothing retries your handler.** The `RetryableOperation` and circuit breaker configured on a subscription wrap the runner's *own* I/O: reading batches from the event store and saving checkpoints. Your `handle()` call is not wrapped. An exception out of `handle()` records failure metrics and `subscription.events_failed` / `last_error`, then either propagates or is logged and swallowed — it is not retried and it is not automatically written to the DLQ. Retrying a flaky database write inside `handle()` is your job.

**`continue_on_error` decides whether one bad event stops the subscription.** With the default `SubscriptionConfig(continue_on_error=True)`, both runners log a warning and move on; the catch-up runner then checkpoints past the failed event, so it will never be redelivered. Set `continue_on_error=False` to re-raise and stop the subscription at the first unhandled exception — the right choice when silently skipping an event would corrupt the read model.

```python
config = SubscriptionConfig(continue_on_error=False)  # halt instead of skipping
```

**Handlers must be idempotent.** Checkpoints are written per batch by default (`CheckpointStrategy.EVERY_BATCH`), so a crash redelivers every event processed since the last checkpoint write. Use upserts keyed by `event.aggregate_id`, or a processed-event-id table, rather than blind inserts. `CheckpointStrategy.EVERY_EVENT` narrows the redelivery window at the cost of a checkpoint write per event.

Note that `SubscriptionConfig.processing_timeout` is validated but not enforced by the runners — a `handle()` that hangs blocks the subscription. Apply your own `asyncio.timeout()` around slow external calls.

To route failures to the dead-letter queue, call the subscription's error handler yourself — see [Route permanently failed events to the DLQ](#route-permanently-failed-events-to-the-dlq).

For an ergonomic alternative to the `isinstance` ladder, `DeclarativeProjection` discovers `@handles(OrderCreated)` methods, generates `subscribed_to()` from them, and dispatches per event type.

### Handle events in batches with handle_batch()

When a projection can write in bulk — one multi-row insert instead of N — implement `handle_batch()`. The `BatchSubscriber` protocol (`@runtime_checkable`, import from `eventsource.application.subscriptions`) requires exactly two members: `subscribed_to()` and `async def handle_batch(self, events: Sequence[DomainEvent]) -> None`. Note it does *not* require `handle()`.

The easier route is `BatchAwareSubscriber`, an ABC extending `BaseSubscriber` whose default `handle_batch()` simply loops over `handle()`, so you override it only where bulk writes pay off:

```python
from collections.abc import Sequence

from eventsource.application.subscriptions import BatchAwareSubscriber


class AnalyticsProjection(BatchAwareSubscriber):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        await self._db.record_metric(event)

    async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
        await self._db.bulk_record_metrics(events)
```

Batch order is the order the events were read from the store, and the sequence may be empty — guard bulk calls that reject empty input.

Two utilities come with it. `supports_batch_handling(obj)` returns `True` when an object has a callable `handle_batch` attribute — a shape check only, it does not inspect the signature. And `handle_batch_with_error_tracking(events)`, a method on `BatchAwareSubscriber`, processes a batch event by event via `handle()`, returning `(success_count, failures)` where `failures` is a list of `(event, exception)` pairs, so one bad event does not lose the rest of the batch (each failure is also logged at warning level):

```python
success_count, failures = await projection.handle_batch_with_error_tracking(events)
for event, exc in failures:
    logger.error("skipped %s: %s", event.event_id, exc)
```

**`SubscriptionManager` does not call `handle_batch()`.** Both the catch-up and live runners deliver events one at a time through `handle()`. `SubscriptionConfig.batch_size` sizes the *read* batch pulled from the event store, not a batch handed to your subscriber. Use `handle_batch()` when you drive a projection yourself — a backfill script, a rebuild job, a test harness feeding it `store.read_all()` output — and keep `handle()` correct regardless, since that is what the manager will call.

`FilteringSubscriber` subclasses `BatchAwareSubscriber` and overrides `handle_batch()` to apply `should_handle()` across the sequence before calling `_process_event()` per surviving event. If you override `handle_batch()` on a `FilteringSubscriber` for bulk writes, re-apply the filter yourself — the override replaces that logic.

## Choose where a subscription starts

`SubscriptionConfig.start_from` decides the position a subscription resolves to when `start()` runs. It accepts four things (`StartPosition = Literal["beginning", "end", "checkpoint"] | Position`), resolved by `StartFromResolver`:

| `start_from` | Resolves to | Use it for |
| --- | --- | --- |
| `"checkpoint"` (default) | The saved checkpoint for this subscription name, or the start of the feed if there is none | Normal long-running projections that must resume after a restart |
| `"beginning"` | The start of the feed | Rebuilding a read model from the full history, every time it starts |
| `"end"` | `await event_store.current_position()` | Live-only consumers (notifications, cache invalidation) that must not replay history |
| `Position` | That exact opaque position token | Backfills and repairs from a known point |

```python
from eventsource.application.subscriptions import SubscriptionConfig

await manager.subscribe(OrderProjection(), SubscriptionConfig())                       # resume
await manager.subscribe(RebuildProjection(), SubscriptionConfig(start_from="beginning"))
await manager.subscribe(Notifier(), SubscriptionConfig(start_from="end"))
repair_position = await event_store.current_position()
await manager.subscribe(Repair(), SubscriptionConfig(start_from=repair_position), name="repair-from-known-point")
```

Positions are **exclusive** and opaque: the catch-up runner reads with `from_position=<resolved position>`, and the store returns only events strictly after it. `start_from="beginning"` therefore reads the entire feed, and a `Position` token delivers events from just after that point onward. Since a checkpoint records the *last processed* position, resuming never redelivers the checkpointed event itself. `Position` values are totally ordered only within the store that produced them — comparing or mixing tokens from different stores raises `PositionForeignError` (see [ADR 0024](../adrs/0024-projection-persistence-ports.md)).

`SubscriptionConfig` no longer accepts a bare `int` for `start_from`; pass one of the string literals or a `Position` obtained from the store or a prior checkpoint.

### "checkpoint" when no checkpoint exists

The first ever start of a subscription has no row in the checkpoint repository. `StartFromResolver` logs `"No checkpoint found, starting from beginning"` and resolves to `0`, so a fresh `"checkpoint"` subscription replays the whole store. That is usually what you want for a projection, but it means a mistyped subscription name silently triggers a full replay under a new checkpoint key instead of resuming the old one. Pin the name explicitly when it matters:

```python
await manager.subscribe(OrderProjection(), name="orders-read-model")
```

(`CheckpointNotFoundError` exists in `eventsource.domain.exceptions` for code that wants to treat a missing checkpoint as fatal; the resolver itself never raises it.)

### What "end" actually skips

`"end"` resolves to the store's current max global position, and the transition coordinator's catch-up target is the same watermark — so catch-up finishes with zero events and the subscription goes straight to live bus delivery. Events written between resolution and the live subscription being registered are covered by the transition buffer, but anything written before the process started is never seen. A `"end"` subscription writes its first checkpoint only once it processes a live event.

### Start position is resolved once

`start_from` is read at `start()` time only. After that the subscription tracks `last_processed_position` and checkpoints from there; `stop()` followed by `start()` re-resolves it. Two consequences:

- A subscription configured with `start_from="beginning"` replays the entire store on *every* process restart, ignoring the checkpoints it wrote. Use it for one-shot rebuilds, not steady state.
- `SubscriptionConfig` is a frozen dataclass, so you cannot change `start_from` on a registered subscription. `await manager.unsubscribe(name)` and re-`subscribe()` with a new config instead.

To force a replay of a subscription that normally uses `"checkpoint"`, either subscribe under a new name (a new checkpoint key) or clear the existing checkpoint while the manager is stopped:

```python
await checkpoint_repo.reset_checkpoint("orders-read-model")
```

### Prebuilt configs

Two factory helpers in `eventsource.application.subscriptions` cover the common shapes:

```python
from eventsource.application.subscriptions import create_catch_up_config, create_live_only_config

create_catch_up_config(batch_size=1000)  # start_from="checkpoint", EVERY_BATCH checkpoints
create_live_only_config()                # start_from="end", batch_size=100, EVERY_EVENT
```

`create_catch_up_config(checkpoint_every_batch=False)` switches to `CheckpointStrategy.PERIODIC` instead.
