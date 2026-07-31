# Event Bus

The event bus is how a published `DomainEvent` reaches the code that reacts to
it. Aggregates and the event store are only responsible for *recording* what
happened; projections, read models, integrations, and audit logs subscribe to
the bus to *act* on it. `eventsource.bus` ships one in-process implementation
and three distributed ones behind a single `EventBus` abstract base class, so
the code that publishes and subscribes stays the same when you move from a
single process to Redis, RabbitMQ, or Kafka.

This guide shows you how to:

- Choose between `InMemoryEventBus`, `RedisEventBus`, `RabbitMQEventBus`, and
  `KafkaEventBus`, and install the optional extra each one needs.
- Publish with `publish()` — synchronously, or fire-and-forget with
  `background=True`.
- Register consumers three ways: `subscribe()` for one event type,
  `subscribe_all()` for a subscriber that declares its own `subscribed_to()`,
  and `subscribe_to_all_events()` for cross-cutting wildcard handlers.
- Drive the distributed backends through their `connect()` / `start_consuming()`
  / `stop_consuming()` / `disconnect()` lifecycle.
- Degrade gracefully when an optional dependency is missing, using the
  `*_AVAILABLE` flags and the matching `*NotAvailableError`.
- Wire the bus into `SubscriptionManager`, and inspect it with `get_stats()`.

Two properties of the distributed backends shape everything below: they make no
ordering guarantee across handlers, and they deliver at least once. Write
handlers to be idempotent.

All names in this guide are importable from `eventsource.bus` (and the
user-facing ones from the top-level `eventsource` package).

## What the event bus does

An event bus takes a list of `DomainEvent` objects from a producer and delivers
each one to every handler registered for its type. `EventBus` is an abstract
base class in `eventsource.bus.interface` with six abstract methods, and every
backend implements exactly that surface:

| Method | Purpose |
| --- | --- |
| `await publish(events, background=False)` | Deliver a list of events to subscribers. |
| `subscribe(event_type, handler)` | Register a handler for one event class. |
| `unsubscribe(event_type, handler)` | Remove it; returns `True` if it was found. |
| `subscribe_all(subscriber)` | Register a subscriber for each type in its `subscribed_to()`. |
| `subscribe_to_all_events(handler)` | Wildcard: receive every published event. |
| `unsubscribe_from_all_events(handler)` | Remove a wildcard handler; returns `True` if found. |

Because the four implementations share this ABC, publishing and subscribing
code is backend-independent. Only construction and — for the distributed
backends — the connection lifecycle differ.

A handler can be either an object with a `handle(event)` method or a plain
callable taking the event (`EventHandlerFunc` is
`Callable[[DomainEvent], Awaitable[None] | None]`). Sync and async handlers are
both accepted; the bus adapts them internally, so `lambda e: print(e)` and an
`async def` coroutine function work equally well.

### Delivery semantics you can rely on

Within a single `publish()` call, `InMemoryEventBus` processes the events in
list order, one at a time. For each event it collects the type-specific
handlers plus the wildcard handlers and invokes them **concurrently** with
`asyncio.gather(..., return_exceptions=True)`, waiting for all of them before
moving to the next event. So events are ordered relative to each other, but
handlers for the same event are not ordered relative to one another.

Handler failures are isolated. Each invocation is wrapped so an exception is
caught, logged with `exc_info=True`, counted in the `handler_errors` stat, and
recorded on the tracing span — it never aborts the other handlers or the rest
of the batch. This means `await publish(...)` returning successfully does *not*
mean every handler succeeded. If a handler must not silently drop work, give it
its own retry and dead-letter path (see [Retries and the dead letter
queue](#retries-and-the-dead-letter-queue)) or run it under
`SubscriptionManager` rather than as a bare bus subscription.

If no handler is registered for an event's type, the bus logs at debug level
and moves on. Publishing to an empty bus is not an error.

### What it deliberately does not do

The bus is a dispatch mechanism, not a store:

- **It is not durable on its own.** `InMemoryEventBus` keeps subscribers and
  any published-event history in process memory; the durable record of what
  happened is the event store, not the bus.
- **It does not guarantee exactly-once delivery.** The distributed backends
  redeliver on failure, so handlers must be idempotent.
- **It does not track progress.** There is no per-handler checkpoint at this
  layer. Resumable, checkpointed consumption is what `SubscriptionManager`
  adds on top (see [Wire the bus into
  SubscriptionManager](#wire-the-bus-into-subscriptionmanager)).
- **It does not validate or transform events.** Events arrive as the pydantic
  models the producer published.

### Observability built in

Every implementation is expected to emit OpenTelemetry spans through the
composition-based tracer from `eventsource.observability`, using standard span
names — `eventsource.event_bus.publish`, `.dispatch`, `.handle`, and, for the
distributed backends, `.consume` and `.process`. Distributed backends also
inject trace context into message headers on publish and extract it on consume,
so a trace follows an event across process boundaries. Pass
`enable_tracing=False` to a bus constructor to turn this off. Alongside
tracing, each bus exposes counters via `get_stats()` — see [Inspect bus health
with get_stats and get_stats_dict](#inspect-bus-health-with-get_stats-and-get_stats_dict).

## Choosing a backend

Pick the backend that matches your deployment shape, then keep the rest of your
code unchanged — all four satisfy the same `EventBus` ABC.

| Backend | Class / config | Extra | Use it when |
| --- | --- | --- | --- |
| In-process | `InMemoryEventBus` | none (core install) | Tests, development, single-process deployments. |
| Redis Streams | `RedisEventBus` + `RedisEventBusConfig` | `redis` | You already run Redis and want durable, replayable streams with consumer groups. |
| RabbitMQ | `RabbitMQEventBus` + `RabbitMQEventBusConfig` | `rabbitmq` | You need broker-side routing (topic/direct/fanout/headers exchanges) and per-consumer prefetch flow control. |
| Kafka | `KafkaEventBus` + `KafkaEventBusConfig` | `kafka` | High throughput, long retention, and per-aggregate ordering across many partitions. |

Start with `InMemoryEventBus`. It needs no external service and no
`connect()` call, so it keeps tests fast and local runs simple. It is also the
only backend that keeps a record of what was published: the `published_events`
property returns the in-process history and `clear_published_events()` resets it, which
is what test assertions hang off. Move off it when events must cross a process
boundary — its subscribers and history live in this process's memory only.

### What the three distributed backends actually give you

They share a feature set: durable delivery that survives a restart, consumer
groups so several instances share the load, at-least-once delivery, a dead
letter queue after `max_retries`, and OpenTelemetry trace context propagated
through message headers. They also share the same lifecycle — `connect()`,
`publish()`, `start_consuming()`, `stop_consuming()`, `disconnect()`. The
differences that should drive your choice:

- **`RedisEventBus`** publishes every event to a single Redis stream named
  `{stream_prefix}:stream`, with a DLQ at `{stream_prefix}:stream_dlq`.
  Consumption uses Redis consumer groups, batched reads (`batch_size`,
  `block_ms`), and recovery of messages abandoned by a dead consumer after
  `pending_idle_ms`. It is the lightest distributed option if Redis is already
  in your stack, and batch publishes are pipelined.
- **`RabbitMQEventBus`** publishes to an exchange (`exchange_name`) whose
  `exchange_type` — `topic` (default), `direct`, `fanout`, or `headers` —
  decides how events reach queues; `routing_key_pattern` controls the binding.
  Choose it when you want the broker, rather than your handlers, to decide who
  sees what. `prefetch_count` bounds unacknowledged messages per consumer, and
  `durable` / `auto_delete` control whether topology survives a broker restart
  (production wants `durable=True`, `auto_delete=False`).
- **`KafkaEventBus`** publishes to `{topic_prefix}.stream` with the DLQ at
  `{topic_prefix}.stream.dlq`. Its distinguishing property is the partition key:
  every message is keyed by `str(event.aggregate_id)`, so all events for one
  aggregate land on the same partition and stay ordered relative to each other
  even as you scale consumers out. It also exposes the most operational surface
  — `acks`, `compression_type`, batching (`batch_size`, `linger_ms`),
  `auto_offset_reset`, SASL/SSL security settings — plus optional OpenTelemetry
  metrics via `enable_metrics`.

### Constraints that apply no matter which you pick

None of the distributed backends offers exactly-once delivery. The bus does not
participate in a transaction with the event store, so a store write can succeed
while the publish fails (consumers miss events) or the reverse (consumers see
events that were never durably stored). If that gap matters, put a
transactional outbox between the store and the bus rather than relying on the
bus. And because redelivery after a consumer crash or rebalance is normal,
every handler you register must be idempotent.

Ordering is also weaker than it looks. Kafka gives you per-aggregate ordering
via the partition key; Redis and RabbitMQ do not promise cross-consumer
ordering; and on *every* backend, handlers for the same event run without any
ordering between them. Do not encode a dependency between two handlers by
registration order.

## Install the right optional extra

Only `InMemoryEventBus` works with the core install. Each distributed backend
is behind an optional extra, so add the one you chose in the previous section:

```bash
# In-process only — nothing to install beyond the core package
pip install eventsource-py

# One extra per distributed backend
pip install "eventsource-py[redis]"       # RedisEventBus     (redis>=5.0,<6.0)
pip install "eventsource-py[rabbitmq]"    # RabbitMQEventBus  (aio-pika>=9.0.0)
pip install "eventsource-py[kafka]"       # KafkaEventBus     (aiokafka>=0.9.0,<1.0.0)
```

Extras compose, so a service that consumes from Redis and writes projections to
PostgreSQL installs both in one command:

```bash
pip install "eventsource-py[redis,postgresql]"
```

With `uv`, the equivalents are `uv add "eventsource-py[redis]"` in a project, or
`uv sync --all-extras` when you are working inside this repository and want
every backend available for the test suite.

Two extras are worth knowing about beyond the buses themselves:

- `telemetry` installs `opentelemetry-api` and `opentelemetry-sdk`. Every bus
  emits spans through `eventsource.observability`, but without this extra the
  tracer is a no-op — install it if you want the publish/dispatch/handle spans
  and the cross-process trace propagation described earlier to actually reach a
  collector.
- `all` pulls in `postgresql`, `sqlite`, `redis`, `rabbitmq`, `kafka`, and
  `telemetry` at once. Convenient for a development machine; prefer naming the
  specific extras in a production image so you are not shipping three broker
  clients to run one.

### You do not need the extra to import the module

Installing the wrong extra is not a startup crash. Each backend module wraps its
third-party import in `try` / `except ImportError` and sets a module-level flag
— `REDIS_AVAILABLE`, `RABBITMQ_AVAILABLE`, `KAFKA_AVAILABLE` — so
`from eventsource.bus import RedisEventBus` succeeds whether or not `redis` is
installed. The failure is deferred to construction: the constructor checks the
flag and raises `RedisNotAvailableError`, `RabbitMQNotAvailableError`, or
`KafkaNotAvailableError` (all subclasses of `ImportError`) with the install
command in the message. That is what makes runtime backend selection from
configuration possible; see
[Handle a missing extra: *_AVAILABLE flags and *NotAvailableError](#handle-a-missing-extra-_available-flags-and-notavailableerror).

!!! note "The extra name in the error message is abbreviated"

    Those exceptions say `pip install eventsource[redis]`, but the distribution
    on PyPI is `eventsource-py`. Install `eventsource-py[redis]`.

### Verify the extra landed

```bash
python -c "from eventsource.bus import REDIS_AVAILABLE, RABBITMQ_AVAILABLE, KAFKA_AVAILABLE; print(REDIS_AVAILABLE, RABBITMQ_AVAILABLE, KAFKA_AVAILABLE)"
```

Each flag is `True` only when that backend's client library imported cleanly. A
`False` here is the same condition the constructor will raise on, so checking it
after a deploy tells you immediately whether the image was built with the extra
you expected.

## Start in-process with InMemoryEventBus

`InMemoryEventBus` needs no configuration and no connection step. Construct it
and it is ready to accept subscriptions and publishes:

```python
from eventsource import InMemoryEventBus

bus = InMemoryEventBus()
```

The constructor takes only keyword arguments, and both are about tracing:

| Argument | Default | Effect |
| --- | --- | --- |
| `tracer` | `None` | Supply your own `Tracer` from `eventsource.observability`. When given, `enable_tracing` is ignored. |
| `enable_tracing` | `True` | When no `tracer` is passed, build one for this module. If OpenTelemetry is not installed the tracer is a no-op, so leaving this `True` costs nothing. |

```python
# Silence bus spans entirely (useful in tight test loops)
bus = InMemoryEventBus(enable_tracing=False)
```

There is no `connect()` / `disconnect()` pair here — those belong to the
distributed backends. A freshly constructed `InMemoryEventBus` is usable
immediately, and the only teardown call is `await bus.shutdown()`, which
matters solely if you publish with `background=True` (see
[Publish in the background and shut down cleanly](#publish-in-the-background-and-shut-down-cleanly)).

### Create it once and share it

The bus's subscriber registry lives on the instance. Two `InMemoryEventBus()`
objects know nothing about each other, so a handler registered on one will
never see events published through the other. Build exactly one per process and
inject it wherever it is needed — into repositories, subscription managers, and
application services — rather than constructing a new one at each call site.

```python
# app/container.py — a single instance for the process
event_bus = InMemoryEventBus()
```

Subscription management is thread-safe: `subscribe()`, `unsubscribe()`,
`subscribe_to_all_events()`, `unsubscribe_from_all_events()`,
`clear_subscribers()`, and `clear_published_events()` all take an internal
`threading.RLock`, so you can register handlers from a worker thread while the
event loop is running. Publishing is not thread-safe in the same way — call
`await bus.publish(...)` from the async context that owns the bus.

### A complete, runnable smoke test

Everything below runs on the core install with no services and no extras:

```python
import asyncio
from uuid import uuid4

from eventsource import DomainEvent, InMemoryEventBus


class OrderPlaced(DomainEvent):
    total: float


async def main() -> None:
    bus = InMemoryEventBus()

    async def send_confirmation(event: OrderPlaced) -> None:
        print(f"confirming order {event.aggregate_id} for {event.total}")

    bus.subscribe(OrderPlaced, send_confirmation)

    await bus.publish([OrderPlaced(aggregate_id=uuid4(), total=42.0)])

    print(bus.get_subscriber_count(OrderPlaced))  # 1
    print(bus.get_stats()["events_published"])    # 1


asyncio.run(main())
```

`publish()` here is fully synchronous: it returns only after
`send_confirmation` has finished. That is what makes the in-memory bus
comfortable to assert against — by the time the `await` resolves, the side
effects you care about have already happened.

### Inspecting and resetting it in tests

Because it is in-process, `InMemoryEventBus` can tell you what went through it —
something no distributed backend offers:

```python
bus = InMemoryEventBus()
await bus.publish([event_one, event_two])

assert len(bus.published_events) == 2
assert bus.published_events[0] is event_one
```

`published_events` is a **property**, not a method, and it returns a *copy* of
the internal list, so mutating the result cannot corrupt the bus. Events are
recorded before dispatch, so an event appears in the history even if every
handler for it raised.

For test isolation, reset the pieces you own between cases:

- `bus.clear_published_events()` — drop the recorded history, keep subscribers.
- `bus.clear_subscribers()` — drop every type-specific *and* wildcard handler.
- `bus.get_subscriber_count(EventType)` — handlers for one type (wildcards not
  included); omit the argument for the total across all types.
- `bus.get_wildcard_subscriber_count()` — handlers registered via
  `subscribe_to_all_events()`.

A pytest fixture that gives each test a clean bus is usually simpler than
resetting a shared one:

```python
import pytest

from eventsource import InMemoryEventBus


@pytest.fixture
def bus() -> InMemoryEventBus:
    return InMemoryEventBus()
```

If you do share a module- or session-scoped bus, call both
`clear_subscribers()` and `clear_published_events()` in teardown — leftover
handlers from an earlier test are a classic source of cross-test interference,
and the counters returned by `get_stats()` accumulate for the life of the
instance and are never reset by either call.

### When to keep it and when to move on

Keep `InMemoryEventBus` for unit and integration tests, local development, and
genuinely single-process deployments. Move to a distributed backend once a
second process needs to react to your events, or once losing in-flight
dispatches on a restart is unacceptable — the bus holds no durable record, and
the recorded `published_events` history grows unbounded for the lifetime of the
instance, which is fine for a test run but not for a long-lived server. The
publish and subscribe calls you write now carry over unchanged; see
[Switch to a distributed bus](#switch-to-a-distributed-bus-the-connect--consume--disconnect-lifecycle).

## Publish events

`publish()` is the only way to get events onto the bus. It takes a **list** of
`DomainEvent` objects, not a single event, and it is `async` on every backend:

```python
await bus.publish([OrderPlaced(aggregate_id=order_id, total=42.0)])
```

Wrap a lone event in a list. Passing the event directly is a common slip and
will fail when the bus iterates it. Publishing an empty list is explicitly a
no-op on all four backends: they return immediately without touching handlers,
stats, or the broker, so you never need to guard the call yourself.

```python
# Safe — returns immediately, nothing dispatched
await bus.publish([])

# Typical: publish whatever the aggregate produced this round
await bus.publish(order.get_uncommitted_events())
```

### What happens between the call and the return

With `InMemoryEventBus` and the default `background=False`, `publish()` walks
the list in order. For each event it appends the event to the published-event
history, then dispatches it, then increments `events_published` — and only then
moves to the next event. Dispatch collects the handlers registered for that
event's exact type plus every wildcard handler, and runs them concurrently via
`asyncio.gather(..., return_exceptions=True)`, waiting for all of them.

The consequences are worth stating plainly:

- **Events are ordered; handlers are not.** Event *n+1* is not dispatched until
  every handler for event *n* has finished, so a batch is applied in list
  order. Within one event, the handlers race each other.
- **The await resolving does not mean the handlers succeeded.** Every handler
  invocation is wrapped: an exception is caught, logged with `exc_info=True`,
  counted in the `handler_errors` stat, and marked on the tracing span. It never
  propagates to the caller and never stops the other handlers or the remaining
  events. `publish()` returns `None` — there is no per-handler result to inspect.
- **A failed handler does not un-publish the event.** The event is recorded in
  `published_events` *before* dispatch, so it appears in the history even if
  everything downstream raised.
- **No handlers is not an error.** An event type with no subscribers is logged
  at debug level and skipped.

If work must not be silently dropped, do not rely on `publish()` raising. Give
the handler its own retry and dead-letter path, or run it under
`SubscriptionManager` (see
[Retries and the dead letter queue](#retries-and-the-dead-letter-queue)).

### Publishing after the aggregate is saved

The usual shape is: save to the event store first, then publish what was saved.
Reversing this means consumers can react to an event that was never durably
recorded.

```python
async def place_order(order: Order) -> None:
    events = order.get_uncommitted_events()

    await store.append(
        StreamId(category="Order", aggregate_id=order.aggregate_id),
        events,
        ExpectedVersion.exact(order.version),
    )
    await bus.publish(events)          # only after the append succeeded

    order.mark_events_as_committed()
```

The bus does not participate in the store's transaction. If the process dies
between the two calls, the events are durable but nobody was notified. That gap
is inherent to publishing directly from application code — close it with a
transactional outbox if your domain cannot tolerate it, rather than by
reordering the calls.

### The `background` flag

Every backend accepts `background: bool = False`, but it means something
slightly different per backend — and in one case nothing at all:

| Backend | `background=True` |
| --- | --- |
| `InMemoryEventBus` | Wraps dispatch in an `asyncio.create_task()` and returns immediately. Handlers run later, on the same loop. |
| `RabbitMQEventBus` | Publishes without waiting for publisher confirms (`wait_for_confirm=not background`). |
| `KafkaEventBus` | Sends without awaiting the broker's record metadata; delivery is tracked by a callback that logs errors and updates metrics. |
| `RedisEventBus` | Ignored — the parameter is accepted and has no effect. |

Leave it `False` unless you have measured a reason not to. `True` trades a
delivery guarantee for latency, and on the in-memory bus it also means an
assertion immediately after `publish()` will race the handler. Details, and the
`shutdown()` call that drains outstanding in-memory tasks, are in
[Publish in the background and shut down cleanly](#publish-in-the-background-and-shut-down-cleanly).

### Publishing on the distributed backends

The call signature is unchanged, but two behaviours differ. First, publishing
goes to a broker rather than to local handlers, so `publish()` returning tells
you the broker accepted the message — not that any consumer processed it; that
happens in a separate consumer loop (see
[Run the consumer loop with start_consuming and stop_consuming](#run-the-consumer-loop-with-start_consuming-and-stop_consuming)).
Second, broker failures *do* raise. Unlike handler errors, a connection or
publish failure propagates to your caller, so publishing to a distributed bus
needs real error handling.

`RedisEventBus` and `RabbitMQEventBus` auto-connect if `publish()` is called
before `connect()`. `KafkaEventBus` does not — it raises
`RuntimeError("Not connected to Kafka. Call connect() first.")`. Connect
explicitly at startup on all three rather than depending on the auto-connect
path.

Batches are optimised: Redis pipelines a multi-event publish into one round
trip, RabbitMQ publishes concurrently with `asyncio.gather()`, and Kafka keys
every message by `str(event.aggregate_id)` so one aggregate's events share a
partition and stay ordered. That is another reason to hand `publish()` the whole
list at once instead of looping over it with one call per event.

## Subscribe a handler to specific event types

`subscribe()` registers one handler against one event class. It is a plain
synchronous method — no `await` — and it returns `None`:

```python
bus.subscribe(OrderPlaced, send_confirmation)
```

To react to several event types with the same handler, call it once per type:

```python
bus.subscribe(OrderPlaced, audit_log)
bus.subscribe(OrderShipped, audit_log)
bus.subscribe(OrderCancelled, audit_log)
```

The signature is identical on all four backends. On the distributed buses,
`subscribe()` only records the handler in a local registry — nothing is sent to
the broker, and nothing is delivered until the consumer loop is running (see
[Run the consumer loop with start_consuming and stop_consuming](#run-the-consumer-loop-with-start_consuming-and-stop_consuming)).

### Matching is on the exact class

Dispatch looks up `type(event)` in the registry. A handler subscribed to a base
class will **not** see instances of its subclasses:

```python
class OrderEvent(DomainEvent): ...
class OrderPlaced(OrderEvent): ...

bus.subscribe(OrderEvent, handler)
await bus.publish([OrderPlaced(aggregate_id=order_id)])   # handler is NOT called
```

Subscribe to each concrete event class you care about, or use
`subscribe_to_all_events()` and filter inside the handler (see
[Subscribe to every event with subscribe_to_all_events](#subscribe-to-every-event-with-subscribe_to_all_events)).

### What counts as a handler

Anything with a `handle(event)` method, or any callable taking the event. Sync
and async are both fine — the bus wraps whatever you give it in a
`HandlerAdapter` that normalises it to an async interface, so all four of these
work:

```python
# 1. Object with an async handle()
class ConfirmationSender:
    async def handle(self, event: DomainEvent) -> None:
        await email.send(event.aggregate_id)

bus.subscribe(OrderPlaced, ConfirmationSender())

# 2. Object with a sync handle()
class AuditLog:
    def handle(self, event: DomainEvent) -> None:
        print(event)

bus.subscribe(OrderPlaced, AuditLog())

# 3. Async function
async def send_confirmation(event: DomainEvent) -> None: ...

bus.subscribe(OrderPlaced, send_confirmation)

# 4. Sync function or lambda
bus.subscribe(OrderPlaced, lambda event: print(event))
```

Two details of the adaptation are worth knowing. An object that has a `handle`
attribute is always treated as a handler object — the `handle` method is used
even if the object is also callable. And a sync handler runs *inline on the
event loop*, not in a thread pool: if it blocks, it blocks the loop. Push
blocking I/O into an async handler, or hand it to `asyncio.to_thread()`
yourself.

Anything that is neither callable nor has a `handle` method is rejected
immediately, at subscribe time rather than at publish time:

```python
bus.subscribe(OrderPlaced, "not_a_handler")
# TypeError: Handler must have a handle() method or be callable, got <class 'str'>
```

### Registering the same handler twice

`subscribe()` appends unconditionally; it does not deduplicate. Calling it twice
with the same handler and event type registers it twice, and the handler is then
invoked twice per event. Subscribe once, at wiring time, rather than inside a
request path or a fixture that may run repeatedly.

### Removing a handler with unsubscribe

`unsubscribe(event_type, handler)` removes the first registration matching that
handler and returns `True`; it returns `False` — never raises — when there is
nothing to remove:

```python
bus.subscribe(OrderPlaced, send_confirmation)

bus.unsubscribe(OrderPlaced, send_confirmation)   # True
bus.unsubscribe(OrderPlaced, send_confirmation)   # False — already gone
bus.unsubscribe(OrderShipped, send_confirmation)  # False — wrong event type
```

Matching is by **object identity** on the handler you originally passed, not by
equality. Two `AuditLog()` instances, or two separately-created bound methods,
are different handlers as far as the bus is concerned. That has one practical
consequence: a lambda or a locally-defined closure cannot be unsubscribed unless
you kept a reference to the exact object you subscribed.

```python
bus.subscribe(OrderPlaced, lambda e: print(e))
bus.unsubscribe(OrderPlaced, lambda e: print(e))  # False — a different object

# Keep the reference if you intend to remove it later
printer = lambda e: print(e)
bus.subscribe(OrderPlaced, printer)
bus.unsubscribe(OrderPlaced, printer)             # True
```

Because `unsubscribe()` removes a single registration, a handler subscribed
twice needs two calls to fully detach.

### Checking what is registered

`InMemoryEventBus` exposes the counts, which is the quickest way to assert your
wiring did what you expected:

```python
bus.subscribe(OrderPlaced, send_confirmation)
bus.subscribe(OrderPlaced, audit_log)
bus.subscribe(OrderShipped, audit_log)

bus.get_subscriber_count(OrderPlaced)  # 2
bus.get_subscriber_count()             # 3 — across all types
```

`get_subscriber_count()` counts type-specific handlers only; wildcard handlers
are reported separately by `get_wildcard_subscriber_count()`.

### Where to call subscribe

Subscription management is guarded by an internal `threading.RLock`, so
`subscribe()` and `unsubscribe()` are safe to call from any thread, including
while the loop is publishing. That does not make scattered registration a good
idea. Wire every subscription in one place at startup, next to where you
construct the bus, so the set of handlers for an event is readable from a single
file:

```python
# app/wiring.py
def register_handlers(bus: EventBus) -> None:
    bus.subscribe(OrderPlaced, ConfirmationSender())
    bus.subscribe(OrderPlaced, InventoryReserver())
    bus.subscribe(OrderShipped, ShipmentTracker())
```

Remember that handlers for the same event run concurrently and in no defined
order, so registration order carries no meaning. If `InventoryReserver` must run
before `ConfirmationSender`, that sequencing belongs inside a single handler —
not in two subscriptions.

## Subscribe a projection with subscribe_all

Calling `subscribe()` once per event type gets repetitive as soon as a
projection handles more than two or three types — and it duplicates knowledge
the projection already has. `subscribe_all()` inverts that: the subscriber
declares its own event types, and the bus reads them.

```python
bus.subscribe_all(order_projection)
```

Like `subscribe()`, it is synchronous, returns `None`, and has an identical
signature on all four backends.

### What the subscriber has to provide

The argument is a `FlexibleEventSubscriber` — anything with two methods:

- `subscribed_to() -> list[type[DomainEvent]]`
- `handle(event) -> Awaitable[None] | None`

That is a runtime-checkable `Protocol`, so you do not have to inherit from
anything. If you want the checked version, subclass the `EventSubscriber` ABC in
`eventsource.protocols`, which declares both methods abstract:

```python
from eventsource import DomainEvent
from eventsource.protocols import EventSubscriber


class OrderProjection(EventSubscriber):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        match event:
            case OrderPlaced():
                await self._on_placed(event)
            case OrderShipped():
                await self._on_shipped(event)
            case OrderCancelled():
                await self._on_cancelled(event)


bus.subscribe_all(OrderProjection())
```

### It is exactly a loop over subscribe()

Every backend implements the method the same way:

```python
event_types = subscriber.subscribed_to()
for event_type in event_types:
    self.subscribe(event_type, subscriber)
```

So `subscribe_all()` is sugar, not a different registration mechanism, and
everything from the previous section still applies:

- **The same object is registered under each type.** Dispatch calls
  `subscriber.handle(event)` for whichever type matched, so `handle()` must
  branch on the event type itself — there is no per-type routing at the bus.
- **Matching is still on the exact class.** Listing a base class in
  `subscribed_to()` does not pick up its subclasses. List every concrete event
  class you handle.
- **`subscribed_to()` is read once, at subscribe time.** A list computed later,
  or mutated after the call, changes nothing. Register after the subscriber is
  fully constructed.
- **An empty list registers nothing.** `subscribe_all()` on a subscriber whose
  `subscribed_to()` returns `[]` is a silent no-op — no error, no handler. If a
  projection mysteriously never fires, check what its `subscribed_to()` actually
  returns before suspecting the bus.
- **There is no deduplication.** Calling `subscribe_all(projection)` twice
  registers the projection twice per type, and `handle()` then runs twice per
  event. Wire it once at startup.

### Removing it again

There is no `unsubscribe_all()`. Undo a `subscribe_all()` by unsubscribing the
same object from each type it declared:

```python
for event_type in projection.subscribed_to():
    bus.unsubscribe(event_type, projection)
```

This works because `unsubscribe()` matches on object identity, and
`subscribe_all()` registered the identical object under every type. Keep the
reference to the subscriber instance if you intend to detach it later. On
`InMemoryEventBus`, `clear_subscribers()` is the blunter option in tests.

### Pairing it with DeclarativeProjection

Writing `subscribed_to()` by hand reintroduces the duplication you were trying
to remove — the list has to stay in sync with the handler methods.
`DeclarativeProjection` (from `eventsource`) closes that loop: it
discovers methods decorated with `@handles(EventType)` at construction time and
auto-generates `subscribed_to()` from them, so the decorators are the single
source of truth.

```python
from eventsource import DeclarativeProjection, handles


class OrderProjection(DeclarativeProjection):
    @handles(OrderPlaced)
    async def _on_placed(self, conn, event: OrderPlaced) -> None:
        ...

    @handles(OrderShipped)
    async def _on_shipped(self, conn, event: OrderShipped) -> None:
        ...


projection = OrderProjection()
projection.subscribed_to()   # [OrderPlaced, OrderShipped] — derived from @handles
bus.subscribe_all(projection)
```

Add a `@handles` method and the subscription list grows with it; no second edit,
and no chance of a handler that is never reached because someone forgot to list
its event type. `DeclarativeProjection` also inherits `CheckpointTrackingProjection`,
so its `handle()` already wraps your handler methods in retry-with-backoff and a
dead-letter fallback — a meaningful upgrade over a bare handler, whose exceptions
the bus merely logs and counts.

### Which of the three registration calls to use

| Call | Register when |
| --- | --- |
| `subscribe(event_type, handler)` | One handler, one or two event types, and the handler has no opinion about which types it wants. |
| `subscribe_all(subscriber)` | The component knows its own event types — projections, read models, saga-style processors. |
| `subscribe_to_all_events(handler)` | Genuinely type-agnostic cross-cutting work: audit logs, metrics, debug tracing. |

`subscribe_all()` is the default for anything projection-shaped. Reach for
`subscribe_to_all_events()` only when "every event" is the actual requirement —
not as a shortcut around listing types, since a wildcard handler is invoked for
every event on the bus and has to filter itself (see the next section).

`subscribe_all` is part of the `EventBus` contract exercised by
`EventBusConformanceSuite`, so any custom backend you write is checked for it
too (see [Test against the EventBus conformance suite](#test-against-the-eventbus-conformance-suite)).
