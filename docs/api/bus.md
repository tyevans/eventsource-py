# Event Bus API Reference

Reference documentation for the event distribution layer exported from
`eventsource.bus`: the abstract `EventBus` base class, the handler and subscriber
types its methods accept, and the four bundled implementations —
`InMemoryEventBus`, `RedisEventBus`, `RabbitMQEventBus`, and `KafkaEventBus` —
together with their config objects, stats objects, availability flags, and
backend-specific errors.

Public names covered here:

| Name | Kind | Purpose |
| --- | --- | --- |
| `EventBus` | ABC | Abstract publish/subscribe contract implemented by every bus |
| `EventHandlerFunc` | type alias | Callable signature accepted by the subscribe methods |
| `EventHandler` / `AsyncEventHandler` / `FlexibleEventHandler` | protocol / ABC | Handler contracts re-exported from `eventsource.ports.handlers` |
| `EventSubscriber` / `FlexibleEventSubscriber` | protocol | Multi-event subscriber contracts used by `subscribe_all()` |
| `InMemoryEventBus` | class | In-process bus for development, tests, and single-instance deployments |
| `RedisEventBus`, `RedisEventBusConfig`, `RedisEventBusStats` | classes | Redis Streams bus, its configuration, and its counters |
| `RedisNotAvailableError`, `REDIS_AVAILABLE` | exception / flag | Redis dependency guard |
| `RabbitMQEventBus`, `RabbitMQEventBusConfig`, `RabbitMQEventBusStats` | classes | RabbitMQ bus, its configuration, and its counters |
| `RabbitMQNotAvailableError`, `RABBITMQ_AVAILABLE` | exception / flag | `aio-pika` dependency guard |
| `KafkaEventBus`, `KafkaEventBusConfig`, `KafkaEventBusStats` | classes | Kafka bus, its configuration, and its counters |
| `KafkaNotAvailableError`, `KAFKA_AVAILABLE` | exception / flag | `aiokafka` dependency guard |

```python
from eventsource.bus import (
    EventBus,
    EventHandlerFunc,
    EventHandler,
    AsyncEventHandler,
    FlexibleEventHandler,
    EventSubscriber,
    FlexibleEventSubscriber,
    InMemoryEventBus,
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
    RedisNotAvailableError,
    REDIS_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
    RABBITMQ_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusStats,
    KafkaNotAvailableError,
    KAFKA_AVAILABLE,
)
```

The bus classes, config objects, and stats objects are also re-exported from the
top-level `eventsource` package; the handler and subscriber protocols live
canonically in `eventsource.ports.handlers` and are re-exported by `eventsource.bus`.

`EventBus.publish()` is `async`; the subscription methods (`subscribe`,
`unsubscribe`, `subscribe_all`, `subscribe_to_all_events`,
`unsubscribe_from_all_events`) are ordinary synchronous methods. Every
implementation module is imported unconditionally, so all names above import
successfully with only the core dependencies installed — the optional backends
fail at construction time instead, via their `*NotAvailableError`.

Behavior described below is that of the current source in
`src/eventsource/bus/interface.py`, `memory.py`, `redis.py`, `rabbitmq.py`, and
`kafka.py`, plus the protocols in `src/eventsource/protocols.py`.

## Overview

Every bus in this module implements the same six-method `EventBus` contract:
one async `publish()` plus five synchronous subscription-management methods.
The differences between implementations are transport, durability, and
lifecycle — not API shape. `InMemoryEventBus` is usable immediately after
construction; the three distributed buses must be connected with `await
bus.connect()` before publishing and started with `await bus.start_consuming()`
before they will deliver events to their handlers.

### Import surface (`eventsource.bus.__all__`)

`eventsource.bus.__all__` contains 23 names, grouped as follows.

| Group | Count | Names |
| --- | --- | --- |
| Interface | 2 | `EventBus`, `EventHandlerFunc` |
| Handler / subscriber contracts | 5 | `EventHandler`, `AsyncEventHandler`, `FlexibleEventHandler`, `EventSubscriber`, `FlexibleEventSubscriber` |
| In-memory | 1 | `InMemoryEventBus` |
| Redis | 5 | `RedisEventBus`, `RedisEventBusConfig`, `RedisEventBusStats`, `RedisNotAvailableError`, `REDIS_AVAILABLE` |
| RabbitMQ | 5 | `RabbitMQEventBus`, `RabbitMQEventBusConfig`, `RabbitMQEventBusStats`, `RabbitMQNotAvailableError`, `RABBITMQ_AVAILABLE` |
| Kafka | 5 | `KafkaEventBus`, `KafkaEventBusConfig`, `KafkaEventBusStats`, `KafkaNotAvailableError`, `KAFKA_AVAILABLE` |

`EventBus` and `EventHandlerFunc` come from `eventsource.bus.interface`. All
five handler and subscriber contracts are re-exports from the canonical
`eventsource.ports.handlers` module — `EventHandler`, `FlexibleEventHandler`, and
`FlexibleEventSubscriber` are `Protocol`s; `EventSubscriber` and
`AsyncEventHandler` are ABCs. Each backend module contributes exactly five
names: its bus class, config dataclass, stats dataclass, dependency-guard
error, and availability flag.

Some names documented in this reference are *not* in `__all__` and must be
imported from their defining module rather than from the package root:

| Name | Kind | Module |
| --- | --- | --- |
| `KafkaEventBusMetrics` | metrics holder | `eventsource.bus.kafka` |
| `EventSerializer`, `DeserializationError` | serialization | `eventsource.bus.kafka` |
| `KafkaRebalanceListener` | consumer rebalance hook | `eventsource.bus.kafka` |
| `DLQMessage`, `QueueInfo`, `HealthCheckResult` | inspection results | `eventsource.bus.rabbitmq` |
| `ShutdownError`, `BatchPublishError` | errors | `eventsource.bus.rabbitmq` |

`eventsource/bus/__init__.py` imports all four implementation modules
unconditionally. Each optional backend wraps its third-party import in
`try`/`except ImportError`, sets its `*_AVAILABLE` flag accordingly, and binds
the missing symbols to `None`. The consequence is that `from eventsource.bus
import KafkaEventBus` always succeeds, even with no Kafka client installed —
`KafkaEventBus(...)` is what raises `KafkaNotAvailableError` (an `ImportError`
subclass). Check the flag if you need to branch before construction.

### Choosing an implementation (in-memory vs Redis vs RabbitMQ vs Kafka)

| | `InMemoryEventBus` | `RedisEventBus` | `RabbitMQEventBus` | `KafkaEventBus` |
| --- | --- | --- | --- | --- |
| Transport | Python objects in-process | Redis Streams | AMQP exchange + queues | Kafka topics |
| Extra | none (core install) | `eventsource-py[redis]` | `eventsource-py[rabbitmq]` | `eventsource-py[kafka]` |
| Driver | — | `redis>=5.0,<6.0` | `aio-pika>=9.0.0` | `aiokafka>=0.9.0,<1.0.0` |
| Scope | single process | multi-process / multi-host | multi-process / multi-host | multi-process / multi-host |
| Durability | none — events are lost on restart | events persisted in the stream | durable exchange and queues | durable topic log |
| Delivery | in-process call, once per handler | at-least-once | at-least-once | at-least-once |
| Ordering | publication order within a call | stream order | queue order | per-partition, keyed by `aggregate_id` |
| Explicit lifecycle | none | `connect()` / `start_consuming()` | `connect()` / `start_consuming()` | `connect()` / `start_consuming()` |
| Destination naming | — | stream `{stream_prefix}:stream` | exchange `{exchange_name}`, queue `{exchange_name}.{consumer_group}` | topic `{topic_prefix}.stream` |
| DLQ | none | `{stream_prefix}:stream{dlq_stream_suffix}` (default `_dlq`) | `{exchange_name}{dlq_exchange_suffix}` exchange + `.dlq` queue | `{topic_prefix}.stream{dlq_topic_suffix}` |
| Defaults | — | `stream_prefix="events"` | `exchange_type="topic"`, `durable=True` | `topic_prefix="events"` |
| Event registry | not needed | `event_registry` for deserialization | `event_registry` for deserialization | `event_registry` for deserialization |

Guidance implied by the source:

- **`InMemoryEventBus`** is documented for "development, testing, and
  single-instance deployments." It has no wire format, so it needs no event
  registry and no serialization; it also provides the test-support surface
  (`published_events`, `clear_published_events()`) that the distributed buses
  do not.
- **`RedisEventBus`** is the lightest distributed option: consumer groups for
  load balancing, event replay, pending-message recovery via
  `recover_pending_messages()`, and a DLQ stream. Choose it when Redis is
  already part of the stack.
- **`RabbitMQEventBus`** adds routing flexibility — the exchange type is
  configurable (`topic` by default, plus `direct`, `fanout`, `headers`) and
  handlers can be bound by event type or raw routing key. It is the option to
  pick when consumers need selective subscriptions rather than reading the whole
  stream, and it exposes the richest operational surface (`HealthCheckResult`,
  `QueueInfo`, `DLQMessage`).
- **`KafkaEventBus`** targets high throughput and horizontal scaling, with
  partition-based ordering keyed by `aggregate_id`, a consumer rebalance
  listener, pluggable serialization via `EventSerializer`, and optional
  OpenTelemetry metrics in addition to tracing.

All three distributed buses provide **at-least-once** delivery, so handlers must
be idempotent. None of them coordinates transactionally with the event store —
the Kafka module states explicitly that exactly-once semantics are not
supported and that a write to the store succeeding while the publish fails (or
the reverse) is possible. Use the transactional outbox
(`eventsource.ports.outbox.OutboxRepository` and its adapters) when
publishing must be atomic with the state change.

Tracing is available on every implementation: `InMemoryEventBus` takes `tracer`
and `enable_tracing` constructor keyword arguments directly, while the
distributed buses take `tracer` as a keyword argument and read
`enable_tracing` from their config dataclass (default `True` in each case).

## `EventBus` (abstract base class)

```python
from eventsource.bus import EventBus
```

Defined in `src/eventsource/bus/interface.py`. `EventBus` is an
`abc.ABC` with six abstract methods and no concrete behavior, no
constructor, and no state of its own — subclasses must implement all six
before they can be instantiated.

| Method | Async | Returns | Purpose |
| --- | --- | --- | --- |
| `publish(events, background=False)` | yes | `None` | Deliver a list of events to subscribers |
| `subscribe(event_type, handler)` | no | `None` | Register a handler for one event type |
| `unsubscribe(event_type, handler)` | no | `bool` | Remove a per-type registration |
| `subscribe_all(subscriber)` | no | `None` | Register a subscriber for every type it declares |
| `subscribe_to_all_events(handler)` | no | `None` | Register a wildcard handler |
| `unsubscribe_from_all_events(handler)` | no | `bool` | Remove a wildcard registration |

Only `publish()` is a coroutine. The five subscription-management methods are
ordinary synchronous methods and can be called before an event loop exists —
which is why wiring a bus up at import or startup time needs no `await`.

The class docstring states two requirements on implementations: they **must be
thread-safe**, and they **must support both synchronous and asynchronous
handlers**. The second requirement is why every subscription method's `handler`
parameter is typed `FlexibleEventHandler | EventHandlerFunc` — an object whose
`handle()` returns either `None` or an awaitable, or a bare callable with the
same latitude. Implementations are responsible for detecting which they got and
awaiting only when needed; `InMemoryEventBus` does this with an internal handler
adapter.

The contract deliberately says nothing about transport, durability, ordering
across processes, or delivery guarantees. Those are implementation concerns,
documented per backend later in this reference. What the base class does pin
down is the error-handling posture: the `publish()` docstring notes that
**handler errors are caught and logged but do not prevent other handlers from
executing**. A failing subscriber degrades its own projection, not the publish
call and not its peers.

### Implementing a custom bus

```python
from eventsource.bus import EventBus, EventHandlerFunc
from eventsource.events.base import DomainEvent
from eventsource.ports.handlers import FlexibleEventHandler, FlexibleEventSubscriber


class MyEventBus(EventBus):
    async def publish(
        self, events: list[DomainEvent], background: bool = False
    ) -> None: ...

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None: ...

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool: ...

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None: ...

    def subscribe_to_all_events(
        self, handler: FlexibleEventHandler | EventHandlerFunc
    ) -> None: ...

    def unsubscribe_from_all_events(
        self, handler: FlexibleEventHandler | EventHandlerFunc
    ) -> bool: ...
```

Beyond the six methods, the docstring gives implementors two further
expectations, neither enforced by the ABC:

- **Tracing.** Implementations *should* compose in a `Tracer` from
  `eventsource.observability` and accept an `enable_tracing` constructor
  argument. The span names and attribute constants are fixed by convention and
  are spelled out under
  [Tracing contract for implementors](#tracing-contract-for-implementors-tracer--enable_tracing-constructor-kwargs).
- **Lifecycle.** `EventBus` declares no `connect()`, `disconnect()`, or
  `shutdown()`. Backends that need them add them on top; code written against
  the bare `EventBus` type therefore cannot assume any of these exist.

`InMemoryEventBus` is named in the docstring as the reference implementation,
including for the tracing pattern.

### Typical usage

```python
event_bus = InMemoryEventBus()

event_bus.subscribe(OrderCreated, order_handler)   # one type
event_bus.subscribe_all(order_projection)          # every type it declares
event_bus.subscribe_to_all_events(audit_logger)    # wildcard

await event_bus.publish([OrderCreated(...)])
```

The subsections that follow document each method's parameters, return value,
and cross-backend behavior in turn.

### `publish(events: list[DomainEvent], background: bool = False) -> None`

```python
await bus.publish([OrderCreated(...), OrderShipped(...)])
```

The only coroutine on the `EventBus` ABC. Takes a **list** of events — there is
no single-event overload; publishing one event means `publish([event])`. Returns
`None`; there is no per-event result, message id, or ack object in the public
contract.

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `events` | `list[DomainEvent]` | required | Events to deliver, processed in list order |
| `background` | `bool` | `False` | Relax the "wait for delivery" guarantee — interpreted per backend |

**Empty list is a no-op.** Every implementation returns early on `if not
events`, so `await bus.publish([])` never touches the transport and never
increments counters.

#### Ordering and error semantics

The ABC docstring fixes two properties. First, **events are processed in
order, and all handlers for one event run before the next event is
dispatched** — `InMemoryEventBus._publish_all()` implements this literally as a
sequential `for` loop over the list, awaiting `_dispatch_event()` for each.
Second, **handler errors are caught and logged but do not prevent other handlers
from executing**: one failing subscriber degrades only itself, not its peers and
not the `publish()` call.

That error isolation is about *handlers*. Failures in the publish path itself —
serialization, or the broker rejecting the write — do propagate. The docstring's
`Raises` clause notes that critical publishing failures surface "only in
synchronous mode": once `background=True` hands the work off, there is no caller
left to raise into, and the error is logged instead.

#### What `background=True` actually does

The flag's contract is "don't block the caller on delivery," but each backend
honors it differently, and one ignores it. Read the row for the bus you use.

| Bus | Effect of `background=True` |
| --- | --- |
| `InMemoryEventBus` | Wraps dispatch in `asyncio.create_task(...)` and returns immediately. The task is retained in an internal set, counted by `get_background_task_count()` and the `background_tasks_created` / `background_tasks_completed` stats, and awaited by `shutdown()`. Exceptions escaping the task are logged by a done-callback. |
| `RedisEventBus` | **Ignored.** The docstring states "Ignored for Redis (Redis is inherently async)"; the `XADD` is awaited either way. |
| `RabbitMQEventBus` | Controls **publisher confirms**, not task spawning — it passes `wait_for_confirm=not background` down to `_publish_single()` / `_publish_batch()`. The message is still handed to the broker before `publish()` returns; the broker's ack is simply not awaited. |
| `KafkaEventBus` | Skips awaiting the producer future. Delivery is tracked by a callback (`_track_background_publish`) that logs errors and updates metrics, so failures are observable but not raisable. |

Two consequences worth internalizing. `background=True` buys response time at
the cost of **eventual consistency** — the `InMemoryEventBus` docstring calls
this out directly: a read immediately after a background publish may see stale
projection state. And with `InMemoryEventBus`, a process that exits without
calling `await bus.shutdown()` can drop scheduled tasks; see
[`shutdown(timeout)`](#shutdowntimeout-float--300).

#### Connection behavior across backends

The ABC says nothing about connections, and the three distributed buses disagree
on what `publish()` does when disconnected:

- **`RedisEventBus`** and **`RabbitMQEventBus`** auto-connect: both call `await
  self.connect()` when `self._connected` is false, then proceed. RabbitMQ
  additionally raises `RuntimeError("Exchange not initialized")` if the exchange
  is still missing after connecting.
- **`KafkaEventBus`** does not. It raises `RuntimeError("Not connected to
  Kafka. Call connect() first.")` before anything else — including before the
  empty-list check, so even `await bus.publish([])` raises on a disconnected
  Kafka bus.

#### Batching and keys

Implementations optimize multi-event calls rather than looping naively:

- `RedisEventBus` uses a single event for a plain `XADD` and switches to a
  non-transactional pipeline for two or more, executing all the `XADD`s in one
  network round-trip (documented as "10-100x faster for multiple events").
- `RabbitMQEventBus` publishes one event directly and routes lists through
  `_publish_batch()`, which publishes concurrently via `asyncio.gather()` under
  a semaphore.
- `KafkaEventBus` sends events one at a time but keys each message by
  `aggregate_id`, which is what gives per-aggregate ordering within a partition.
  It records a `batch_size` histogram and a `publish_duration` histogram per
  call when metrics are enabled.

#### Tracing

Every implementation opens a span named `eventsource.event_bus.publish` around
the call. The Redis implementation is representative: attributes for event
count, `messaging.system`, and `messaging.destination` (the stream name), plus a
`publish.success` boolean and a recorded exception on failure. Distributed buses
also inject trace context into message headers so consumers can continue the
trace. See
[Tracing contract for implementors](#tracing-contract-for-implementors-tracer--enable_tracing-constructor-kwargs).

#### Stats

Successful publishes increment a counter you can read back:
`InMemoryEventBus` bumps `events_published` in `get_stats()` (and appends to
`published_events` for test assertions), while `RedisEventBus`,
`RabbitMQEventBus`, and `KafkaEventBus` increment `events_published` on their
respective stats dataclasses. Kafka also stamps `last_publish_at`.

### `subscribe(event_type: type[DomainEvent], handler: FlexibleEventHandler | EventHandlerFunc) -> None`

```python
bus.subscribe(OrderCreated, order_handler)
bus.subscribe(OrderCreated, lambda e: print(e))
```

Registers one handler for one event type. Synchronous — no `await`, no event
loop required, so a bus can be wired up at import or startup time. Returns
`None`.

| Parameter | Type | Meaning |
| --- | --- | --- |
| `event_type` | `type[DomainEvent]` | The event **class** to subscribe to, not an instance and not a name |
| `handler` | `FlexibleEventHandler \| EventHandlerFunc` | An object with a `handle()` method, or a plain callable taking one `DomainEvent` |

`subscribe()` is additive: **multiple handlers may be registered for the same
event type**, and they are stored in a list in registration order. Registering
the *same* handler object twice appends it twice, and it will then be invoked
twice per event — there is no deduplication.

#### What counts as a handler

Every implementation immediately wraps the argument in a `HandlerAdapter`
(`src/eventsource/handlers/adapter.py`), which normalizes four shapes to a
single async interface:

| Shape | Example | Normalization |
| --- | --- | --- |
| Object with `async def handle(event)` | a `DeclarativeProjection` | bound method used directly |
| Object with `def handle(event)` | a sync projection | wrapped in an async wrapper; a returned coroutine is still awaited |
| Async callable | `async def on_order(e): ...` | used directly |
| Sync callable | `lambda e: print(e)` | wrapped in an async wrapper |

Anything else — no `handle()` attribute and not callable — raises `TypeError`
from the adapter constructor, so **`subscribe()` fails immediately at
registration time** rather than at publish time:

```
TypeError: Handler must have a handle() method or be callable, got <class 'int'>
```

Note the dispatch rule implied by `_normalize()`: if the object has a `handle`
attribute, that attribute is used and the object is never called directly, even
if it is also callable.

The adapter also derives a `name` (class name for instances, `__name__` for
functions, `repr()` otherwise) used in the log line every implementation emits
on registration, and defines `__eq__`/`__hash__` in terms of the **identity** of
the original handler. That identity rule is what makes
[`unsubscribe()`](#unsubscribeevent_type-typedomainevent-handler-flexibleeventhandler--eventhandlerfunc---none)
work: you must pass the same object you subscribed, since a fresh
`lambda e: print(e)` is a different object from the one registered.

#### Matching is by exact type

Handlers are keyed by the event class itself. `InMemoryEventBus`,
`RedisEventBus`, and `RabbitMQEventBus` look up `self._subscribers.get(type(event))`;
`KafkaEventBus` keys its `_handlers` dict by `event_type.__name__` instead
(because the type is recovered from the wire by name). Either way the match is
exact — **subscribing to a base event class does not receive its subclasses**.
Register each concrete class you care about, or use
[`subscribe_to_all_events()`](#subscribe_to_all_eventshandler-flexibleeventhandler--eventhandlerfunc---none)
for a wildcard.

Per-type handlers run before wildcard handlers: the dispatch path builds
`specific_handlers + wildcard_handlers` and invokes that list in order.

#### Behavior per backend

The signature and registration semantics are identical everywhere; what differs
is where the events come from.

| Bus | Notes |
| --- | --- |
| `InMemoryEventBus` | Registration takes the bus's lock and is explicitly documented as thread-safe — callable from any thread. Handlers fire on the next `publish()`. |
| `RedisEventBus` | "Subscriptions are registered locally. The actual consumption happens via `start_consuming()`." Registration is a plain list append (relying on the GIL), because `subscribe()` is sync and the bus's lock is an `asyncio.Lock`. |
| `RabbitMQEventBus` | Same local-registration note. `subscribe()` registers a *handler*; it does **not** create an AMQP binding. With the default `topic` exchange and `#` pattern the queue already receives everything, but on a `direct` exchange or a restrictive `routing_key_pattern` you must also `await bus.bind_event_type(OrderCreated)` or the message will never reach the queue for the handler to see. |
| `KafkaEventBus` | Handlers are in-memory only and never persisted to Kafka; keyed by `event_type.__name__`. Subscribing does not require a connection. |

For the three distributed buses, subscribing on its own delivers nothing —
`await bus.connect()` and `await bus.start_consuming()` are what turn a
registration into invocations.

#### Counting registrations

`InMemoryEventBus.get_subscriber_count(event_type=None)` returns the count for
one type, or the total across all types when called with no argument;
wildcard handlers are excluded either way and counted by
`get_wildcard_subscriber_count()`. `RedisEventBus` and `RabbitMQEventBus`
expose an equivalent per-type count, and `KafkaEventBus` counts by event type
name.

#### Related

- [`subscribe_all()`](#subscribe_allsubscriber-flexibleeventsubscriber---none) —
  convenience wrapper that calls `subscribe()` once per type a subscriber
  declares via `subscribed_to()`.
- [`unsubscribe()`](#unsubscribeevent_type-typedomainevent-handler-flexibleeventhandler--eventhandlerfunc---none) —
  removes a single registration, returning `bool`.

### `unsubscribe(event_type: type[DomainEvent], handler: FlexibleEventHandler | EventHandlerFunc) -> None`

```python
removed = bus.unsubscribe(OrderCreated, order_handler)
```

Removes one previously registered per-type handler. Synchronous, like
`subscribe()`. **The source returns `bool`, not `None`** — `True` if a matching
registration was found and removed, `False` if there was nothing to remove. The
outline's `-> None` is inaccurate; every implementation and the ABC declare
`-> bool`.

| Parameter | Type | Meaning |
| --- | --- | --- |
| `event_type` | `type[DomainEvent]` | The event class the handler was registered against |
| `handler` | `FlexibleEventHandler \| EventHandlerFunc` | The **same object** passed to `subscribe()` |

#### Matching is by identity, not equality

`unsubscribe()` wraps the argument in a `HandlerAdapter` and scans the list
registered for `event_type`, comparing adapters. `HandlerAdapter.__eq__`
(`src/eventsource/handlers/adapter.py`) compares `self._original is
other._original` — object identity — and `__hash__` is `id(self._original)`.
The `KafkaEventBus` docstring states the rule outright: "Handlers are compared
by identity (using `is`), not equality. This means you must pass the exact same
handler object that was used in `subscribe()`."

The practical trap is anonymous handlers:

```python
bus.subscribe(OrderCreated, lambda e: print(e))
bus.unsubscribe(OrderCreated, lambda e: print(e))  # False — different object
```

Keep a reference if you intend to unsubscribe:

```python
handler = lambda e: print(e)
bus.subscribe(OrderCreated, handler)
bus.unsubscribe(OrderCreated, handler)   # True
bus.unsubscribe(OrderCreated, handler)   # False — already removed
```

Because the adapter's identity is that of the *original* handler, it does not
matter that `subscribe()` and `unsubscribe()` each build a throwaway adapter —
two adapters wrapping the same handler object compare equal.

#### Removes one registration, not all

The loop `pop`s at the first match and returns immediately. If the same handler
was subscribed to the same event type twice, one `unsubscribe()` call removes
one registration and leaves the other in place; call it again to remove the
second. Handlers registered for *other* event types, and the same handler's
wildcard registration, are untouched —
[`unsubscribe_from_all_events()`](#unsubscribe_from_all_eventshandler-flexibleeventhandler--eventhandlerfunc---none)
is the wildcard counterpart.

Removing the last handler for a type does not delete the dict entry; the key
survives with an empty list. That is invisible through the public API —
`get_subscriber_count(OrderCreated)` reports `0` either way.

#### Never raises

Unsubscribing something that was never subscribed is not an error. A missing
event type, a missing handler, or a bus with no subscribers at all all yield
`False` plus a `logger.debug` line ("Handler ... not found for ..."). A
successful removal logs at `INFO` on the in-memory, Redis, and RabbitMQ buses
and at `DEBUG` on Kafka.

Note the asymmetry with `subscribe()`: registration validates the handler and
raises `TypeError` for something that is neither callable nor has a `handle()`
method. `unsubscribe()` builds a `HandlerAdapter` the same way, so passing a
non-handler object raises `TypeError` from the adapter constructor rather than
returning `False`.

#### Behavior per backend

| Bus | Notes |
| --- | --- |
| `InMemoryEventBus` | Performs the scan under the bus's `threading` lock and is documented as thread-safe — callable from any thread. |
| `RedisEventBus` | Local registry mutation only; nothing is sent to Redis. Consumer group membership and the stream are unaffected. |
| `RabbitMQEventBus` | Removes the handler but does **not** remove any AMQP binding created by `bind_event_type()` / `bind_routing_key()`. Messages of that type keep arriving at the queue and are simply dispatched to no handler. |
| `KafkaEventBus` | Looks the type up by `event_type.__name__`; returns `False` early if no list exists for that name. Topic subscription and partition assignment are unaffected. |

For the distributed buses this means unsubscribing stops *dispatch*, not
*consumption*: the bus keeps reading and acknowledging messages.

#### Bulk removal

To drop everything rather than one registration, each implementation offers
`clear_subscribers()`, which empties both the per-type registry and the
wildcard list. It is not part of the `EventBus` ABC — code typed against the
base class cannot call it.

#### Related

- [`subscribe()`](#subscribeevent_type-typedomainevent-handler-flexibleeventhandler--eventhandlerfunc---none) —
  the registration this reverses.
- [`unsubscribe_from_all_events()`](#unsubscribe_from_all_eventshandler-flexibleeventhandler--eventhandlerfunc---none) —
  same identity semantics, applied to the wildcard list.
