# 0007 - Event Bus Delivery Semantics and Tracing Contract

## Status

Accepted. The contract is declared by the `EventBus` ABC in
`src/eventsource/bus/interface.py` (whose docstring carries the tracing
convention) and summarised in `src/eventsource/bus/README.md`. It is implemented
by all four adapters shipped in `src/eventsource/bus/` -- `memory.py`,
`redis.py`, `rabbitmq.py`, and `kafka.py` -- using the tracer helpers in
`src/eventsource/observability/tracer.py` and the attribute constants in
`src/eventsource/observability/attributes.py`.

Compliance is pinned by `tests/unit/bus/test_eventbus_tracing_patterns.py`,
which inspects every adapter's source and signature, with
`tests/unit/bus/test_memory.py` and `tests/unit/bus/test_rabbitmq_tracing.py`
covering behaviour. The broker extras this ADR depends on are declared in
`[project.optional-dependencies]` in `pyproject.toml`.

This ADR describes the semantics as they exist today, including one known gap:
`RedisEventBus` does not yet propagate distributed trace context.

**Amended by [0011 - Uniform Handler-Error Isolation with `HandlerDispatchError`
and No-Ack-on-Failure](0011-handler-error-isolation-with-no-ack.md)**: D3
("Handler errors are caught, logged, and swallowed") and the "Do not expect
`await bus.publish(...)` to raise" consequence no longer hold as stated.
Isolation across handlers is unchanged; failures are now aggregated into a
raised `HandlerDispatchError`, and on the broker consume paths (Redis,
RabbitMQ, Kafka) an aggregate failure withholds the ack/commit so the
backend's existing redelivery mechanism runs, closing a silent
at-most-once gap on Redis.

**Amended by [0010 - Uniform Event Bus Contract: `background` Semantics and
`BaseEventBus`](0010-uniform-event-bus-contract.md)**: D4's description of
`InMemoryEventBus` owning its own `threading.RLock` no longer reflects the
implementation -- subscription state and its lock now live in the shared
`SubscriptionRegistry` inside `BaseEventBus`, used by all four backends.

## Context

`EventBus` is the fan-out seam of the library: aggregates and stores produce events, and projections, read models, and integration handlers consume them. Because the same abstract base class is implemented over an in-process dictionary *and* over three network brokers with very different semantics, the interface has to state plainly what it does and does not promise. Anything left implicit gets assumed away by users, and the assumption that bites hardest is "my handler runs exactly once, in order, and if it throws, publishing fails."

None of that is true. This ADR records what is actually guaranteed and why.

### The four adapters and what they actually are (InMemoryEventBus, RedisEventBus, RabbitMQEventBus, KafkaEventBus)

- **`InMemoryEventBus`** (`bus/memory.py`) -- a single-process registry: a `_subscribers` dict of `event_type -> handler adapters` plus an `_all_event_handlers` wildcard list, both guarded by a single `threading.RLock`. It is the reference implementation, the default in tests, and the only adapter with no broker, no retry, and no DLQ.
- **`RedisEventBus`** (`bus/redis.py`) -- Redis *Streams* (not Pub/Sub, despite the module-map line in `bus/README.md`), with a consumer group over `XADD` / `XREADGROUP` / `XACK`, an `XPENDING` + `XCLAIM` reclaim path for messages idle past `pending_idle_ms` (default 60000 ms), and a `_dlq`-suffixed stream (`<prefix>:stream_dlq`) once `max_retries` (default 3) is exceeded. Connection lifecycle is guarded by an `asyncio.Lock`.
- **`RabbitMQEventBus`** (`bus/rabbitmq.py`) -- AMQP via `aio-pika`'s `connect_robust` (reconnecting by construction), with per-consumer `prefetch_count` (default 10), an `x-retry-count` header driving exponential backoff with `retry_jitter` (default 0.1), and `_send_to_dlq` routing to a `_dlq`-suffixed exchange and a `.dlq` queue after `max_retries` (default 3). Also holds an `asyncio.Lock` over connection and channel setup.
- **`KafkaEventBus`** (`bus/kafka.py`) -- `aiokafka`, keyed by `aggregate_id` so events for one aggregate land on one partition, with `enable_auto_commit=False` and an explicit `commit()` *after* handler dispatch, a `KafkaRebalanceListener` that commits offsets in `on_partitions_revoked`, and `_send_to_dlq` publishing to a `.dlq`-suffixed topic.

Three of the four are network transports over brokers whose own delivery model is at-least-once. The fourth is not, but users move between them without rewriting handlers, so the contract must be the weakest of the set.

### Rationale currently lives only in docstrings (bus/interface.py, bus/README.md invariants list)

Today the rules are all written down. The reasoning behind them is not.

The `EventBus` class docstring in `bus/interface.py` carries the entire tracing convention as a numbered five-step "Tracing Support" recipe: inject a tracer via `create_tracer(__name__, enable_tracing)`, accept `enable_tracing: bool = True`, use the five `eventsource.event_bus.*` span names, use the `ATTR_*` constants from `eventsource.observability.attributes`, and -- for the distributed buses -- inject and extract trace context with `opentelemetry.propagate`. It ends with a worked `MyEventBus` example. It is a good docstring. But it is imperative throughout: every line says *do this*, and not one says *because*.

`bus/README.md` states the delivery semantics, as six one-line bullets under "Invariants": thread-safe, async-first, flexible handlers, no ordering guarantees, at-least-once delivery, optional tracing. Each bullet is a bare assertion. "At-least-once delivery: distributed buses may deliver events multiple times (handlers should be idempotent)" tells a reader what to do without telling them which broker behaviour forces it, or what happens if they ignore it. The same file's module map still describes `redis.py` as "using Redis Pub/Sub" when the implementation is Redis Streams with a consumer group -- exactly the kind of drift that survives when a one-liner has no argument attached to keep it honest.

`tests/unit/bus/test_eventbus_tracing_patterns.py` closes the loop by inspecting source: it asserts the ABC docstring still mentions "Tracing Support", `enable_tracing`, the span names, `ATTR_EVENT_TYPE`/`ATTR_EVENT_ID`/`ATTR_HANDLER_NAME`, and context propagation, then asserts the same literals appear in each adapter module. So the rules are enforced, and enforced against the docstring itself.

Enforcement is not explanation. A contributor writing a fifth adapter meets a wall of assertions -- span names must match this pattern, `OTEL_AVAILABLE` must not be redefined, the constructor must take these two parameters -- with no way to tell which are load-bearing and which are incidental. When a rule's cost shows up (a swallowed handler exception that nobody wanted swallowed, a span name that does not fit the broker's vocabulary), the natural move is to work around it, because nothing on record says what would break. Docstrings are also the wrong medium for the part that matters most here: the *rejected* options. There is no place in a class docstring to explain why exactly-once was declined, and no reader would look there.

This ADR is that missing argument. It restates the six invariants as six decisions, each with the source behaviour that forces it, the cost it imposes on users, and the alternative it beat.

## Decision

Six decisions, one per invariant in `bus/README.md`. Each states the rule, the source behaviour that forces it, and the cost it pushes onto callers.

### D1: At-least-once delivery, never exactly-once

A published event reaches each matching handler *at least* once. The bus never promises exactly-once, and no adapter attempts it.

#### Why not exactly-once: Redis Streams consumer-group redelivery, AMQP redelivery on nack, Kafka offset commit-after-process

Every broker's recovery mechanism is, structurally, a redelivery mechanism. Removing redelivery would mean removing recovery.

- **Redis Streams**: `RedisEventBus` reads via `XREADGROUP` and only calls `xack` after handlers have run. An entry that is read but not acked sits in the consumer group's pending list; `reclaim_pending_messages` finds it with `XPENDING`, and once it has been idle past `pending_idle_ms` (default 60000 ms) reclaims it with `XCLAIM` for another attempt. A consumer that dies between handling and acking gets its work redone by someone else.
- **AMQP**: `RabbitMQEventBus._handle_failed_message` republishes the message with an incremented `x-retry-count` header after a backoff delay, and any connection drop before `ack()` returns the unacked message to the queue. Both paths re-run the handler. `aio_pika.connect_robust` makes the second path routine rather than exceptional.
- **Kafka**: the consumer is built with `enable_auto_commit=False` and `commit()` is called *after* `_dispatch_to_handlers` returns. A crash in between re-consumes the offset on restart. Committing first would give at-most-once -- silently dropping work -- which is strictly worse in a system where the event store, not the bus, is the source of truth: a lost projection update is invisible, whereas a repeated one is something an idempotent handler absorbs.

Exactly-once at the bus boundary would require a bus-owned deduplication store consulted on every message. That is a durable dependency the bus does not have and should not acquire (see Alternatives).

#### Consequence for users: handlers must be idempotent

This is a requirement, not a recommendation. Applying an event twice must leave the same state as applying it once: upsert rather than insert, guard on `event_id`, or make the write naturally convergent. Projections that persist a checkpoint get most of this for free. Handlers with external side effects -- sending mail, charging a card, calling a partner API -- do not, and need their own dedup key derived from `event_id`.

### D2: No ordering guarantees across handlers on distributed buses

`bus/README.md` states it flatly: distributed buses do not guarantee event order across handlers. The guarantee that does exist is narrow and belongs to the in-memory bus only.

#### In-process ordering that IS guaranteed (events processed in list order; handlers per event before next event)

`InMemoryEventBus._publish_all` iterates the `events` list in order and awaits `_dispatch_event` for each, so event *N+1* is not dispatched until every handler for event *N* has settled. That is the whole guarantee, and it comes with two caveats visible in the same file. Within one event, `_invoke_handlers` runs handlers concurrently through `asyncio.gather(..., return_exceptions=True)`, so handlers for a single event are unordered relative to each other. And `publish(events, background=True)` hands the whole batch to `asyncio.create_task`, which returns immediately -- ordering *within* that batch still holds, but nothing orders it against subsequent publishes or against the caller's own reads.

#### What breaks the guarantee: partitioning, concurrent consumers, per-handler scheduling

On the three distributed adapters all three break it. Kafka keys records by `aggregate_id`, which preserves order within one aggregate's partition and says nothing across aggregates or topics. Redis consumer groups and RabbitMQ's `prefetch_count` (default 10) both put several messages in flight at once, across processes. And concurrent handler scheduling means two handlers watching the same stream can legitimately observe different interleavings.

The practical rule: derive correctness from the aggregate version recorded in the event store, never from the order in which the bus happened to deliver.

### D3: Handler errors are caught, logged, and swallowed -- one failing handler does not abort the publish

`InMemoryEventBus._safe_handle` wraps each handler invocation in `try/except Exception`. On failure it increments the `handler_errors` stat, logs at ERROR with `exc_info=True` and structured `extra` fields (handler, event type, event id, error), and returns normally. `publish()` does not raise. The abstract `EventBus.publish` docstring says the same thing: handler errors are caught and logged but do not prevent other handlers from executing.

#### Why isolation over fail-fast for a fan-out bus

A bus has N mutually unaware subscribers per event. Propagating the first exception would let an audit logger's disk-full error abort the read-model update, coupling subscribers that were deliberately decoupled -- and, because `asyncio.gather` collects results in task order, would make the observed failure depend on registration order. Isolation keeps each subscriber's failure domain its own, which is the reason to use a bus rather than a direct call in the first place.

#### Where failed events actually go (logging, ATTR_HANDLER_SUCCESS=False on the handle span, DLQ as the recovery path)

Swallowed is not silent. A failed handler emits three signals:

1. An ERROR log line with handler name, event type, and event id in `extra`.
2. An `eventsource.event_bus.handle` span carrying `ATTR_HANDLER_SUCCESS = False` plus `span.record_exception(e)` -- the mirror of `ATTR_HANDLER_SUCCESS = True` on the success path.
3. On the distributed adapters, retry-then-DLQ: `_send_to_dlq` writes to a `_dlq`-suffixed stream on Redis, a `.dlq` queue behind a `_dlq` exchange on RabbitMQ, and a `.dlq`-suffixed topic on Kafka, each after `max_retries` (default 3). Redis and RabbitMQ additionally expose replay helpers (`replay_dlq_message`, DLQ consumption with `x-retry-count` reset to 0).

The DLQ, not an exception out of `publish()`, is the recovery path. Anything monitoring this bus should watch the ERROR logs, the failed-handler spans, and DLQ depth -- not the return value of `publish`.

### D4: Thread-safety is a required invariant of every implementation

The `EventBus` docstring says implementations must be thread-safe and support both sync and async handlers; `bus/README.md` lists thread-safety first among the invariants. It is a precondition of the interface, not a per-adapter courtesy, because handlers are registered by application setup code that the library does not control.

#### threading.RLock for the synchronous in-memory subscription registry vs. asyncio.Lock for the async distributed connections

The two lock types guard different hazards, and the choice follows from which one an adapter actually has.

`InMemoryEventBus` holds a single `threading.RLock` over `_subscribers` and `_all_event_handlers` because `subscribe()`, `unsubscribe()`, and `clear()` are ordinary synchronous methods callable from any thread, while dispatch reads the registry from the event loop. `_dispatch_event` copies both handler lists into a local under the lock and releases it before awaiting, so no lock is ever held across an `await` -- the copy, not the lock duration, is what makes concurrent dispatch and re-subscription safe.

`RedisEventBus` and `RabbitMQEventBus` instead hold an `asyncio.Lock` over connection and channel lifecycle. Their hazard is not cross-thread mutation but concurrent coroutines in one loop racing to connect, reconnect, or declare topology. A `threading.Lock` there would block the loop; an `asyncio.Lock` in the in-memory registry would be unusable from the synchronous `subscribe()`.

### D5: A fixed tracing convention is part of the EventBus contract, not a suggestion

Span names and attribute keys are specified in the ABC docstring and enforced by `tests/unit/bus/test_eventbus_tracing_patterns.py`. An adapter that traces thoroughly but under names of its own choosing is non-compliant.

#### The five mandated span names: publish, dispatch, handle, consume, process

`eventsource.event_bus.publish`, `.dispatch`, `.handle`, `.consume`, `.process`. All five are `eventsource.event_bus.<verb>` -- exactly three dot-separated parts, a shape `TestEventBusSpanNamingConsistency` asserts directly.

The list is a closed vocabulary, not a checklist: adapters use the subset that fits their transport. `InMemoryEventBus` emits `dispatch` and `handle` (there is no wire operation to name). `RedisEventBus` emits `publish`, `process`, `dispatch`, and `handle`. `RabbitMQEventBus` emits `publish`, `consume`, and `handle`. `KafkaEventBus` emits `publish`, `consume`, and `dispatch`, with no `handle`.

Two places already stretch the rule and are worth knowing about before you copy them: Kafka appends the event type to the span name (`f"eventsource.event_bus.publish {event.event_type}"`, following the OTel messaging convention of `<operation> <destination>`), and RabbitMQ's batch path uses `eventsource.event_bus.publish_batch`, a sixth verb outside the documented five. The prefix holds in both cases; the exact-three-parts assertion is checked against the standard names, not scraped from the adapters.

#### The mandated ATTR_* constants from eventsource.observability.attributes

Attribute keys come from `eventsource/observability/attributes.py`, imported by name: `ATTR_EVENT_TYPE`, `ATTR_EVENT_ID`, `ATTR_AGGREGATE_ID`, `ATTR_HANDLER_NAME`, `ATTR_HANDLER_COUNT`, `ATTR_HANDLER_SUCCESS`, and for brokers `ATTR_MESSAGING_SYSTEM` and `ATTR_MESSAGING_DESTINATION`. Importing the constant rather than typing the string makes a rename one edit instead of a grep across four adapters, and keeps a dashboard written against one backend valid against the others.

#### Tracer injection by composition (create_tracer / NullTracer) rather than inheritance or a global

Every adapter takes a `tracer` and an `enable_tracing` flag (default `True`) -- directly in `__init__` for `InMemoryEventBus`, on the config object for the three broker adapters -- and resolves them with `self._tracer = tracer or create_tracer(__name__, enable_tracing)`. `create_tracer` returns an `OpenTelemetryTracer` when tracing is requested *and* `OTEL_AVAILABLE`, and a `NullTracer` otherwise. `NullTracer.span()` is a context manager yielding `None`, which is why call sites take the uniform shape:

```python
with self._tracer.span(name, attrs) as span:
    ...
    if span:
        span.set_attribute(ATTR_HANDLER_SUCCESS, True)
```

No branch on whether OpenTelemetry is installed, and no cost when it is not.

Composition beats the alternatives on three counts. Tests inject a `NullTracer` or a recording tracer through the constructor instead of patching module globals. There is no tracing mixin in the MRO to reason about when a broker client also wants to be a base class. And `OTEL_AVAILABLE` is defined once in `observability/tracing.py` and imported -- a duplication the test suite forbids outright, with `test_no_duplicate_otel_available` scanning each adapter's source for a local `OTEL_AVAILABLE = True/False` and `test_imports_otel_from_observability` asserting the import line is present.

#### Distributed context propagation: inject on publish, extract on consume

`RabbitMQEventBus` and `KafkaEventBus` both `from opentelemetry.propagate import extract, inject` behind a `PROPAGATION_AVAILABLE` flag, and carry W3C trace context in message headers: `inject(carrier)` into the header dict on publish, `extract(carrier)` on consume so the `consume` span continues the producer's trace rather than starting an orphan. Both guard every use with `if ... PROPAGATION_AVAILABLE and inject is not None`, so the path degrades to untraced rather than crashing on a core install.

`RedisEventBus` traces its own operations but does not inject or extract -- there is no propagation call anywhere in `bus/redis.py`. Producer and consumer spans on Redis are therefore separate traces. That is a known gap, not a decision, and closing it does not change this ADR.

#### How tests/unit/bus/test_eventbus_tracing_patterns.py enforces this by source inspection across all four adapters

The convention is checked mechanically rather than trusted. The test module uses `inspect.signature` to assert each adapter accepts `tracer` and `enable_tracing` (or that its config dataclass defaults `enable_tracing=True`), and `inspect.getsource` to assert the required span-name and `ATTR_*` literals appear in each module, that no module redefines `OTEL_AVAILABLE`, and that RabbitMQ and Kafka mention `inject`/`extract`. It also asserts the `EventBus` docstring still documents the tracing pattern, the span names, the standard attributes, and distributed propagation -- so the prose and the code cannot drift apart without a red test.

Source inspection is blunt: it proves a string is present, not that a span is emitted at the right moment with the right parent. It was chosen anyway because the honest alternative -- a live broker plus an OTel SDK per adapter -- cannot run in the unit tier, and the failure mode these tests exist to prevent is a new adapter quietly inventing its own vocabulary, which is exactly what a grep catches. Behavioural coverage lives alongside it (`test_memory.py`, `test_rabbitmq_tracing.py`) and in the integration suite.

### D6: All brokers stay optional extras; core install is pydantic + sqlalchemy only

A default `pip install eventsource-py` pulls pydantic and sqlalchemy and nothing else. Nobody pays -- in install size, transitive dependencies, or CVE surface -- for a broker client they do not run.

#### The extras map: redis, rabbitmq, kafka, kafka-schema-registry, telemetry, all

From `[project.optional-dependencies]` in `pyproject.toml`: `redis` (`redis>=5.0,<6.0`), `rabbitmq` (`aio-pika>=9.0.0`), `kafka` (`aiokafka>=0.9.0,<1.0.0`), `kafka-schema-registry` (aiokafka plus `confluent-kafka>=2.0.0,<3.0.0`), and `telemetry` (`opentelemetry-api` and `opentelemetry-sdk`, both `>=1.0,<2.0`), alongside the `postgresql` and `sqlite` store extras. `all` is the union of all six. OpenTelemetry being an extra rather than a core dependency is precisely why `NullTracer` has to exist.

#### Why guarded imports and skip-if-unavailable test fixtures follow from this

Each adapter wraps its client import in `try/except ImportError` and sets a module flag -- `REDIS_AVAILABLE`, `RABBITMQ_AVAILABLE`, `KAFKA_AVAILABLE`, and `OTEL_AVAILABLE` centrally -- so `import eventsource.bus.redis` succeeds on a core install. The failure is deferred to construction, where each adapter raises a dedicated `ImportError` subclass (`RedisNotAvailableError`, `RabbitMQNotAvailableError`, `KafkaNotAvailableError`) whose message names the exact extra to install. An import-time crash would tell the user the wrong thing at the wrong time; a named constructor error tells them what to type.

The tests mirror the same structure: each adapter's compliance class has a fixture that `pytest.skip`s when its package is missing. The one unit suite therefore passes unchanged on a core install and on `[all]`, covering whatever happens to be present.

## Consequences

### For users writing handlers

Make handlers idempotent -- this is the price of at-least-once, and it is not optional. Do not rely on cross-aggregate ordering; if you need a sequence, read it from the event store. Do not expect `await bus.publish(...)` to raise when a handler fails: it will not, so monitor the ERROR logs, the `ATTR_HANDLER_SUCCESS = False` spans, and the DLQ instead. Pass `enable_tracing=False` (or an explicit `NullTracer`) in tests and benchmarks where span overhead is unwanted.

### For contributors adding a fifth adapter

Subclass `EventBus`; accept both `tracer` and `enable_tracing=True`; resolve them through `create_tracer`; import `OTEL_AVAILABLE` from `eventsource.observability` rather than redefining it; use only the five mandated span names and the `ATTR_*` constants; guard the client import behind an `*_AVAILABLE` flag and raise a named error naming the extra; add the extra to `pyproject.toml` and fold it into `all`; make the implementation thread-safe; and add a compliance test class plus a skip fixture to `test_eventbus_tracing_patterns.py`. If your broker can propagate headers, inject on publish and extract on consume.

### For observability backends consuming these spans

Span names and attribute keys are stable across adapters, so one dashboard works for all of them: filter on `eventsource.event_bus.*`, break down by `ATTR_MESSAGING_SYSTEM` to separate backends, and alert on `eventsource.handler.success = false`. Where propagation is wired (RabbitMQ, Kafka), producer and consumer spans join into one trace; on Redis they do not yet.

## Alternatives Considered

### Exactly-once via a bus-level dedup store

Rejected. It would give the bus a durable dependency of its own -- another store to provision, migrate, and reason about -- and would still only be exactly-once at the bus boundary, since a handler with an external side effect can fail after the dedup record commits. Idempotent handlers solve the same problem where the knowledge actually lives.

### Per-handler retry / propagate-first-error instead of swallow-and-log

Rejected for the in-process dispatch path. Propagating couples independent subscribers and makes behaviour depend on registration order. Retry at dispatch time also duplicates what the brokers already do better, with delivery-count tracking and durable state: `x-retry-count` on RabbitMQ, the pending-entries list on Redis, offsets on Kafka. Retry belongs at the transport layer, escalating to a DLQ; the dispatch layer's job is isolation and reporting.

### Free-form span names per adapter

Rejected. Backend-specific names would make every dashboard and alert backend-specific too, and would defeat the point of a shared `EventBus` interface -- swapping Redis for Kafka would silently break observability. The fixed vocabulary costs a small amount of expressiveness and buys portability.

### Vendoring brokers as core dependencies

Rejected. It would force `redis`, `aio-pika`, `aiokafka`, and `confluent-kafka` on users of the in-memory bus, expand the install size and CVE surface, and pin version ranges for clients the application may already manage itself. Extras plus guarded imports keep the core at pydantic + sqlalchemy.

## References

- `src/eventsource/bus/interface.py` -- `EventBus` ABC; the tracing contract in its docstring
- `src/eventsource/bus/README.md` -- the invariants list
- `src/eventsource/bus/memory.py` -- reference implementation; `_safe_handle`, `_invoke_handlers`, `threading.RLock`
- `src/eventsource/bus/redis.py`, `rabbitmq.py`, `kafka.py` -- the distributed adapters, retry, and DLQ paths
- `src/eventsource/observability/tracer.py` -- `create_tracer`, `NullTracer`, `OpenTelemetryTracer`
- `src/eventsource/observability/attributes.py` -- the `ATTR_*` constants
- `tests/unit/bus/test_eventbus_tracing_patterns.py` -- source-inspection enforcement
- `pyproject.toml` -- `[project.optional-dependencies]`
- `docs/core-surface.md` -- the Tier 0 dependency boundary
