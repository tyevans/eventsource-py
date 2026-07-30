# Broker Backend God-Class Decomposition — Design

**Date:** 2026-07-29
**Status:** Approved
**Target version:** 0.7.0
**Predecessor:** `2026-07-29-event-bus-contract-and-coverage-design.md` (shipped as PR #68 / 0.6.0), which deliberately deferred this work.

## Problem

`RabbitMQEventBus` (4,228-line module; the class itself ~3,570 lines) and
`KafkaEventBus` (3,141-line module; class ~2,195 lines) are god classes.
Each mixes connection lifecycle, topology declaration, serialization,
publish paths, consume/dispatch, retry/DLQ write paths, DLQ administration,
health checks, and stats into one object with 15-20 instance fields.
Symptoms:

- `RabbitMQEventBus.health_check` (147 lines) reads connection, topology,
  stats, and config state in one method.
- `_process_message` (Rabbit, 194 lines) and `_dispatch_to_handlers`
  (Kafka, 191 lines) each touch five responsibility clusters.
- Kafka gauge registration happens inside `connect()` — metrics leaking
  into lifecycle.
- The Rabbit reconnect path re-runs the entire topology cluster from
  inside the connection cluster with no visible seam.
- ~1,500 lines of logic are covered only end-to-end through Docker
  integration tests.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Scope | RabbitMQ **and** Kafka, decomposed along backend-private seams. No new cross-backend abstractions this cycle (explicitly deferred, not rejected). `bus/redis.py` untouched. |
| 2 | Pattern | **Collaborator composition**: stateful collaborators own their domain; each `*EventBus` becomes a facade that composes and delegates. Rejected: mixin split (coupling unchanged), function-module extraction (god object survives). |
| 3 | Exposure | Collaborators are **internal**. Nothing new is exported from `bus/__init__.py` or `eventsource/__init__.py`. |
| 4 | Compatibility | Minor public breakage allowed under **0.7.0** — spent on exactly three Kafka items (see API Changes). Everything else keeps exact public signatures and behavior. |
| 5 | Layout | Backend subpackages `bus/rabbitmq/` and `bus/kafka/`; package `__init__.py` re-exports everything the old flat module exported, so all existing imports keep working verbatim. |
| 6 | Testing | **Full backfill**: new unit tests per collaborator, plus unit backfill for facade paths currently covered only by integration tests (reconnect, shutdown ordering, health composition). Pure/logic-heavy modules join mutmut targets. |

## Package Layout

```
bus/rabbitmq/                            bus/kafka/
  __init__.py     re-exports               __init__.py     re-exports
  config.py       RabbitMQEventBusConfig   config.py       KafkaEventBusConfig
  models.py       Stats, DLQMessage,       models.py       KafkaEventBusStats,
                  QueueInfo,                               DeserializationError,
                  HealthCheckResult,                       KafkaNotAvailableError
                  RabbitMQNotAvailableError,
                  ShutdownError,
                  BatchPublishError
  connection.py   ConnectionManager        connection.py   ConnectionManager +
                                                           KafkaRebalanceListener
  topology.py     Topology                 metrics.py      KafkaEventBusMetrics +
                                                           gauge registration
  serialization.py serialize/deserialize   serialization.py EventSerializer
                  + AMQP message building                  (moves as-is)
  publisher.py    publish/batch paths      publisher.py    send/ack split-phase
  consumer.py     consume loop, dispatch,  consumer.py     consume loop, dispatch,
                  retry + DLQ write path                   retry + DLQ write path
  dlq.py          DLQAdmin (get/count/     dlq.py          DLQAdmin (get/count/
                  replay/purge)                            replay; throwaway-
                                                           consumer helper)
  death_headers.py 7 pure header functions bus.py          KafkaEventBus facade
  bus.py          RabbitMQEventBus facade
```

- Kafka has no `topology.py`: its topology is config-driven. The
  asymmetry is real and stays.
- The old flat modules `bus/rabbitmq.py` and `bus/kafka.py` are deleted;
  the packages take their import paths.
- Rabbit's seven static death-header helpers become module-level
  functions in `death_headers.py`; `RabbitMQEventBus` keeps thin static
  aliases **permanently** (they are documented public API).
- The `try/except ImportError` + `*_AVAILABLE` guard (ADR 0015 pattern)
  lives in each package `__init__.py` with identical observable behavior.

## State Ownership & Wiring

Every instance field moves to exactly one owner. The only deliberately
shared mutable objects are the stats dataclass and the ConnectionManager.

- **ConnectionManager** owns the broker clients (Rabbit:
  `_connection`/`_channel`; Kafka: `_producer`/`_consumer` + rebalance
  listener), `_connected`/`_reconnecting` flags, SSL context creation,
  and URL sanitizing. Collaborators hold a reference to the
  ConnectionManager and ask it for a live channel/producer at call time —
  never a raw client captured at construction (clients don't exist until
  `connect()`).
- **Reconnect hook:** ConnectionManager exposes
  `on_reconnect(callback)`. The facade registers `topology.redeclare`
  and `consumer.resume_if_was_consuming` at construction. This replaces
  today's implicit connection→topology→consumer tangle with one
  observable seam.
- **Topology** (Rabbit) owns `_exchange`, `_dlq_exchange`,
  `_consumer_queue`, `_dlq_queue`; provides declare-all, redeclare,
  `bind_event_type`, `bind_routing_key`.
- **Publisher** holds references to ConnectionManager,
  Topology/exchange, serializer, stats, tracer. Kafka keeps its
  split-phase shape (sequential send handoff, gathered acks).
- **Consumer** owns the consume task, consuming flags, and the
  retry/DLQ **write** path (it is part of message processing). It
  receives two callables from the facade — `handlers_for(event_class)`
  and `resolve_event_class(name)` — so `BaseEventBus`'s
  `SubscriptionRegistry` stays on the facade and the consumer never sees
  the registry. Handler-error isolation semantics (ADR 0011:
  run all handlers, aggregate into `HandlerDispatchError`, no ack)
  are preserved bit-for-bit.
- **Stats** remains one mutable dataclass created by the facade and
  passed by reference into publisher/consumer/connection; each mutates
  its own counters. `get_stats()`/`get_stats_dict()`/`reset_stats()`
  behavior unchanged.
- **Tracing:** tracer constructor-injected into publisher and consumer
  only (ADR 0016 composition preserved).
- **Kafka metrics:** gauge registration moves from `connect()` into
  `metrics.py`; the facade wires it after connect.
- **Shutdown/drain ordering is facade-owned:** stop consumer → drain
  in-flight → `_drain_background()` (BaseEventBus) → disconnect.
  Collaborators expose stop/close primitives; only the facade knows the
  order.
- **`health_check`** (Rabbit) becomes composition: connection, topology,
  and stats each report their slice; the facade assembles
  `HealthCheckResult`. Result shape unchanged.
- Background-task tracking (`_track_background`) stays on the facade;
  collaborators that need it receive a callable.

## Public API Changes (0.7.0)

Exactly three, all Kafka:

1. **Remove `KafkaEventBus.get_handlers_for_event`** — a
   `DeprecationWarning` shim since 0.6.0; this is its scheduled exit.
2. **Internalize `record_reconnection` / `record_rebalance`** — logic
   moves into the connection/metrics collaborators. The facade keeps
   one-release deprecation shims (warn + delegate), removed in 0.8.0.
3. **`KafkaRebalanceListener` moves** to `bus/kafka/connection.py`
   without a shim — it was never exported from `bus/__init__.py` or the
   top level.

Everything else preserves exact public signatures and behavior,
including: `bind_event_type`, `bind_routing_key`, stats accessors, DLQ
admin methods, `get_queue_info`, `health_check`, `is_shutdown`,
`start_consuming_in_background`, `get_topic_info`, the death-header
statics, and all dataclass/exception import paths.

`pyproject.toml` → 0.7.0. CHANGELOG documents the three changes.

## Testing Strategy (full backfill)

**Layer 1 — behavior safety net (must pass unmodified):** the 9-test
`EventBusConformanceSuite` subclasses for all four backends, and every
integration test that exercises only public API. Exceptions, expected to
change because they pin private internals:

- `tests/integration/bus/test_rabbitmq.py` asserts `_exchange`,
  `_dlq_queue`, `_consumer_queue`, `_dlq_exchange`, `_config`,
  `_event_registry` → rewritten to assert through `health_check()` /
  `get_queue_info()` or the topology collaborator, so tests stop pinning
  facade field names.
- `tests/unit/bus/test_serialization_properties.py` reaches
  `_serialize_event` / `_deserialize_event` / `_deserialize_message` →
  repointed at the new `serialization.py` modules (same assertions).
- Tracing tests reach `_tracer` / `_enable_tracing` → repointed at
  collaborator constructors.
- `tests/integration/bus/test_kafka.py` reaches `_metrics` and gauge
  flags → repointed at the metrics collaborator.

**Layer 2 — collaborator unit tests** in `tests/unit/bus/rabbitmq/` and
`tests/unit/bus/kafka/` (mirroring the packages), with mocked broker
objects: topology declare/redeclare idempotence; reconnect-hook firing
order; retry/DLQ write-path decisions; DLQ admin paging/replay;
death-header functions (pure — property tests where cheap); Kafka gauge
registration; publisher batch strategies.

**Layer 3 — facade-path backfill:** unit tests for wiring currently
proven only via Docker: reconnect re-declares topology and resumes
consuming; shutdown ordering (stop → drain in-flight → drain background
→ disconnect); `health_check` composition; stats accumulation across
collaborators. All against mocked ConnectionManagers.

**Mutation targets:** `death_headers.py`, both `serialization.py`
modules, both consumers' retry-decision logic, and — clearing last
cycle's deferred item — `bus/memory.py` join mutmut `only_mutate`.

Coverage gate stays `fail_under = 87`; expected to rise as Docker-only
logic gains unit coverage. Wall-clock perf assertions remain excluded
from CI via the `benchmark` marker (unchanged from 0.6.0).

## Risks

- **Reconnect and shutdown are the moves most likely to regress** —
  state migrates owners there. Mitigation: Layer 3 backfill tests are
  written against the new seams first; the Rabbit/Kafka integration
  suites (61 + 41 tests) run in CI via testcontainers and gate merge.
- **Import-time behavior:** package `__init__.py` must reproduce the
  optional-dependency guard exactly, including `*_AVAILABLE = False`
  fallbacks when the driver is missing. A unit test asserts the guard
  works with the dependency absent (mock the import failure).
- **Diff size:** this is a large mechanical move. Mitigation: leaf-first
  extraction order (pure helpers → DLQ admin → serialization →
  publisher/consumer → connection/topology → facade), each step green
  before the next; ordering detail lives in the implementation plan.

## ADR Impact

| ADR | Status vs this work |
|-----|---------------------|
| 0001 async-first design | **Stands.** |
| 0007 event bus delivery semantics | **Stands** — delivery behavior untouched. |
| 0010 uniform event bus contract | **Stands** — facades still inherit `BaseEventBus`; registry, `background` semantics, drain hooks stay on the facade. |
| 0011 handler-error isolation, no-ack | **Stands** — consumers keep `HandlerDispatchError`/no-ack semantics bit-for-bit. |
| 0013 handler registry composition | **Stands** — this work extends its composition principle to backend internals. |
| 0015 optional dependency extras | **Stands** — guard pattern moves to package `__init__.py`, observable behavior identical. |
| 0016 optional tracing no-op by default | **Stands** — tracer injection preserved via constructor passing. |
| **New: 0020 broker backend collaborator decomposition** | Records the facade + state-owning-collaborators pattern, internal (non-exported) seams, the reconnect hook, and rejected alternatives (mixins; function-module extraction; shared cross-backend contracts — deferred, not rejected). |

**Housekeeping folded in:** `docs/adrs/` currently has two `0009-` files
(`0009-multi-instance-subscription-coordination.md`,
`0009-postgresql-advisory-locks.md`) — a collision PR #73 did not cover.
Both landed in the same commit, so age does not disambiguate;
`0009-postgresql-advisory-locks.md` renumbers to **0019** (the
subscription-coordination ADR is bus-domain and keeps 0009).
`index.md` and inbound references update; ADR bodies untouched. The new
decomposition ADR then takes **0020**.

## Non-Goals

- No shared cross-backend contracts (common DLQAdmin interface, shared
  consumer pipeline) — deferred to a future cycle with Redis as the
  third implementor.
- No decomposition of `bus/redis.py` (1,336 lines — not a god class).
- No new features: no Kafka `health_check` parity, no Redis DLQ error
  context, no new public exports.
- No behavior changes to delivery, retry, or error-isolation semantics.
