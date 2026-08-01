# ADR 0020: Broker Backend Collaborator Decomposition

## Status

Accepted (2026-07-29)

**Amended by [ADR 0031](0031-bus-ring-split.md)** -- for module locations
only: the Rabbit/Kafka collaborator packages live under
`eventsource.adapters.{rabbitmq,kafka}` now, not `eventsource.bus`. This
ADR's collaborator decomposition Decision is unchanged.

## Context

RabbitMQEventBus (~3,570-line class) and KafkaEventBus (~2,195-line class)
each mixed connection lifecycle, topology declaration, serialization,
publish paths, consume/dispatch, retry/DLQ write paths, DLQ administration,
health checks, and stats in one object. Cross-cutting methods
(health_check, _process_message, _dispatch_to_handlers) read state from
four or five responsibility clusters, and ~1,500 lines of logic were
covered only through Docker integration tests.

## Decision

Each broker backend is a package of internal, state-owning collaborators
composed by a facade that keeps the public API:

- ConnectionManager owns broker clients, connected/reconnecting flags, and
  an explicit `on_reconnect(callback)` hook; other collaborators request
  live clients at call time and never capture them at construction.
- RabbitMQTopology owns exchange/queue objects and declaration; Kafka has
  no topology collaborator (config-driven — the asymmetry is real).
- Publisher and Consumer own their paths; the Consumer owns the retry/DLQ
  write path and receives `handlers_for` / `resolve_event_class` callables
  so the SubscriptionRegistry stays on the facade (ADR 0013, ADR 0010).
- DLQAdmin owns get/count/replay(/purge) administration.
- Pure logic (RabbitMQ death-header introspection, serialization) lives in
  plain modules.
- Collaborators are internal: nothing new is exported from
  `eventsource.bus` or `eventsource`. Facades keep every public signature;
  the only 0.7.0 API changes are removing the already-deprecated
  `KafkaEventBus.get_handlers_for_event`, deprecating
  `record_reconnection`/`record_rebalance`, and moving
  `KafkaRebalanceListener` to `bus/kafka/connection.py` (still re-exported
  from `eventsource.bus.kafka`). `background=True` publishes on
  Kafka are now registered with the shared background-task tracker per
  ADR 0010 — this cycle fixed a latent nonconformance (previously aiokafka
  future callbacks, drain was a no-op), with cross-call ordering/
  error-surfacing caveats documented in the module docs.
- Shutdown/drain ordering and health-check assembly are facade-owned;
  collaborators expose stop/close primitives and health slices.

## Alternatives Considered

- **Mixin split**: files shrink but every mixin still reads shared `self`
  state — coupling unchanged, only harder to see. Rejected.
- **Function-module extraction**: logic moves to parameterized functions
  but all state stays on one class with 15-20 fields. Rejected.
- **Shared cross-backend contracts** (common DLQAdmin/consumer pipeline
  interfaces implemented by Redis too): deferred, not rejected — revisit
  once both per-backend decompositions have settled.

## Consequences

- Collaborators are unit-testable with mocked broker objects; reconnect
  and shutdown wiring have direct unit coverage instead of Docker-only.
- Delivery, retry, and error-isolation semantics (ADR 0007, ADR 0011) are
  unchanged; the conformance suite passes unmodified.
- Internal collaborator APIs may change freely between releases.
