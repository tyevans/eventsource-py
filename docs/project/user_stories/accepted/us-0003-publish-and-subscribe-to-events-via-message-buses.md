---
id: '0003'
title: Publish and Subscribe to Events via Message Buses
status: Accepted
created: 2026-10-07
persona: Jordan (The Streaming & Distributed Systems Platform Engineer)
target_bc: adapters
feature: FEAT-BUS-ADAPTERS
governing_prd: PRD-0003
scenarios:
- Publish domain event to bus and dispatch to subscriber
- Aggregate multiple handler failures into HandlerDispatchError and withhold broker
  acknowledgment
- Bounded background publishing degrades automatically to inline execution under backpressure
- Graceful bus shutdown awaits in-flight background tasks before closing transport
  connections
- Multi-topic event routing isolates handler failures across concurrent subscriptions
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0101
- ADR-0107
---

# US-0003 — Publish and Subscribe to Events via Message Buses

## Governing PRD
- [`PRD-0003: Distributed Streaming and Subscription Coordination`](../../product/accepted/prd-0003-distributed-streaming-and-subscription-coordination.md)

## User Story

**As a** streaming and distributed systems platform engineer (Jordan),
**I want** to publish and subscribe to domain events over Kafka, RabbitMQ, Redis, or InMemory message buses with bounded background concurrency and uniform error isolation,
**So that** downstream subscribers receive events with at-least-once delivery, handler dispatch exceptions withhold broker acknowledgments for redelivery, and high write volumes do not cause unbounded memory growth.

## Acceptance Criteria

```gherkin
Scenario: Publish domain event to bus and dispatch to subscriber
  Given an active "EventBus" adapter and a registered async subscriber for "OrderShipped"
  When an "OrderShipped" event is published to the bus
  Then the subscriber handler is invoked with the deserialized event within timeout bounds.
```

```gherkin
Scenario: Aggregate multiple handler failures into HandlerDispatchError and withhold broker acknowledgment
  Given a broker-backed message bus with two registered handlers for "OrderPlaced"
  And the first handler raises an exception while the second handler succeeds
  When an "OrderPlaced" event is consumed from the broker transport
  Then both handlers are executed to completion
  And the consumer loop aggregates handler exceptions into a "HandlerDispatchError"
  And broker message acknowledgment or offset commit is withheld to trigger redelivery.
```

```gherkin
Scenario: Bounded background publishing degrades automatically to inline execution under backpressure
  Given an EventBus configured with a background task manager bounded to capacity 10
  When the publisher issues 15 background publish calls faster than tasks drain
  Then the first 10 publish operations run concurrently as tracked background tasks
  And the remaining 5 publish calls degrade automatically to inline execution without dropping messages.
```

```gherkin
Scenario: Graceful bus shutdown awaits in-flight background tasks before closing transport connections
  Given an EventBus with active in-flight background publish tasks
  When the caller invokes "shutdown()" with a 5-second timeout
  Then all tracked background publishing tasks drain to completion
  And underlying broker transport connections and client pools are cleanly closed.
```

```gherkin
Scenario: Multi-topic event routing isolates handler failures across concurrent subscriptions
  Given an EventBus with subscribers registered on "orders" and "notifications" topics
  When an event is published and the "orders" subscriber raises an unexpected error
  Then the "notifications" subscriber processes the event successfully
  And the fault in "orders" does not corrupt or abort delivery to "notifications".
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0107, ADR-0110, ADR-0111, ADR-0120, ADR-0131, ADR-0160
- **Verified Test Suites**:
  - `tests/unit/adapters/_bus/test_base.py`: Verifies background publishing bounds, drain timeout handling, and subscriber scoping.
  - `tests/unit/adapters/_bus/test_eventbus_tracing_patterns.py`: Verifies OpenTelemetry span propagation across bus boundaries.
  - `tests/unit/test_rabbitmq_event_bus.py`: Verifies RabbitMQ publisher/consumer lifecycle, batch limits, and error isolation.
  - `tests/unit/adapters/kafka/`: Verifies Kafka connection, publisher concurrency bounds, and consumer partition management.
  - `tests/unit/adapters/sync/test_adapter.py`: Verifies synchronous event bus contracts and in-flight task draining.
- **Architectural Invariants Verified**:
  - *Uniform EventBus Port*: Consistent contract across Kafka, RabbitMQ, Redis, and InMemory transports.
  - *At-Least-Once Delivery*: `HandlerDispatchError` aggregation with unacknowledged broker delivery on failure.
  - *Bounded Background Publishing*: Automatic degradation to inline publishing when concurrency ceiling is reached.
