---
id: '0009'
title: Coordinate Subscriptions with Delivery Guarantees and DLQ Error Isolation
status: Accepted
created: 2026-10-08
persona: Jordan (The Streaming & Distributed Systems Platform Engineer)
target_bc: subscriptions
feature: FEAT-SUBSCRIPTIONS
governing_prd: PRD-0003
scenarios:
- LiveRunner executes feed-driven checkpointing treating bus message as wake-up signal
- Subscription delivery is strictly sequential per subscription
- Live batch delivery dispatches feed page without accumulator window
- Persistent handler failure routes event to Dead Letter Queue without stalling feed
- SubscriptionManager coordinates graceful shutdown using single declaration timeout
- Multi-instance subscription coordination elects leader over reserved bus topic
governing_adrs:
- ADR-0007
- ADR-0107
- ADR-0109
- ADR-0124
- ADR-0132
- ADR-0147
- ADR-0159
- ADR-0161
- ADR-0162
- ADR-0163
---

# US-0009 — Coordinate Subscriptions with Delivery Guarantees and DLQ Error Isolation

## Governing PRD
- [`PRD-0003: Distributed Streaming and Subscription Coordination`](../../product/accepted/prd-0003-distributed-streaming-and-subscription-coordination.md)

## User Story

**As a** streaming and distributed systems platform engineer (Jordan),
**I want** to coordinate multi-instance subscriptions with feed-driven live runners, strictly sequential delivery, unified retry and shutdown policies, and segregated DLQ isolation,
**So that** live checkpointing is deterministic, single-active coordination prevents duplicate consumers, poison-pill events never stall streaming feeds, and runners terminate gracefully.

## Acceptance Criteria

```gherkin
Scenario: LiveRunner executes feed-driven checkpointing treating bus message as wake-up signal
  Given an active LiveRunner subscribed to a domain event notification topic
  And a checkpoint recorded at global position 10 in CheckpointRepository
  When a wake-up signal without position metadata arrives over the message bus
  Then LiveRunner queries "GlobalEventFeed.read_all(from_position=10)"
  And dispatches returned events to handlers in feed order
  And checkpoints the exact position returned by the feed without re-reading past events.
```

```gherkin
Scenario: Subscription delivery is strictly sequential per subscription
  Given a subscription consumer processing a sequence of ordered domain events
  When multiple events are ready for delivery to the subscriber
  Then the current event is dispatched and awaited to completion
  And its position is checkpointed before the next event begins processing.
```

```gherkin
Scenario: Live batch delivery dispatches feed page without accumulator window
  Given a live subscription handler implementing "handle_batch()" with batch size 50
  When a bus wake-up triggers a feed read returning a page of 15 available events
  Then LiveRunner immediately dispatches the 15-event page to "handle_batch()"
  And does not hold or delay delivery waiting for an accumulator window to fill.
```

```gherkin
Scenario: Persistent handler failure routes event to Dead Letter Queue without stalling feed
  Given a subscriber configured with a "ProjectionRetryPolicy" allowing 3 retries
  And a persistent handler defect that raises an exception on every attempt
  When a poison-pill event is delivered to the subscriber
  Then the event is retried exactly 3 times according to backoff policy
  And the poison-pill event and error context are written to the "DeadLetterQueue" repository
  And the subscription checkpoint advances past the failed event allowing subsequent events to process.
```

```gherkin
Scenario: SubscriptionManager coordinates graceful shutdown using single declaration timeout
  Given a SubscriptionManager configured with "shutdown_timeout=5.0"
  And active subscription runners executing event delivery loops
  When the manager receives a shutdown signal
  Then in-flight event handlers and checkpoints are allowed up to 5.0 seconds to finish
  And runner tasks are cleanly cancelled and resources released if the timeout expires.
```

```gherkin
Scenario: Multi-instance subscription coordination elects leader over reserved bus topic
  Given two subscription worker instances configured with a "LeaderElector" protocol
  When both instances start and publish heartbeats over the reserved coordination bus topic
  Then exactly one instance acquires active leadership to execute catch-up processing
  And the standby instance remains idle until leadership is released or heartbeat expires.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0107, ADR-0109, ADR-0124, ADR-0132, ADR-0147, ADR-0159, ADR-0161, ADR-0162, ADR-0163
- **Verified Test Suites**:
  - `tests/unit/application/subscriptions/test_pause_resume.py`: Verifies pause/resume semantics and state tracking.
  - `tests/unit/application/subscriptions/test_shutdown.py`: Verifies periodic checkpointing, signal handling, and single-declaration shutdown deadlines.
  - `tests/unit/application/subscriptions/test_live_batch_dispatch.py`: Verifies live grouped dispatch without artificial window delays.
  - `tests/unit/application/subscriptions/test_health_api.py`: Verifies readiness probes, lag calculation, and degradation metrics.
  - `tests/integration/subscriptions/test_full_flow.py`: Verifies multi-subscriber independent processing, starting offsets, and ordered delivery.
  - `tests/integration/subscriptions/test_advanced_features.py`: Verifies pause/resume, OpenTelemetry metrics, and health API integration.
- **Architectural Invariants Verified**:
  - *Feed-Driven Live Checkpointing*: Bus events trigger wakeups; positions are fetched and verified against `GlobalEventFeed`.
  - *Strict Per-Subscription Ordering*: Next event waits for the active event to finish and checkpoint.
  - *Bounded DLQ Isolation*: Poison events route to `DeadLetterQueue` after retry exhaustion without stalling subscription streams.
