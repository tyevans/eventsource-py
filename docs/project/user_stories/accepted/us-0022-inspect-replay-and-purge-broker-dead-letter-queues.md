---
id: '0022'
title: Inspect, Replay, and Purge Broker Dead-Letter Queues with Loop Protection
status: Accepted
created: 2026-10-09
persona: Jordan (The Streaming & Distributed Systems Platform Engineer)
target_bc: adapters
feature: FEAT-BROKER-DLQ
governing_prd: PRD-0003
scenarios:
- Non-destructive inspection of dead-letter queue messages
- Replay failed message to primary exchange with header tracking
- Infinite replay loop protection rejects over-replayed messages
- Dead-letter queue purge removes exhausted poison pill messages
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0107
---

# US-0022: Inspect, Replay, and Purge Broker Dead-Letter Queues with Loop Protection

## Governing PRD
- [`PRD-0003: Distributed Streaming and Subscription Coordination`](../../product/accepted/prd-0003-distributed-streaming-and-subscription-coordination.md)

## User Story

**As a** streaming and distributed systems platform engineer (Jordan),
**I want** administrative interfaces to non-destructively inspect, replay, and purge broker-level Dead-Letter Queues (DLQ) across Kafka, RabbitMQ, and Redis,
**So that** operators can triage poison-pill messages, safely redeliver corrected events without causing infinite retry loops, and clean dead queues during maintenance.

## Acceptance Criteria

```gherkin
Scenario: Non-destructive inspection of dead-letter queue messages
  Given poison pill messages routed to a broker Dead-Letter Queue
  When the administrator queries "get_messages()" via DLQAdmin
  Then messages are returned with their failure reason, death headers, and payload intact
  And inspection does not acknowledge, dequeue, or commit message offsets on the broker.
```

```gherkin
Scenario: Replay failed message to primary exchange with header tracking
  Given a dead-lettered message in the queue
  When "replay_message(message_id)" is executed
  Then the event is re-published to the original topic or exchange
  And retry count is reset while "x-replayed-from-dlq" and timestamp headers are stamped.
```

```gherkin
Scenario: Infinite replay loop protection rejects over-replayed messages
  Given a message whose "dlq_replay_count" has reached "dlq_max_replay_attempts"
  When the caller invokes "replay_message()" without force override
  Then the replay request is rejected with a MaxReplayAttemptsExceededError
  And re-poisoning the primary stream is precluded unless explicitly overridden with "force=True".
```

```gherkin
Scenario: Dead-letter queue purge removes exhausted poison pill messages
  Given resolved or abandoned messages residing in the DLQ
  When the operator triggers "purge()"
  Then all dead-lettered records are purged from the queue
  And the purged record count is reported accurately.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/adapters/kafka/dlq.py`: `KafkaDLQAdmin`.
  - `src/eventsource/adapters/rabbitmq/dlq.py`: `RabbitMQDLQAdmin`.
  - `src/eventsource/adapters/redis/bus.py`: Redis DLQ methods.
- **Verified Test Suites**:
  - `tests/unit/adapters/rabbitmq/test_dlq_admin.py`: RabbitMQ DLQ inspection, replay, and purge.
  - `tests/unit/adapters/test_memory_dlq.py`: In-memory DLQ inspection and retry counting.
  - `tests/unit/adapters/test_memory_dlq_properties.py`: Property-based invariants on replay and retention.
