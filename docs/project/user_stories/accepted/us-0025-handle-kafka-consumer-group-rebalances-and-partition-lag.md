---
id: '0025'
title: Handle Kafka Consumer Group Rebalances and Partition Lag Monitoring
status: Accepted
created: 2026-10-09
persona: Jordan (The Streaming & Distributed Systems Platform Engineer)
target_bc: adapters
feature: FEAT-KAFKA-REBALANCE-LAG
governing_prd: PRD-0003
scenarios:
- Offset commit on partition revocation prevents duplicate processing
- Partition assignment initializes state and registers telemetry gauges
- Real-time consumer lag observation calculates highwater offset delta
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0107
---

# US-0025: Handle Kafka Consumer Group Rebalances and Partition Lag Monitoring

## Governing PRD
- [`PRD-0003: Distributed Streaming and Subscription Coordination`](../../product/accepted/prd-0003-distributed-streaming-and-subscription-coordination.md)

## User Story

**As a** streaming and distributed systems platform engineer (Jordan),
**I want** `KafkaEventBus` to handle consumer group rebalance events via `KafkaRebalanceListener` and publish real-time per-partition lag metrics,
**So that** scaling consumer worker instances commits in-flight offsets cleanly before partitions migrate and Prometheus/OTel dashboards observe consumer lag.

## Acceptance Criteria

```gherkin
Scenario: Offset commit on partition revocation prevents duplicate processing
  Given an active Kafka consumer processing messages across assigned partitions
  When a cluster rebalance triggers "on_partitions_revoked"
  Then in-flight processed message offsets are committed synchronously to Kafka
  And duplicate deliveries upon partition handover are prevented.
```

```gherkin
Scenario: Partition assignment initializes state and registers telemetry gauges
  Given a cluster rebalance completing with new partition assignments
  When "on_partitions_assigned" is invoked
  Then consumer state for newly assigned partitions is initialized
  And lag observation callbacks are registered for OpenTelemetry metrics collection.
```

```gherkin
Scenario: Real-time consumer lag observation calculates highwater offset delta
  Given an active consumer group reading a partitioned topic
  When metric collection samples "eventsource.kafka.consumer.lag"
  Then the lag is computed as "highwater_offset - current_committed_offset" per partition
  And published as a labeled gauge metric.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/adapters/kafka/connection.py`: `KafkaRebalanceListener`.
  - `src/eventsource/adapters/kafka/bus.py`: Lag observation registration and offset tracking.
  - `src/eventsource/adapters/kafka/metrics.py`: Metrics definitions.
- **Verified Test Suites**:
  - `tests/unit/adapters/kafka/test_consumer.py`: Consumer loop and rebalance handling.
  - `tests/unit/adapters/kafka/test_metrics.py`: Lag gauges and telemetry attributes.
