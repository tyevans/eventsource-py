---
id: "0182"
title: Non-Blocking Kafka Retry Scheduling, Remove Kafka Bus Shims, and Deterministic Bus Benchmarks
status: Complete
target_bc: adapters
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0009
  - ADR-0107
governing_prds:
  - PRD-0003
governing_stories:
  - US-0003
  - US-0025
priority: P1
---

# TASK-0182: Non-Blocking Kafka Retry Scheduling, Remove Kafka Bus Shims, and Deterministic Bus Benchmarks

## Summary
Replace synchronous partition-blocking retry sleep in `KafkaConsumerLoop` with non-blocking `RetryScheduler` execution, remove private backward-compatible proxy shims on `KafkaEventBus`, decompose `adapters/kafka/` modules below 400 lines, and make Kafka/bus performance assertions deterministic.

## Problem Statement & Context
- `KafkaConsumerMessageMixin._await_retry_after` (`src/eventsource/adapters/kafka/consumer_message.py`) blocks the consumer loop inline via `await asyncio.sleep(remaining)`, causing head-of-line blocking on the partition.
- `KafkaEventBus` (`src/eventsource/adapters/kafka/bus.py` and `bus_consume.py`) retains 13 private delegating proxy properties and methods (`_connected`, `_producer`, `_consumer`, `_rebalance_listener`, `_metrics`, `_consuming`, `_consume_task`, `_process_message`, `_deserialize_message`, `_get_header_value`, `_get_retry_count`, `_dispatch_to_handlers`, `_calculate_retry_delay`, `_send_to_dlq`) as legacy shims.
- `kafka/config.py` (440 lines), `kafka/publisher.py` (429 lines), `kafka/dlq.py` (403 lines), `kafka/connection.py` (400 lines), and `tests/unit/adapters/kafka/test_consumer.py` (464 lines) trigger proactive file-length warnings (>=400 lines).

## Required Actions
1. Update `KafkaConsumerLoop` (`consumer.py`, `consumer_retry.py`, `consumer_message.py`) to schedule delayed retry republishes via `RetryScheduler` (matching `RabbitMQConsumer`) instead of blocking inline in `_await_retry_after`, and migrate `kafka/dlq.py` from stdlib `json` to `json_loads`.
2. Remove private backward-compatible proxy properties/methods from `KafkaEventBus` (`bus.py` and `bus_consume.py`) and update `tests/unit/test_kafka_event_bus/` to use public APIs or explicit collaborators.
3. Decompose/slim `kafka/config.py`, `kafka/publisher.py`, `kafka/dlq.py`, `kafka/connection.py`, and `tests/unit/adapters/kafka/test_consumer.py` so every file is strictly under 400 lines.
4. Ensure Kafka metric and retry tests use deterministic assertions rather than flaky wall-clock thresholds.

## Definition of Done (Blackbox Frontdoor TDD)
1. Retrying a failed message in `KafkaConsumerLoop` schedules the backoff via `RetryScheduler` without blocking subsequent messages.
2. Zero legacy private proxy shims remain on `KafkaEventBus`.
3. All `src/eventsource/adapters/kafka/` and `tests/unit/adapters/kafka/` files are strictly under 400 lines.
4. All unit and property tests pass cleanly.
