---
id: '0466'
title: Contain poison messages in the RabbitMQ and Kafka consumers
status: Complete
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: adapters
priority_rank: 6
---

# TASK-0466: Contain poison messages in the RabbitMQ and Kafka consumers

## Summary
One crafted message stops RabbitMQ consumption or blocks a Kafka partition. Both consumers parse publisher-controlled headers outside the per-message error boundary. RabbitMQ also acknowledges and drops messages that fail validation, so events are lost without a DLQ entry.

## Problem Statement & Context
- **Severity**: High
- **Weakness**: CWE-20 (improper input validation)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

Any party that can publish to the exchange or topic can do this. No handler has to fail for the first trigger. RabbitMQ: `x-retry-count="abc"` raises before the try block; the consume task ends and nothing restarts it while the connection stays up. Kafka: a non-UTF-8 `event_type` or `retry_after` header, or a negative `retry_count` with a failing handler, raises into the loop; the loop reconnects without a commit and reads the same record again. Kafka DLQ listing also fails on one bad header, and an inbound `dlq_replay_count` header reaches the replay cap check.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/rabbitmq/consumer_dispatch.py:58` (int() before the try at :82), :82-88 (ack and drop)
- `src/eventsource/adapters/rabbitmq/consumer.py:79-81`, 92-106 (no restart)
- `src/eventsource/adapters/rabbitmq/serialization.py:138-152` (returns None on decode error)
- `src/eventsource/adapters/_bus/retry.py:76-77` (negative count raises)
- `src/eventsource/adapters/kafka/consumer.py:100-127` (reconnect without commit)
- `src/eventsource/adapters/kafka/consumer_message.py:68`, 145-149; adapters/kafka/consumer_helpers.py:27, 35
- `src/eventsource/adapters/kafka/consumer_retry.py:58`, 113
- `src/eventsource/adapters/kafka/dlq.py:67`, 81, 104-108

**Verification**: Proven with fake queues: RabbitMQ `start()` raised `ValueError: invalid literal for int() ... 'abc'` and the next good message was not acknowledged. Kafka with `event_type=b'\xff'` reopened the consumer 3 times in 3.5 s on the same record.

## Definition of Done (Blackbox Frontdoor TDD)
1. One decode-safe header reader is used by both consumers. It returns a default for missing, non-UTF-8 or non-integer values and clamps counts to 0 or more.
2. An error in header parsing or deserialization sends the message to the DLQ and acknowledges or commits it. Consumption continues with the next message.
3. RabbitMQ routes validation failures and unknown event types to the DLQ, as Kafka does.
4. Kafka DLQ listing decodes headers with replacement and parses counts safely. The DLQ writer removes inbound `dlq_*` headers.
5. Tests drive each consumer with a fake broker: a poison message followed by a good message. The good message is handled. The tests fail on the current code.
6. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
