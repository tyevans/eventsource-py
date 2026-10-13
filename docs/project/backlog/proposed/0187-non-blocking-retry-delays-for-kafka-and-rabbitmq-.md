---
id: "0187"
title: Non-Blocking Retry Delays for Kafka and RabbitMQ Consumers
status: Proposed
target_bc: adapters
governing_adrs:
  - ADR-0007
  - ADR-0158
priority: P2
---

# TASK-0187: Non-Blocking Retry Delays for Kafka and RabbitMQ Consumers

## Summary
Implement asynchronous, non-blocking retry backoff scheduling for Kafka and RabbitMQ message consumers using dedicated delay queues/topics, preventing head-of-line blocking on consumer threads and partitions.

## Problem Statement & Context
- Retry backoff delays currently block consumers synchronously during message reprocessing.
- RabbitMQ consumers block inline during execution (`time.sleep` or synchronous waiting).
- Kafka consumers block partition consumption before `_process_message`.
- This causes unrelated messages on the worker or partition to experience artificial latency and queue buildup.

## Required Actions
1. Implement a dedicated retry topic and delay scheduler pattern for Kafka consumers (e.g., dead-letter / retry topics with scheduled pollers).
2. Implement dead-letter exchanges (DLX) and message time-to-live (TTL) delayed queues for RabbitMQ consumers.
3. Add integration tests verifying non-blocking consumer throughput while failed messages undergo retry backoff periods.

## Definition of Done (Blackbox Frontdoor TDD)
1. In-flight failure of a single message does not halt delivery of subsequent healthy messages across the broker partition or channel.
2. Verified via automated integration tests measuring throughput under injected message failures.
3. All source files strictly comply with ADR-0002 (<500 lines limit).
