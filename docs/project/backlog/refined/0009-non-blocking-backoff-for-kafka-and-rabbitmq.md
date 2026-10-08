---
id: '0009'
title: Non-Blocking Backoff for Kafka and RabbitMQ Retries
status: Refined
created: 2026-10-07
governing_adrs:
  - ADR-0002
  - ADR-0003
governing_prds:
  - PRD-0001
governing_stories:
  - US-0003
target_bc: adapters
---

# TASK-0009: Non-Blocking Backoff for Kafka and RabbitMQ Retries

## Summary
Consumer loops for Kafka and RabbitMQ currently use synchronous sleep during backoff, blocking unrelated partition messages. Refactor to use asynchronous non-blocking scheduling for retried messages.

## Definition of Done
1. Exponential retry scheduling uses non-blocking async timers.
2. Concurrent partition messages continue processing while failed message backs off.
3. Integration tests verify throughput during transient error injection.
