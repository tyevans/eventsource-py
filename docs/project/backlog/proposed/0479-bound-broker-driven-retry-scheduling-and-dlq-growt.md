---
id: 0479
title: Bound broker-driven retry scheduling and DLQ growth
status: Proposed
dependencies:
- TASK-0466
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: adapters
priority_rank: 19
---

# TASK-0479: Bound broker-driven retry scheduling and DLQ growth

## Summary
A Kafka `retry_after` header with a far-future time creates one sleeping task per message, with no upper limit on the delay or on the task count. The RabbitMQ DLQ has no default length or TTL.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-770 (allocation without limits)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

Memory grows with each attacker message. Deferred retries are also lost at restart, because a later commit moves past the deferred offset.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/kafka/consumer_helpers.py:38-40` (no upper bound)
- `src/eventsource/adapters/kafka/consumer_message.py:58-64`
- `src/eventsource/adapters/_bus/retry_scheduler.py:27`, 74-76 (unbounded task set)
- `src/eventsource/adapters/rabbitmq/config.py:25-26` (dlq_message_ttl, dlq_max_length default None)

**Verification**: Code read.

## Definition of Done (Blackbox Frontdoor TDD)
1. The `retry_after` delay is clamped to `retry_max_delay`. NaN and negative values give 0.
2. `RetryScheduler` has a maximum task count. Above it, the message goes to the DLQ.
3. The RabbitMQ DLQ has a documented default maximum length.
4. Tests use the public consumer with a fake broker.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
