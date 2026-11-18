---
id: 0491
title: Keep Kafka deferred retries across consumer restart
status: Proposed
dependencies:
- TASK-0479
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: adapters
---

# TASK-0491: Keep Kafka deferred retries across consumer restart

## Summary
A Kafka consumer that defers a retry does not keep the deferred message across a restart. The consumer commits offsets past the deferred message, so a restart loses the retry.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during TASK-0479. The fix for TASK-0479 limits the delay and the task count, but deferred retries stay in memory only. A crash or a deploy during the delay drops the message without a DLQ entry.

## Evidence
- `src/eventsource/adapters/kafka/consumer_message.py` (deferred retry scheduling)
- `src/eventsource/adapters/_bus/retry_scheduler.py` (in-memory task set)

## Definition of Done (Blackbox Frontdoor TDD)
1. Choose a design: do not commit past a pending retry, or republish the message to a retry topic, or write it to the DLQ at shutdown.
2. Record the choice in an ADR.
3. A test stops the public consumer during a pending retry, starts it again, and shows that the message is delivered or dead-lettered.
4. Tests use public entry points only. All files stay under the line ceilings.
