---
id: "0184"
title: Reconcile Dropped Live Events on Stop or Transition Failure
status: Proposed
target_bc: application
governing_adrs:
  - ADR-0007
  - ADR-0164
priority: P3
---

# TASK-0184: Reconcile Dropped Live Events on Stop or Transition Failure

## Summary
Reconcile subscription lag metrics when a subscription stops or fails during transition by properly tracking dropped buffered events, preventing permanently inflated lag values.

## Problem Statement & Context
- When a subscription stops or fails during transition, the system drops buffered events without recording them as unseen.
- This behavior violates the lag reconciliation contract in `Subscription.lag` and leaves lag values permanently inflated.
- The drop occurs in `_cleanup` within `src/eventsource/application/subscriptions/transition.py` (lines 392-402) and in `stop()` within the subscription live runner.
- Buffered events increment the seen counter upon receipt from the message bus; restarting an existing subscription object inherits inflated lag values.

## Required Actions
1. In `src/eventsource/application/subscriptions/transition.py` and the subscription live runner:
   - Invoke `record_events_unseen` with the queue size (`qsize()`) when dropping transition or pause buffers.
   - Alternatively, ensure both seen and processed counters are systematically reset at the start of transition execution or restart.
2. Add comprehensive unit and integration tests to verify lag reconciliation when stopping and restarting subscriptions.

## Definition of Done (Blackbox Frontdoor TDD)
1. Stopping a subscription or aborting a transition does not cause subsequent `Subscription.lag` queries to report inflated counts.
2. Verified via automated blackbox tests asserting `lag == 0` when buffers are cleared.
3. All source files strictly comply with ADR-0002 (<500 lines limit).
