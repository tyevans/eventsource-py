---
id: '0462'
title: Dispatch subscription handler failures to SubscriptionErrorHandler and the
  DLQ
status: Complete
governing_adrs:
- ADR-0108
governing_prds:
- PRD-0003
governing_stories:
- US-0009
target_bc: application
priority_rank: 2
---

# TASK-0462: Dispatch subscription handler failures to SubscriptionErrorHandler and the DLQ

## Summary
The subscription runners never call `SubscriptionErrorHandler.handle_error()`. A configured DLQ repository, error strategy, or alert callback never runs. With the default `continue_on_error=True`, the catch-up runner then writes a checkpoint past the failed event, so the event is lost.

## Problem Statement & Context
- **Severity**: High
- **Weakness**: CWE-390 (error detected, no action taken)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

`SubscriptionManager` builds the error handler, but no production code calls `handle_error()`. The runners call `record_event_failed()` only, which changes a counter. A handler that fails on a security-relevant event (for example, a permission revocation) skips that event permanently and no alarm occurs. This is the declared-but-never-dispatched defect class. `docs/guides/subscriptions.md:435-441` says that a DLQ row is written. That statement is false today. The runners also count the failed event in `events_processed`.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/subscriptions/error_handling_handler.py:62` (handle_error: no production caller)
- `src/eventsource/application/subscriptions/manager_registry.py:68-75` (only construction site)
- `src/eventsource/application/subscriptions/runners/catchup_delivery.py:121-131`
- `src/eventsource/application/subscriptions/runners/catchup_batch.py:77-78`, 112-118
- `src/eventsource/application/subscriptions/runners/live_delivery.py:140-149`
- `src/eventsource/application/subscriptions/config.py:40` (continue_on_error=True)
- `docs/guides/subscriptions.md:249`, 435-441; docs/api/subscriptions.md:595-597

**Verification**: Proven with the in-memory store and `InMemoryDLQRepository`: the handler fails on event 2 of 3. Result: checkpoint `Position(3)`, `events_failed=1`, DLQ rows 0.

## Definition of Done (Blackbox Frontdoor TDD)
1. Both runners call the error handler on each handler failure, before they write a checkpoint past the event.
2. A test starts a real `SubscriptionManager` with a failing handler and a DLQ repository. It asserts one DLQ row and one callback invocation. The test fails on the current code.
3. A failed event does not increase `events_processed`.
4. The subscription guide and API reference describe the behavior that the code has.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
