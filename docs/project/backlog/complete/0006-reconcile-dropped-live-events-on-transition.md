---
id: '0006'
title: Reconcile Dropped Live Events on Subscription Transition Failure
status: Complete
governing_adrs:
- ADR-0007
- ADR-0132
- ADR-0147
- ADR-0159
governing_prds:
- PRD-0001
governing_stories:
- US-0004
target_bc: application
---

# TASK-0006: Reconcile Dropped Live Events on Subscription Transition Failure

## Summary
When subscription runners transition from catchup to live mode or fail during pause, buffered live events dropped during transition leave lag telemetry artificially inflated. Implement buffer draining and lag reconciliation.

## Definition of Done
1. Buffer drain mechanism clears stale buffered events on transition abort.
2. Lag telemetry correctly reports zero or accurate deficit post-transition.
3. Unit tests reproduce and verify transition recovery.
