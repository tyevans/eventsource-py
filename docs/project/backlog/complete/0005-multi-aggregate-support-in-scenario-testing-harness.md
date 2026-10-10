---
id: '0005'
title: Multi-Aggregate Support in Scenario Testing Harness
status: Complete
governing_adrs:
- ADR-0003
- ADR-0007
- ADR-0122
- ADR-0143
governing_prds:
- PRD-0001
governing_stories:
- US-0001
target_bc: testing
---

# TASK-0005: Multi-Aggregate Support in Scenario Testing Harness

## Summary
`given_events` currently constrains scenarios to a single aggregate identifier. Extend the BDD test harness to support multi-aggregate event history setup without requiring real database backing.

## Definition of Done
1. `DeciderScenario` and BDD Given steps accept events with heterogeneous aggregate IDs.
2. Unit tests verify multi-aggregate setup and command evaluation.
3. Public documentation updated in `docs/tutorials/08-testing.md`.
