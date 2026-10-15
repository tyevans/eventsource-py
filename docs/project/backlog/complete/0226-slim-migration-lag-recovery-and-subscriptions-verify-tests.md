---
id: TASK-0226
title: "Slim migration lag tracker, recovery, and coordinator subscription verification tests below 320 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0226: Slim migration lag tracker, recovery, and coordinator subscription verification tests below 320 lines

## Summary
Compact `tests/unit/application/migration/test_sync_lag_tracker/test_sync_lag_tracker_calculate.py` (373 lines), `tests/unit/application/migration/test_final_integration/test_recovery.py` (373 lines), and `tests/unit/application/migration/test_coordinator_subscriptions/test_coordinator_subscriptions_verify.py` (373 lines) below 320 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
