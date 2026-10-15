---
id: TASK-0280
title: "Slim test_tenant_context, test_handler_registry, sync_lag_tracker_stats, metrics_trackers_and_timers, and test_phase4_components below 285 lines"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0280: Slim test_tenant_context, test_handler_registry, sync_lag_tracker_stats, metrics_trackers_and_timers, and test_phase4_components below 285 lines

## Summary
Compact `tests/unit/domain/test_tenant_context.py`, `tests/unit/application/projections/test_handler_registry.py`, `tests/unit/application/migration/test_sync_lag_tracker/test_sync_lag_tracker_stats_and_history.py`, `tests/unit/application/migration/test_migration_metrics/test_metrics_trackers_and_timers.py`, and `tests/unit/application/migration/test_final_integration/test_phase4_components.py` strictly below 285 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
