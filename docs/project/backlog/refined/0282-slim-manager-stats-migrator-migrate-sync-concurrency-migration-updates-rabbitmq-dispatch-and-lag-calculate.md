---
id: TASK-0282
title: "Slim manager_stats_and_metrics, subscription_migrator_migrate, sync/test_concurrency, migration test_updates, rabbitmq_consumption_dispatch, and sync_lag_tracker_calculate below 285 lines"
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

# TASK-0282: Slim manager_stats_and_metrics, subscription_migrator_migrate, sync/test_concurrency, migration test_updates, rabbitmq_consumption_dispatch, and sync_lag_tracker_calculate below 285 lines

## Summary
Compact `tests/unit/application/subscriptions/test_subscription_manager/test_manager_stats_and_metrics.py`, `tests/unit/application/migration/test_subscription_migrator/test_subscription_migrator_migrate.py`, `tests/unit/adapters/sync/test_concurrency.py`, `tests/unit/adapters/sql/migration/test_migration_repository/test_updates.py`, `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_consumption_dispatch.py`, and `tests/unit/application/migration/test_sync_lag_tracker/test_sync_lag_tracker_calculate.py` strictly below 285 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
