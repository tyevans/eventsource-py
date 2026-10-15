---
id: TASK-0269
title: "Slim rabbitmq_opentelemetry, subscription_migrator_models_and_workflows, bulk_copier_run, sqlite_snapshots_operations, and basic_projection below 295 lines"
status: Complete
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

# TASK-0269: Slim rabbitmq_opentelemetry, subscription_migrator_models_and_workflows, bulk_copier_run, sqlite_snapshots_operations, and basic_projection below 295 lines

## Summary
Compact `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_opentelemetry.py`, `tests/unit/application/migration/test_subscription_migrator/test_subscription_migrator_models_and_workflows.py`, `tests/unit/application/migration/test_bulk_copier/test_bulk_copier_run.py`, `tests/unit/adapters/test_sqlite_snapshots/test_sqlite_snapshots_operations.py`, and `examples/subscriptions/basic_projection.py` strictly below 295 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
