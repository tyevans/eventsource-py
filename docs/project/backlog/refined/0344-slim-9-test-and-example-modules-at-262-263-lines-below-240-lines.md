---
id: TASK-0344
title: "Compact 9 test and example modules at 262-263 lines below 240 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0344: Compact 9 test and example modules at 262-263 lines below 240 lines

## Summary
Compact `tests/unit/application/migration/test_coordinator_dual_write/test_dual_write_lifecycle.py` (263 lines), `tests/integration/adapters/test_postgresql_conformance.py` (263 lines), `examples/subscriptions/multi_subscriber/projections.py` (263 lines), `tests/unit/test_fixtures/test_aggregates_and_integration.py` (262 lines), `tests/unit/application/subscriptions/test_shutdown/test_shutdown_deadline.py` (262 lines), `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_locks_and_lag.py` (262 lines), `tests/unit/application/aggregates/test_repository/test_repository_execute.py` (262 lines), `tests/integration/subscriptions/test_advanced_features/test_pause_resume.py` (262 lines), and `tests/integration/bus/test_rabbitmq/test_exchange_types.py` (262 lines) strictly below 240 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
