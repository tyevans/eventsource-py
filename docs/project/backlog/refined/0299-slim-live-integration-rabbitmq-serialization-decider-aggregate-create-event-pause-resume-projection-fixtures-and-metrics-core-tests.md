---
id: TASK-0299
title: "Compact 7 test modules at 292-293 lines below 265 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0004
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0299: Compact 7 test modules at 292-293 lines below 265 lines

## Summary
Compact `tests/integration/subscriptions/test_live.py` (293 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_serialization.py` (292 lines), `tests/unit/domain/test_decider_aggregate.py` (292 lines), `tests/unit/domain/test_create_event/test_create_event_order.py` (292 lines), `tests/unit/application/subscriptions/test_manager_pause_resume/test_pause_resume_bulk_and_lifecycle.py` (292 lines), `tests/unit/application/projections/test_projection_base/_fixtures.py` (292 lines), and `tests/unit/application/migration/test_migration_metrics/test_metrics_core.py` (292 lines) strictly below 265 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
