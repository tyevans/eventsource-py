---
id: TASK-0342
title: "Compact 10 test modules at 265-266 lines below 240 lines"
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

# TASK-0342: Compact 10 test modules at 265-266 lines below 240 lines

## Summary
Compact `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_stats.py` (266 lines), `tests/unit/application/subscriptions/test_backpressure.py` (266 lines), `tests/unit/application/projections/test_projection_base/test_projection_checkpoints_and_declarative.py` (266 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_force_disconnect.py` (265 lines), `tests/unit/testing/test_decider_scenario.py` (265 lines), `tests/unit/application/subscriptions/test_shutdown/test_shutdown_periodic_checkpoint.py` (265 lines), `tests/unit/application/migration/test_sync_lag_tracker/test_sync_lag_tracker_calculate.py` (265 lines), `tests/unit/application/migration/test_router/test_router_interceptor_and_pause.py` (265 lines), `tests/unit/application/migration/test_phase3_integration/_repositories.py` (265 lines), and `tests/unit/application/migration/test_coordinator_subscriptions/test_coordinator_subscriptions_cutover.py` (265 lines) strictly below 240 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
