---
id: TASK-0236
title: "Slim the 5 test modules at >= 350 lines strictly below 310 lines"
status: Refined
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

# TASK-0236: Slim the 5 test modules at >= 350 lines strictly below 310 lines

## Summary
Compact `tests/unit/application/migration/test_chaos/_repositories.py` (356 lines), `tests/integration/bus/test_rabbitmq/test_stats.py` (353 lines), `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_state_and_readiness.py` (352 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_health_check.py` (350 lines), and `tests/unit/application/migration/test_router/test_router_interceptor_and_pause.py` (350 lines) below 310 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
