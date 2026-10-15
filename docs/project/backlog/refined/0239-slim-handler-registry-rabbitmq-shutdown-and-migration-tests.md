---
id: TASK-0239
title: "Slim handler registry, rabbitmq shutdown, dual write lifecycle, metrics/status, and operation error tests below 310 lines"
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

# TASK-0239: Slim handler registry, rabbitmq shutdown, dual write lifecycle, metrics/status, and operation error tests below 310 lines

## Summary
Compact `tests/unit/application/projections/test_handler_registry.py` (346 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_shutdown.py` (345 lines), `tests/unit/application/migration/test_coordinator_dual_write/test_dual_write_lifecycle.py` (345 lines), `tests/unit/application/migration/test_final_integration/test_metrics_and_status.py` (344 lines), and `tests/unit/application/migration/test_exceptions/test_exceptions_operation_errors.py` (344 lines) below 310 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
