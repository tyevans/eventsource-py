---
id: TASK-0298
title: "Compact top 7 test modules at 293-296 lines below 265 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0298: Compact top 7 test modules at 293-296 lines below 265 lines

## Summary
Compact `tests/integration/repositories/test_outbox/test_outbox_lifecycle.py` (296 lines), `tests/unit/application/migration/test_exceptions/test_exceptions_operation_errors.py` (295 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_topology_exchange.py` (294 lines), `tests/unit/application/aggregates/test_tenant_repository/test_tenant_repository_read.py` (294 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_consumption_control.py` (293 lines), `tests/unit/application/migration/test_status_streamer/test_status_streamer_lifecycle.py` (293 lines), and `tests/unit/application/aggregates/test_repository/test_repository_execute.py` (293 lines) strictly below 265 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
