---
id: TASK-0268
title: "Slim phase2 _anchor_base, rabbitmq test_dlq_admin, fixtures/aggregates, dual_write_operations, and postgresql_catchup benchmark below 295 lines"
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

# TASK-0268: Slim phase2 _anchor_base, rabbitmq test_dlq_admin, fixtures/aggregates, dual_write_operations, and postgresql_catchup benchmark below 295 lines

## Summary
Compact `tests/unit/application/migration/test_phase2_integration/_anchor_base.py`, `tests/unit/adapters/rabbitmq/test_dlq_admin.py`, `tests/fixtures/aggregates.py`, `tests/unit/application/migration/test_dual_write/test_dual_write_operations.py`, and `tests/benchmarks/test_postgresql_catchup.py` strictly below 295 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
