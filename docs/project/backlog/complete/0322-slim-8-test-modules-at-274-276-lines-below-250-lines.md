---
id: TASK-0322
title: "Compact 8 test modules at 274-276 lines below 250 lines"
status: Complete
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

# TASK-0322: Compact 8 test modules at 274-276 lines below 250 lines

## Summary
Compact `tests/unit/adapters/test_sqlite_conformance.py` (276 lines), `tests/unit/application/projections/test_projection_decorators.py` (275 lines), `tests/unit/application/migration/test_write_pause/test_write_pause_operations.py` (275 lines), `tests/unit/application/migration/test_write_pause/test_write_pause_init_and_state.py` (275 lines), `tests/unit/application/migration/test_load_benchmarks/_repositories.py` (275 lines), `tests/unit/application/migration/test_coordinator_subscriptions/test_coordinator_subscriptions_verify.py` (275 lines), `tests/unit/application/migration/test_coordinator_dual_write/test_coordinator_dual_write_lifecycle.py` (274 lines), and `tests/unit/adapters/test_memory_store/test_memory_store_tenant.py` (274 lines) strictly below 250 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
