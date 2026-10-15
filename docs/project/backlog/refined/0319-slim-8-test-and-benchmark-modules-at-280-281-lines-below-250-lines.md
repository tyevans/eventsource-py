---
id: TASK-0319
title: "Compact 8 test and benchmark modules at 280-281 lines below 250 lines"
status: Refined
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

# TASK-0319: Compact 8 test and benchmark modules at 280-281 lines below 250 lines

## Summary
Compact `tests/unit/application/migration/test_load_benchmarks/test_position_and_streaming.py` (281 lines), `tests/unit/application/aggregates/test_repository/test_repository_loading.py` (281 lines), `tests/unit/testing/test_module_structure.py` (280 lines), `tests/unit/readmodels/test_sqlite/test_sqlite_queries.py` (280 lines), `tests/unit/application/migration/test_load_benchmarks/test_regression_and_summary.py` (280 lines), `tests/integration/subscriptions/test_transition.py` (280 lines), `tests/integration/bus/test_rabbitmq/test_publishing.py` (280 lines), and `tests/benchmarks/test_postgresql_catchup.py` (280 lines) strictly below 250 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
