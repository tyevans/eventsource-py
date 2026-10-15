---
id: TASK-0310
title: "Compact 8 test and example modules at 283-284 lines below 260 lines"
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

# TASK-0310: Compact 8 test and example modules at 283-284 lines below 260 lines

## Summary
Compact `tests/unit/application/projections/test_projection_base/test_projection_database.py` (284 lines), `tests/unit/application/migration/test_position_mapper/test_position_mapper_translation.py` (284 lines), `tests/unit/application/migration/test_error_classification.py` (284 lines), `tests/unit/adapters/test_memory_dlq/test_sql_queries.py` (284 lines), `tests/unit/domain/test_tenant_context_properties/test_tenant_context_properties_restores.py` (283 lines), `tests/unit/application/subscriptions/test_coordination/test_work_redistribution.py` (283 lines), `tests/unit/application/aggregates/test_repository_snapshot/test_repository_snapshot_auto.py` (283 lines), and `examples/subscriptions/basic_projection.py` (283 lines) strictly below 260 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
