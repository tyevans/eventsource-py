---
id: TASK-0330
title: "Compact 8 test and example modules at 273-274 lines below 245 lines"
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

# TASK-0330: Compact 8 test and example modules at 273-274 lines below 245 lines

## Summary
Compact `tests/unit/readmodels/test_postgresql/test_postgresql_model.py` (274 lines), `tests/unit/observability/test_projection_tracing/test_projection_tracing_order.py` (274 lines), `tests/unit/application/subscriptions/test_shutdown/test_shutdown_metrics.py` (274 lines), `tests/unit/adapters/sql/migration/test_audit_log_repository/test_queries_and_counts.py` (274 lines), `examples/imperative_example.py` (274 lines), `tests/unit/readmodels/test_soft_delete/test_soft_delete_queries.py` (273 lines), `tests/unit/application/subscriptions/test_shutdown/test_pre_shutdown_hooks.py` (273 lines), and `tests/unit/application/subscriptions/test_shutdown/test_post_shutdown_hooks.py` (273 lines) strictly below 245 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
