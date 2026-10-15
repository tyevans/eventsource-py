---
id: TASK-0331
title: "Compact 9 test modules at 272-273 lines below 245 lines"
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

# TASK-0331: Compact 9 test modules at 272-273 lines below 245 lines

## Summary
Compact `tests/unit/application/migration/test_dual_write/test_dual_write_operations.py` (273 lines), `tests/unit/application/migration/test_chaos/_infrastructure.py` (273 lines), `tests/unit/application/migration/test_bulk_copier/test_bulk_copier_batches.py` (273 lines), `tests/unit/application/aggregates/test_aggregate_type_inference.py` (273 lines), `tests/unit/adapters/test_snapshots_tracing/test_snapshots_tracing_spans.py` (273 lines), `tests/unit/adapters/test_memory_dlq/test_memory.py` (273 lines), `tests/integration/readmodels/test_projection/test_sqlite_workflows.py` (273 lines), `tests/unit/application/subscriptions/test_live_runner/test_live_runner_processing.py` (272 lines), and `tests/unit/application/migration/test_subscription_migrator/test_subscription_migrator_migrate.py` (272 lines) strictly below 245 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
