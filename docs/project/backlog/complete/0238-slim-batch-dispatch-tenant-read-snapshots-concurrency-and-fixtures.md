---
id: TASK-0238
title: "Slim live batch dispatch, tenant repository read, memory snapshots, sync concurrency, and integration fixtures below 310 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0238: Slim live batch dispatch, tenant repository read, memory snapshots, sync concurrency, and integration fixtures below 310 lines

## Summary
Compact `tests/unit/application/subscriptions/test_live_batch_dispatch.py` (347 lines), `tests/unit/application/aggregates/test_tenant_repository/test_tenant_repository_read.py` (347 lines), `tests/unit/adapters/test_memory_snapshots.py` (347 lines), `tests/unit/adapters/sync/test_concurrency.py` (347 lines), and `tests/integration/conftest_helpers/fixtures.py` (347 lines) below 310 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
