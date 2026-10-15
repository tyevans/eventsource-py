---
id: TASK-0289
title: "Compact remaining 300+ line test modules below 275 lines so 100% of repository is < 300 lines"
status: Complete
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

# TASK-0289: Compact remaining 300+ line test modules below 275 lines so 100% of repository is < 300 lines

## Summary
Compact `tests/unit/application/subscriptions/test_live_batch_dispatch.py` (301 lines), `tests/unit/application/projections/test_projection_base/test_projection_checkpoints_and_declarative.py` (301 lines), `tests/unit/adapters/test_postgresql_snapshots/test_postgresql_snapshots_basic.py` (301 lines), `tests/unit/testing/test_builder/test_builder_methods.py` (300 lines), `tests/unit/test_redis_event_bus/test_redis_event_bus_consumer.py` (300 lines), and `tests/unit/application/migration/test_chaos/test_stores_and_infrastructure.py` (300 lines) strictly below 275 lines so 100% of the entire repository is strictly `< 300` lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
