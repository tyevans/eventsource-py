---
id: TASK-0300
title: "Compact remaining 290+ line test and benchmark modules below 265 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0006
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0300: Compact remaining 290+ line test and benchmark modules below 265 lines

## Summary
Compact `tests/integration/subscriptions/test_catchup.py` (292 lines), `tests/integration/bus/test_rabbitmq/test_reliability.py` (292 lines), `tests/benchmarks/test_repositories/test_checkpoint_dlq.py` (292 lines), `tests/unit/application/subscriptions/test_retry/test_retry_policies.py` (291 lines), `tests/unit/application/subscriptions/test_retry/test_retry_execution.py` (291 lines), and `tests/unit/application/projections/test_projection_coordinator/test_subscriber_registry_and_concurrency.py` (291 lines) strictly below 265 lines so 100% of `tests/` and `examples/` is strictly `< 290` lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
