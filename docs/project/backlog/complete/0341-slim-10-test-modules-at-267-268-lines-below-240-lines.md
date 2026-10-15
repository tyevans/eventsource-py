---
id: TASK-0341
title: "Compact 10 test modules at 267-268 lines below 240 lines"
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

# TASK-0341: Compact 10 test modules at 267-268 lines below 240 lines

## Summary
Compact `tests/integration/subscriptions/test_full_flow/test_lifecycle_and_checkpoints.py` (268 lines), `tests/integration/observability/conftest.py` (268 lines), `tests/integration/bus/test_rabbitmq/test_stats.py` (268 lines), `tests/unit/test_redis_event_bus_tracing/test_redis_tracing_core.py` (267 lines), `tests/unit/test_redis_event_bus/test_redis_event_bus_core.py` (267 lines), `tests/unit/observability/test_tracing.py` (267 lines), `tests/unit/application/migration/test_final_integration/test_recovery.py` (267 lines), `tests/unit/application/migration/test_final_integration/_mocks.py` (267 lines), `tests/unit/application/migration/test_chaos/_repositories.py` (267 lines), and `tests/integration/readmodels/test_projection/conftest.py` (267 lines) strictly below 240 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
