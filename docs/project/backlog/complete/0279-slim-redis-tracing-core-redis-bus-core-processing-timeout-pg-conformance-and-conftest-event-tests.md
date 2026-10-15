---
id: TASK-0279
title: "Slim redis_tracing_core, redis_event_bus_core, test_processing_timeout, test_postgresql_conformance, and conftest_event below 285 lines"
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

# TASK-0279: Slim redis_tracing_core, redis_event_bus_core, test_processing_timeout, test_postgresql_conformance, and conftest_event below 285 lines

## Summary
Compact `tests/unit/test_redis_event_bus_tracing/test_redis_tracing_core.py`, `tests/unit/test_redis_event_bus/test_redis_event_bus_core.py`, `tests/unit/application/subscriptions/test_processing_timeout.py`, `tests/integration/adapters/test_postgresql_conformance.py`, and `tests/conftest_helpers/conftest_event.py` strictly below 285 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
