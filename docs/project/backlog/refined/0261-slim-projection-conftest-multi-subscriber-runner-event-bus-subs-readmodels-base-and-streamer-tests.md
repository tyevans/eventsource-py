---
id: TASK-0261
title: "Slim readmodels projection conftest, multi_subscriber runner, event_bus_subscriptions, readmodels test_base, and status_streamer tests below 300 lines"
status: Refined
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

# TASK-0261: Slim readmodels projection conftest, multi_subscriber runner, event_bus_subscriptions, readmodels test_base, and status_streamer tests below 300 lines

## Summary
Compact `tests/integration/readmodels/test_projection/conftest.py`, `examples/subscriptions/multi_subscriber/runner.py`, `tests/unit/test_event_bus/test_event_bus_subscriptions.py`, `tests/unit/readmodels/test_base.py`, and `tests/unit/application/migration/test_status_streamer/test_status_streamer_streaming.py` strictly below 300 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
