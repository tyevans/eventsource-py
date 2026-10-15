---
id: TASK-0267
title: "Slim catchup_runner.py, rabbitmq/models.py, memory/bus.py, error_handling_types.py, sync_lag_tracker.py, and readmodels_query.py below 275 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0267: Slim catchup_runner.py, rabbitmq/models.py, memory/bus.py, error_handling_types.py, sync_lag_tracker.py, and readmodels_query.py below 275 lines

## Summary
Compact all 6 remaining `src/eventsource/` modules at >= 290 lines (`catchup_runner.py`, `rabbitmq/models.py`, `memory/bus.py`, `error_handling_types.py`, `sync_lag_tracker.py`, and `sqlite/readmodels_query.py`) strictly below 275 lines so 100% of `src/eventsource/` is < 290 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
