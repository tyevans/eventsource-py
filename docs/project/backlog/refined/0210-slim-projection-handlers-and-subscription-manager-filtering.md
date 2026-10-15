---
id: TASK-0210
title: "Slim projections/handlers.py, subscriptions/manager_lifecycle.py, and subscriptions/filtering.py below 340 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0210: Slim projections/handlers.py, subscriptions/manager_lifecycle.py, and subscriptions/filtering.py below 340 lines

## Summary
Compact `src/eventsource/application/projections/handlers.py` (371 lines), `src/eventsource/application/subscriptions/manager_lifecycle.py` (367 lines), and `src/eventsource/application/subscriptions/filtering.py` (361 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
