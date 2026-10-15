---
id: TASK-0199
title: "Add Ergonomic given_commands Builder to DeciderScenario"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0003
  - ADR-0006
dependencies: []
---

# TASK-0199: Add Ergonomic given_commands Builder to DeciderScenario

## Summary
Add Scenario.given_commands(*commands) to DeciderScenario so BDD and unit tests can seed prior state from commands as well as events.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
