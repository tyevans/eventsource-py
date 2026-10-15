---
id: TASK-0264
title: "Synchronize Diataxis guides for Scenario.then_event / then_no_events and single-projection replay"
status: Complete
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

# TASK-0264: Synchronize Diataxis guides for Scenario.then_event / then_no_events and single-projection replay

## Summary
Update `docs/guides/testing.md` and `docs/guides/projections.md` to document `Scenario.then_event(...)`, `Scenario.then_no_events()`, single-projection `replay(store, projection)`, and `ReplayReport.succeeded`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
