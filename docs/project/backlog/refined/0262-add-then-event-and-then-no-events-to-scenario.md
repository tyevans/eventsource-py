---
id: TASK-0262
title: "Add then_event and then_no_events fluent assertions to Scenario and DeciderScenario"
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

# TASK-0262: Add then_event and then_no_events fluent assertions to Scenario and DeciderScenario

## Summary
Add `then_event(event_type, **expected_fields)` and `then_no_events()` fluent assertions to `DeciderScenario` / `Scenario` in `src/eventsource/testing/scenario.py` (keeping `scenario.py` < 295 lines) with unit tests.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
