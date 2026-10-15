---
id: TASK-0241
title: "Add ergonomic then_event_types(*event_classes) assertion helper to Scenario and DeciderScenario"
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

# TASK-0241: Add ergonomic then_event_types(*event_classes) assertion helper to Scenario and DeciderScenario

## Summary
Add `then_event_types(self, *event_classes: type[DomainEvent]) -> Self` to `Scenario` and `DeciderScenario` in `src/eventsource/testing/scenario.py` with unit tests in `tests/unit/testing/`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
