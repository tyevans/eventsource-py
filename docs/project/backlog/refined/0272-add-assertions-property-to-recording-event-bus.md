---
id: TASK-0272
title: "Add assertions property to RecordingEventBus"
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

# TASK-0272: Add assertions property to RecordingEventBus

## Summary
Add `@property def assertions(self) -> EventAssertions:` to `RecordingEventBus` in `src/eventsource/testing/recording.py` with unit tests.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
