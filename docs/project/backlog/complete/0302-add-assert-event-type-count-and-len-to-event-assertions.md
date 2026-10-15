---
id: TASK-0302
title: "Add assert_event_type_count(event_type, count) and __len__ to EventAssertions"
status: Complete
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0015
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0302: Add assert_event_type_count(event_type, count) and __len__ to EventAssertions

## Summary
Add `assert_event_type_count(event_type, count) -> list[TEvent]` and `__len__(self) -> int` to `EventAssertions` in `src/eventsource/testing/assertions.py` while compacting `assertions.py` below 215 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
