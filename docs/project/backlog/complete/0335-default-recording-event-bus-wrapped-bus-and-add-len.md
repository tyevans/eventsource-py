---
id: TASK-0335
title: "Default RecordingEventBus wrapped bus to InMemoryEventBus and add __len__"
status: Complete
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0335: Default RecordingEventBus wrapped bus to InMemoryEventBus and add __len__

## Summary
Allow `RecordingEventBus(wrapped=None, *, max_events=10_000)` to default `wrapped` to `InMemoryEventBus(enable_tracing=False)` and add `__len__` returning the count of recorded events.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
