---
id: TASK-0198
title: "Add Typed published_of_type Helper to InMemoryEventBus and RecordingEventBus"
status: Complete
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

# TASK-0198: Add Typed published_of_type Helper to InMemoryEventBus and RecordingEventBus

## Summary
Add ergonomic published_of_type(event_cls) typed filtering helpers to InMemoryEventBus and RecordingEventBus for expressive blackbox test assertions.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
