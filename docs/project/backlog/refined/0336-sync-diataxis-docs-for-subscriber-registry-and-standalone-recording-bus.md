---
id: TASK-0336
title: "Synchronize Diataxis guides for SubscriberRegistry and standalone RecordingEventBus"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0336: Synchronize Diataxis guides for SubscriberRegistry and standalone RecordingEventBus

## Summary
Update `docs/guides/projections.md` and `docs/guides/testing.md` to document `SubscriberRegistry` O(1) routing/registration ergonomics and standalone `RecordingEventBus()` with `len(bus)`.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
