---
id: TASK-0294
title: "Sync Diataxis testing guide for EventBuilder.build_many and InMemoryTestHarness.append_events"
status: Refined
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

# TASK-0294: Sync Diataxis testing guide for EventBuilder.build_many and InMemoryTestHarness.append_events

## Summary
Update `docs/guides/testing.md` to document `EventBuilder(EventCls, **fields)`, `EventBuilder.build_many(count)`, and `await harness.append_events(*events)`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
