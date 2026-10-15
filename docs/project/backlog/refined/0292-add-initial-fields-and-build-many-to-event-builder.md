---
id: TASK-0292
title: "Add initial keyword fields and build_many(count) to EventBuilder"
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

# TASK-0292: Add initial keyword fields and build_many(count) to EventBuilder

## Summary
Allow passing `**fields` directly to `EventBuilder(event_class, **fields)` and add `EventBuilder.build_many(count, *, start_version=None)` to build a sequence of events with auto-incrementing `aggregate_version` in `src/eventsource/testing/builder.py` while compacting `builder.py` below 230 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
