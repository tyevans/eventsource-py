---
id: TASK-0263
title: "Support passing a single projection to replay() and add ReplayReport.succeeded property"
status: Refined
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

# TASK-0263: Support passing a single projection to replay() and add ReplayReport.succeeded property

## Summary
Allow `replay(feed, projections, ...)` to accept a single `EventSubscriber` in addition to `Sequence[EventSubscriber]`, and add `ReplayReport.succeeded -> bool` property in `src/eventsource/application/projections/replay.py`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
