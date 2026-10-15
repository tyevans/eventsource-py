---
id: TASK-0325
title: "Synchronize Diataxis guides for ProjectionCoordinator and bdd.then_event_count"
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

# TASK-0325: Synchronize Diataxis guides for ProjectionCoordinator and bdd.then_event_count

## Summary
Update `docs/guides/projections.md` and `docs/guides/testing.md` to document `ProjectionRegistry`/`ProjectionCoordinator` registration ergonomics and `then_event_count(..., event_type=...)`.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
