---
id: TASK-0323
title: "Enhance ProjectionRegistry and ProjectionCoordinator registration ergonomics"
status: Complete
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

# TASK-0323: Enhance ProjectionRegistry and ProjectionCoordinator registration ergonomics

## Summary
Allow `ProjectionRegistry(projections=..., handlers=...)` and `registry.register(*items)` for fluent registration, and allow `ProjectionCoordinator` to accept either a `ProjectionRegistry` or a `Sequence[Projection]` directly.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
