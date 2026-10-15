---
id: TASK-0202
title: "Slim Projection Checkpoint Base and SQL Projection Modules Below 350 Lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0102
dependencies: []
---

# TASK-0202: Slim Projection Checkpoint Base and SQL Projection Modules Below 350 Lines

## Summary
Slim src/eventsource/application/projections/base_checkpoint.py and src/eventsource/adapters/sql/projection.py below 350 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
