---
id: TASK-0208
title: "Slim sql/schemas/__init__.py and sql/readmodel_projection.py below 340 lines"
status: Refined
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0208: Slim sql/schemas/__init__.py and sql/readmodel_projection.py below 340 lines

## Summary
Compact `src/eventsource/adapters/sql/schemas/__init__.py` (383 lines) and `src/eventsource/adapters/sql/readmodel_projection.py` (367 lines) below 340 lines while keeping all schema loading and read-model projection APIs intact.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
