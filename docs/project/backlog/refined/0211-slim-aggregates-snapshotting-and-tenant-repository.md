---
id: TASK-0211
title: "Slim aggregates/snapshotting.py and aggregates/tenant_repository.py below 340 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0211: Slim aggregates/snapshotting.py and aggregates/tenant_repository.py below 340 lines

## Summary
Compact `src/eventsource/application/aggregates/snapshotting.py` (364 lines) and `src/eventsource/application/aggregates/tenant_repository.py` (362 lines) below 340 lines while keeping all snapshotting and tenant repository contracts unchanged.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
