---
id: TASK-0314
title: "Sync Diataxis projections and testing guides for ReplayReport.raise_for_failures and harness.replay"
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

# TASK-0314: Sync Diataxis projections and testing guides for ReplayReport.raise_for_failures and harness.replay

## Summary
Update `docs/guides/projections.md` and `docs/guides/testing.md` to document `ReplayReport.raise_for_failures()` and `await harness.replay(projection)`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
