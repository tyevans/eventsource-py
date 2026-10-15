---
id: TASK-0312
title: "Add raise_for_failures() helper to ReplayReport"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0008
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0312: Add raise_for_failures() helper to ReplayReport

## Summary
Add `raise_for_failures(self) -> None` to `ReplayReport` in `src/eventsource/application/projections/replay.py` (raising `ReplayFailedError` when `not self.succeeded`) with unit tests.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
