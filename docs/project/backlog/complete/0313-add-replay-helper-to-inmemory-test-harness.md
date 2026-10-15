---
id: TASK-0313
title: "Add await harness.replay(projections, **kwargs) helper to InMemoryTestHarness"
status: Complete
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

# TASK-0313: Add await harness.replay(projections, **kwargs) helper to InMemoryTestHarness

## Summary
Add `async def replay(self, projections, **kwargs) -> ReplayReport` to `InMemoryTestHarness` in `src/eventsource/testing/harness.py` (delegating to `replay(self._event_store, projections, **kwargs)`) with unit tests.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
