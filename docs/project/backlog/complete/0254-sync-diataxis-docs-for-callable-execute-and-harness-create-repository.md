---
id: TASK-0254
title: "Synchronize Diataxis guides for callable repo.execute and InMemoryTestHarness.create_repository / assertions"
status: Complete
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

# TASK-0254: Synchronize Diataxis guides for callable repo.execute and InMemoryTestHarness.create_repository / assertions

## Summary
Update `docs/guides/aggregates.md` and `docs/guides/testing.md` to document callable commands in `repo.execute(id, lambda agg: ...)` and `harness.create_repository(...)` / `harness.assertions` / `harness.snapshot_store`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
