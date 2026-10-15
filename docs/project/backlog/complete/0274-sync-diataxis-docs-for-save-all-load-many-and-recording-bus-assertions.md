---
id: TASK-0274
title: "Synchronize Diataxis guides for repo.save_all, repo.load_many, and RecordingEventBus.assertions"
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

# TASK-0274: Synchronize Diataxis guides for repo.save_all, repo.load_many, and RecordingEventBus.assertions

## Summary
Update `docs/guides/aggregates.md` and `docs/guides/testing.md` to document `repo.save_all(...)`, `repo.load_many(...)`, and `RecordingEventBus.assertions`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
