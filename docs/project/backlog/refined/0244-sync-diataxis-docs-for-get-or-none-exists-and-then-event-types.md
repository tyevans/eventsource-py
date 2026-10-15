---
id: TASK-0244
title: "Synchronize Diataxis guides for repo.get_or_none, repo.exists, and Scenario.then_event_types"
status: Refined
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0244: Synchronize Diataxis guides for repo.get_or_none, repo.exists, and Scenario.then_event_types

## Summary
Update `docs/guides/aggregates.md` and `docs/guides/testing.md` to document `repo.get_or_none(id)`, `repo.exists(id)`, `harness.published_of_type(EventCls)`, and `Scenario.then_event_types(*EventClasses)`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
