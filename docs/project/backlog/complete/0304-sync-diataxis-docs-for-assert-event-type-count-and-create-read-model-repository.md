---
id: TASK-0304
title: "Sync Diataxis testing guide for assert_event_type_count and create_read_model_repository"
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

# TASK-0304: Sync Diataxis testing guide for assert_event_type_count and create_read_model_repository

## Summary
Update `docs/guides/testing.md` to document `EventAssertions.assert_event_type_count(EventCls, count)` and `harness.create_read_model_repository(ModelCls)`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
