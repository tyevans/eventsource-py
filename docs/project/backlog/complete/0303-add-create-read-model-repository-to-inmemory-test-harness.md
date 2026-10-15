---
id: TASK-0303
title: "Add create_read_model_repository(model_class) helper to InMemoryTestHarness"
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

# TASK-0303: Add create_read_model_repository(model_class) helper to InMemoryTestHarness

## Summary
Add `create_read_model_repository(model_class) -> InMemoryReadModelRepository[TModel]` to `InMemoryTestHarness` in `src/eventsource/testing/harness.py` with unit tests.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
