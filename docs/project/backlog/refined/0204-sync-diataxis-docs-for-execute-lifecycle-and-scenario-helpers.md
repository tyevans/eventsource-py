---
id: TASK-0204
title: "Synchronize Diataxis Documentation for Repository.execute, Async Context Managers, and Scenario Helpers"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0001
  - ADR-0006
dependencies: []
---

# TASK-0204: Synchronize Diataxis Documentation for Repository.execute, Async Context Managers, and Scenario Helpers

## Summary
Update docs/guides/ and docs/api/ to document AggregateRepository.execute, SupportsAsyncContextManager (async with store:), and Scenario.when_all / then_state.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
