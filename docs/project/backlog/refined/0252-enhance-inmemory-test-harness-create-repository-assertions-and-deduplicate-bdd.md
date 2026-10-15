---
id: TASK-0252
title: "Add create_repository, snapshot_store, and assertions to InMemoryTestHarness and deduplicate testing/bdd.py"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0252: Add create_repository, snapshot_store, and assertions to InMemoryTestHarness and deduplicate testing/bdd.py

## Summary
Add `snapshot_store`, `create_repository(aggregate_factory, ...)`, and `assertions` (`EventAssertions`) to `InMemoryTestHarness`, and delegate `testing/bdd.py` assertion functions to `EventAssertions` to eliminate duplicated assertion logic.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
