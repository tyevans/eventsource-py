---
id: TASK-0324
title: "Support event_type in bdd.then_event_count and delegate to harness.assertions"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0324: Support event_type in bdd.then_event_count and delegate to harness.assertions

## Summary
Enhance `then_event_count(harness, count, *, event_type=None)` in `src/eventsource/testing/bdd.py` to support type-filtered counts via `harness.assertions.assert_event_type_count` and delegate all `bdd` assertion helpers to `harness.assertions`.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
