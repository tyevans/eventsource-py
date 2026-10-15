---
id: TASK-0203
title: "Optimize EventRegistry Tuple Caching and Slim Module Below 350 Lines"
status: Complete
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0009
dependencies: []
---

# TASK-0203: Optimize EventRegistry Tuple Caching and Slim Module Below 350 Lines

## Summary
Slim src/eventsource/domain/event_registry.py below 350 lines and add Hypothesis property tests for registry lookup invariants.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
