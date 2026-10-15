---
id: TASK-0214
title: "Slim eventsource/__init__.py and application/subscriptions/__init__.py below 330 lines"
status: Refined
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0214: Slim eventsource/__init__.py and application/subscriptions/__init__.py below 330 lines

## Summary
Compact `src/eventsource/__init__.py` (367 lines) and `src/eventsource/application/subscriptions/__init__.py` (346 lines) below 330 lines while preserving 100% of public `__all__` exports.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
