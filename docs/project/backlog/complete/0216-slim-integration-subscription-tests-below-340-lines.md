---
id: TASK-0216
title: "Slim integration subscription tests (test_live, test_transition, test_catchup) below 340 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0216: Slim integration subscription tests (test_live, test_transition, test_catchup) below 340 lines

## Summary
Compact `tests/integration/subscriptions/test_live.py` (399 lines), `tests/integration/subscriptions/test_transition.py` (386 lines), and `tests/integration/subscriptions/test_catchup.py` (377 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
