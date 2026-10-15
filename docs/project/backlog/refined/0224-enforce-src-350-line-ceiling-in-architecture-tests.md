---
id: TASK-0224
title: "Add architectural guard test enforcing < 350 line ceiling across src/eventsource/"
status: Refined
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0224: Add architectural guard test enforcing < 350 line ceiling across src/eventsource/

## Summary
Add an architectural test in `tests/unit/architecture/` verifying that every `.py` file under `src/eventsource/` remains strictly `< 350` lines, locking in the Wave 1–6 module factoring gains.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
