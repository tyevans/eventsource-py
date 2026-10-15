---
id: TASK-0295
title: "Tighten src/eventsource ceiling guard to < 270 lines and test/example ceiling to < 297 lines"
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

# TASK-0295: Tighten src/eventsource ceiling guard to < 270 lines and test/example ceiling to < 297 lines

## Summary
Update `tests/unit/architecture/test_source_file_ceiling.py` to tighten `MAX_SRC_LINES` from `280` to `270` and `MAX_TEST_EXAMPLE_LINES` from `305` to `297`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
