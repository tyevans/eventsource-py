---
id: TASK-0275
title: "Tighten src/eventsource ceiling guard to < 290 lines and test/example ceiling to < 315 lines"
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

# TASK-0275: Tighten src/eventsource ceiling guard to < 290 lines and test/example ceiling to < 315 lines

## Summary
Update `tests/unit/architecture/test_source_file_ceiling.py` to tighten `MAX_SRC_LINES` from `300` to `290` and `MAX_TEST_EXAMPLE_LINES` from `325` to `315`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
