---
id: TASK-0245
title: "Tighten src/eventsource ceiling guard to < 325 lines and test/example ceiling to < 345 lines"
status: Complete
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

# TASK-0245: Tighten src/eventsource ceiling guard to < 325 lines and test/example ceiling to < 345 lines

## Summary
Update `tests/unit/architecture/test_source_file_ceiling.py` to tighten `MAX_SRC_LINES` from `330` to `325` and `MAX_TEST_EXAMPLE_LINES` from `360` to `345`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
