---
id: TASK-0225
title: "Tighten src/eventsource ceiling guard to < 340 lines and enforce < 380 line ceiling across tests/ and examples/"
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

# TASK-0225: Tighten src/eventsource ceiling guard to < 340 lines and enforce < 380 line ceiling across tests/ and examples/

## Summary
Update `tests/unit/architecture/test_source_file_ceiling.py` to tighten `MAX_SRC_LINES` from `350` to `340` across `src/eventsource/` and add `MAX_TEST_EXAMPLE_LINES = 380` across `tests/` and `examples/`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
