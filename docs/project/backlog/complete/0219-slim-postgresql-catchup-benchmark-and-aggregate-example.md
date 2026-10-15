---
id: TASK-0219
title: "Slim test_postgresql_catchup.py benchmark and examples/aggregate_example.py below 340 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0219: Slim test_postgresql_catchup.py benchmark and examples/aggregate_example.py below 340 lines

## Summary
Compact `tests/benchmarks/test_postgresql_catchup.py` (390 lines) and `examples/aggregate_example.py` (378 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
