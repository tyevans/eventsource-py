---
id: TASK-0326
title: "Tighten architectural file line ceiling to <244 for src and <275 for tests/examples"
status: Complete
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0326: Tighten architectural file line ceiling to <244 for src and <275 for tests/examples

## Summary
Tighten `MAX_SRC_LINES = 244` and `MAX_TEST_EXAMPLE_LINES = 275` in `tests/unit/architecture/test_source_file_ceiling.py` to lock in Wave 17 module compaction gains.

## Acceptance Criteria
1. `MAX_SRC_LINES = 244` and `MAX_TEST_EXAMPLE_LINES = 275` enforced in `tests/unit/architecture/test_source_file_ceiling.py`.
2. `uv run pytest tests/unit/architecture/test_source_file_ceiling.py` passes with 0 violations.
