---
id: TASK-0337
title: "Tighten architectural line ceiling to 237 lines for src/ and 269 lines for tests/ & examples/"
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
dependencies:
  - TASK-0327
  - TASK-0328
  - TASK-0329
  - TASK-0330
  - TASK-0331
  - TASK-0332
  - TASK-0333
---

# TASK-0337: Tighten architectural line ceiling to 237 lines for src/ and 269 lines for tests/ & examples/

## Summary
Ratchet `MAX_SRC_LINES = 237` and `MAX_TEST_EXAMPLE_LINES = 269` in `tests/unit/architecture/test_source_file_ceiling.py` following Wave 18 compaction.
