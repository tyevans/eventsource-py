---
id: TASK-0436
title: "Sync Wave 27 backlog and tighten architectural line ceiling to 172/213"
status: Complete
target_bc: architecture
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0001-domain-aggregates-and-events.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-file-length-limit.md
  - docs/project/adrs/accepted/ADR-0005-backlog-isolation.md
depends_on:
  - TASK-0426
  - TASK-0427
  - TASK-0428
  - TASK-0429
  - TASK-0430
  - TASK-0431
  - TASK-0432
  - TASK-0433
  - TASK-0434
  - TASK-0435
---

# TASK-0436: Sync Wave 27 backlog and tighten architectural line ceiling to 172/213

## Summary
Synchronize Wave 27 tasks (`TASK-0426`..`TASK-0435`) into `docs/project/backlog/complete/`, tighten `MAX_SRC_LINES = 172` and `MAX_TEST_EXAMPLE_LINES = 213` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 28 (`TASK-0437`..`TASK-0446`).
