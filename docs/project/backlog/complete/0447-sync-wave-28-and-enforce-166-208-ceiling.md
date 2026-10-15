---
id: TASK-0447
title: "Sync Wave 28 backlog and tighten architectural line ceiling to 166/208"
status: Complete
target_bc: architecture
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0001-domain-aggregates-and-events.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-file-length-limit.md
  - docs/project/adrs/accepted/ADR-0005-backlog-isolation.md
depends_on:
  - TASK-0437
  - TASK-0438
  - TASK-0439
  - TASK-0440
  - TASK-0441
  - TASK-0442
  - TASK-0443
  - TASK-0444
  - TASK-0445
  - TASK-0446
---

# TASK-0447: Sync Wave 28 backlog and tighten architectural line ceiling to 166/208

## Summary
Synchronize Wave 28 tasks (`TASK-0437`..`TASK-0446`) into `docs/project/backlog/complete/`, tighten `MAX_SRC_LINES = 166` and `MAX_TEST_EXAMPLE_LINES = 208` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 29 (`TASK-0448`..`TASK-0457`).
