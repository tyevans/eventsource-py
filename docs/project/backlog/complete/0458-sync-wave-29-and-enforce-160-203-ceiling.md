---
id: TASK-0458
title: "Sync Wave 29 backlog and tighten architectural line ceiling to 160/203"
status: Complete
target_bc: architecture
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0001-domain-aggregates-and-events.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-file-length-limit.md
  - docs/project/adrs/accepted/ADR-0005-backlog-isolation.md
depends_on:
  - TASK-0448
  - TASK-0449
  - TASK-0450
  - TASK-0451
  - TASK-0452
  - TASK-0453
  - TASK-0454
  - TASK-0455
  - TASK-0456
  - TASK-0457
---

# TASK-0458: Sync Wave 29 backlog and tighten architectural line ceiling to 160/203

## Summary
Synchronize Wave 29 tasks (`TASK-0448`..`TASK-0457`) into `docs/project/backlog/complete/` and tighten `MAX_SRC_LINES = 160` and `MAX_TEST_EXAMPLE_LINES = 203` in `tests/unit/architecture/test_source_file_ceiling.py`.
