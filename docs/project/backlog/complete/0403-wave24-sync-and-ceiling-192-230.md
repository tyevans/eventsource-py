---
id: TASK-0403
title: "Sync Wave 24 backlog and tighten source ceiling to 192 / 230 lines"
status: Complete
target_bc: architecture
dependencies: [TASK-0392]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0403: Sync Wave 24 backlog and tighten source ceiling to 192 / 230 lines

- **Status**: Complete
- **Target Bounded Context**: `architecture`

## Objective

Synchronize Wave 24 completed tasks (`TASK-0393`..`TASK-0402`), tighten `MAX_SRC_LINES = 192` and `MAX_TEST_EXAMPLE_LINES = 230` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 25 (`TASK-0404`..`TASK-0413`).
