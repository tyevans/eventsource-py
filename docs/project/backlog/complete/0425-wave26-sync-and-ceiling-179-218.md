---
id: TASK-0425
title: "Sync Wave 26 backlog and tighten source ceiling to 179 / 218 lines"
status: Complete
target_bc: architecture
dependencies: [TASK-0414]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0425: Sync Wave 26 backlog and tighten source ceiling to 179 / 218 lines

- **Status**: Complete
- **Target Bounded Context**: `architecture`

## Objective

Synchronize Wave 26 completed tasks (`TASK-0415`..`TASK-0424`), tighten `MAX_SRC_LINES = 179` and `MAX_TEST_EXAMPLE_LINES = 218` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 27 (`TASK-0426`..`TASK-0435`).
