---
id: TASK-0414
title: "Sync Wave 25 backlog and tighten source ceiling to 185 / 223 lines"
status: Complete
target_bc: architecture
dependencies: [TASK-0403]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0414: Sync Wave 25 backlog and tighten source ceiling to 185 / 223 lines

- **Status**: Complete
- **Target Bounded Context**: `architecture`

## Objective

Synchronize Wave 25 completed tasks (`TASK-0404`..`TASK-0413`), tighten `MAX_SRC_LINES = 185` and `MAX_TEST_EXAMPLE_LINES = 223` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 26 (`TASK-0415`..`TASK-0424`).
