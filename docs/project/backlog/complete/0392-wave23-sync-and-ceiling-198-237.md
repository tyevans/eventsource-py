---
id: TASK-0392
title: "Sync Wave 23 backlog and tighten source ceiling to 198 / 237 lines"
status: Complete
target_bc: architecture
dependencies: [TASK-0381]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0392: Sync Wave 23 backlog and tighten source ceiling to 198 / 237 lines

- **Status**: Complete
- **Target Bounded Context**: `architecture`

## Objective

Synchronize Wave 23 completed tasks (`TASK-0382`..`TASK-0391`), tighten `MAX_SRC_LINES = 198` (breaking the 200-line ceiling across 100% of `src/`!) and `MAX_TEST_EXAMPLE_LINES = 237` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 24 (`TASK-0393`..`TASK-0402`).
