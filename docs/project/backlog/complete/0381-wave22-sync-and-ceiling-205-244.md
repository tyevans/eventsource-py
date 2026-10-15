---
id: TASK-0381
title: "Synchronize Wave 22 backlog, tighten ceiling to 205/244 lines, and refine Wave 23"
status: Complete
target_bc: architecture
dependencies: [TASK-0371, TASK-0372, TASK-0373, TASK-0374, TASK-0375, TASK-0376, TASK-0377, TASK-0378, TASK-0379, TASK-0380]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0381: Synchronize Wave 22 Backlog, Tighten Ceiling to 205/244 Lines, and Refine Wave 23

- **Status**: Complete
- **Target Bounded Context**: `architecture`

## Objective

Synchronize Wave 22 (`TASK-0371`..`TASK-0380`) to `complete/`, tighten `MAX_SRC_LINES = 205` and `MAX_TEST_EXAMPLE_LINES = 244` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 23 (`TASK-0382`..`TASK-0391`).
