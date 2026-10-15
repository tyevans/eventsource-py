---
id: TASK-0370
title: "Synchronize Wave 21 backlog, tighten ceiling to 213/250 lines, and refine Wave 22"
status: Complete
target_bc: architecture
dependencies: [TASK-0360, TASK-0361, TASK-0362, TASK-0363, TASK-0364, TASK-0365, TASK-0366, TASK-0367, TASK-0368, TASK-0369]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0370: Synchronize Wave 21 Backlog, Tighten Ceiling to 213/250 Lines, and Refine Wave 22

- **Status**: Complete
- **Target Bounded Context**: `architecture`

## Objective

Synchronize Wave 21 (`TASK-0360`..`TASK-0369`) to `complete/`, tighten `MAX_SRC_LINES = 213` and `MAX_TEST_EXAMPLE_LINES = 250` in `tests/unit/architecture/test_source_file_ceiling.py`, and refine Wave 22 (`TASK-0371`..`TASK-0380`).
