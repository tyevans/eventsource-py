---
id: TASK-0415
title: "Compact 183-184 line src modules below 165 lines (Batch 1)"
status: Refined
target_bc: architecture
dependencies: [TASK-0414]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0415: Compact 183-184 line src modules below 165 lines (Batch 1)

- **Status**: Refined
- **Target Bounded Context**: `architecture`

## Objective

Compact 11 source modules (`assertions.py`, `live_runner.py`, `status_streamer_manager.py`, `exceptions_base.py`, `handler_adapter.py`, `ports/snapshots.py`, `ports/outbox.py`, `transition_executor.py`, `error_handling_handler.py`, `subscription_migrator_planning.py`, `snapshotting.py`) strictly below 165 lines.
