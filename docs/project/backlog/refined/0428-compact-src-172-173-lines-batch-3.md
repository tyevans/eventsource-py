---
id: TASK-0428
title: "Compact 172-173 line src modules below 160 lines (Batch 3)"
status: Refined
target_bc: architecture
dependencies: [TASK-0425]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0428: Compact 172-173 line src modules below 160 lines (Batch 3)

- **Status**: Refined
- **Target Bounded Context**: `architecture`

## Objective

Compact 11 source modules (`coordinator_status.py`, `sqlite/outbox.py`, `redis/bus_dlq.py`, `live_drain.py`, `catchup_batch.py`, `coordination_work.py`, `write_pause_manager.py`, `position_mapping_mutation.py`, `memory/outbox.py`, `memory/dlq.py`, `kafka/consumer.py`) strictly below 160 lines.
