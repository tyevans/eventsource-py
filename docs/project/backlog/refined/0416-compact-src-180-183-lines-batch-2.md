---
id: TASK-0416
title: "Compact 180-183 line src modules below 165 lines (Batch 2)"
status: Refined
target_bc: architecture
dependencies: [TASK-0414]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0416: Compact 180-183 line src modules below 165 lines (Batch 2)

- **Status**: Refined
- **Target Bounded Context**: `architecture`

## Objective

Compact 11 source modules (`position_mapping_query.py`, `consumer_message.py`, `conformance_ports/snapshots.py`, `partitioned_memory.py`, `redis/bus_consume.py`, `postgresql/locks.py`, `conformance_ports/coordination.py`, `conformance_ports/bus.py`, `domain/decorators.py`, `shutdown_metrics.py`, `subscription_migrator_execution.py`) strictly below 165 lines.
