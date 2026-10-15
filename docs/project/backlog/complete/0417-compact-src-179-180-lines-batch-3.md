---
id: TASK-0417
title: "Compact 179-180 line src modules below 165 lines (Batch 3)"
status: Complete
target_bc: architecture
dependencies: [TASK-0414]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0417: Compact 179-180 line src modules below 165 lines (Batch 3)

- **Status**: Complete
- **Target Bounded Context**: `architecture`

## Objective

Compact 11 source modules (`rabbitmq/connection.py`, `memory/bus.py`, `domain/event_registry.py`, `domain/event.py`, `position_mapper_query.py`, `coordinator_copy.py`, `tenant_repository.py`, `sqlite/snapshots.py`, `sql/projection.py`, `rabbitmq/consumer.py`, `rabbitmq/bus_health.py`) strictly below 165 lines.
