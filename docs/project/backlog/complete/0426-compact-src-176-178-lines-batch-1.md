---
id: TASK-0426
title: "Compact 176-178 line src modules below 160 lines (Batch 1)"
status: Complete
target_bc: architecture
dependencies: [TASK-0425]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0426: Compact 176-178 line src modules below 160 lines (Batch 1)

- **Status**: Refined
- **Target Bounded Context**: `architecture`

## Objective

Compact 11 source modules (`subscriptions/models.py`, `error_handling_types.py`, `exceptions_operations.py`, `readmodel_schema_type.py`, `conformance_ports/outbox.py`, `catchup_runner.py`, `manager_dispatch.py`, `error_handling_dlq.py`, `tracer/protocol.py`, `coordinator_registry.py`, `bulk_copier.py`) strictly below 160 lines.
