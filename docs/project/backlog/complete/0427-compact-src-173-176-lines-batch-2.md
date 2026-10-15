---
id: TASK-0427
title: "Compact 173-176 line src modules below 160 lines (Batch 2)"
status: Complete
target_bc: architecture
dependencies: [TASK-0425]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0427: Compact 173-176 line src modules below 160 lines (Batch 2)

- **Status**: Refined
- **Target Bounded Context**: `architecture`

## Objective

Compact 12 source modules (`topology_bindings.py`, `consumer_retry.py`, `subscription_pause.py`, `sync_lag_tracker.py`, `publisher_batch.py`, `metrics_subscription.py`, `router_management.py`, `bus_publish.py`, `conformance_ports/readmodels.py`, `ports/readmodels/exceptions.py`, `ports/exceptions.py`, `ports/__init__.py`) strictly below 160 lines.
