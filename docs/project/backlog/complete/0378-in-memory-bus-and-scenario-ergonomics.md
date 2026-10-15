---
id: TASK-0378
title: "Add __len__, __bool__, and clear() to InMemoryEventBus and assertions/__len__/__bool__ to DeciderScenario"
status: Complete
target_bc: testing
dependencies: [TASK-0370]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0378: Add __len__, __bool__, and clear() to InMemoryEventBus and assertions/__len__/__bool__ to DeciderScenario

- **Status**: Complete
- **Target Bounded Context**: `testing`

## Objective

Add __len__, __bool__, and clear() to InMemoryEventBus and assertions/__len__/__bool__ to DeciderScenario, preserving all behavioral contracts and keeping modified files well below the architectural line ceiling.
