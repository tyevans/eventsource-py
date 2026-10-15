---
id: TASK-0411
title: "Add unsubscribe_all to BaseEventBus and remove_subscriber / container dunders to bus SubscriptionRegistry"
status: Complete
target_bc: adapters
dependencies: [TASK-0403]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0411: Add unsubscribe_all to BaseEventBus and remove_subscriber / container dunders to bus SubscriptionRegistry

- **Status**: Complete
- **Target Bounded Context**: `adapters`

## Objective

Add unsubscribe_all to BaseEventBus and remove_subscriber / container dunders to bus SubscriptionRegistry, preserving all behavioral contracts and keeping modified files well below the architectural line ceiling.
