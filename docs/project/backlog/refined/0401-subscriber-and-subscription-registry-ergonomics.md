---
id: TASK-0401
title: "Add container dunders and clear to SubscriberRegistry and SubscriptionRegistry"
status: Refined
target_bc: application
dependencies: [TASK-0392]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0401: Add container dunders and clear to SubscriberRegistry and SubscriptionRegistry

- **Status**: Refined
- **Target Bounded Context**: `application`

## Objective

Add container dunders and clear to SubscriberRegistry and SubscriptionRegistry, preserving all behavioral contracts and keeping modified files well below the architectural line ceiling.
