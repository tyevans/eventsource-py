---
id: TASK-0412
title: "Add subscribe_many, target-flexible unsubscribe/get_subscription, and container dunders to SubscriptionManager"
status: Refined
target_bc: application
dependencies: [TASK-0403]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0412: Add subscribe_many, target-flexible unsubscribe/get_subscription, and container dunders to SubscriptionManager

- **Status**: Refined
- **Target Bounded Context**: `application`

## Objective

Add subscribe_many, target-flexible unsubscribe/get_subscription, and container dunders to SubscriptionManager, preserving all behavioral contracts and keeping modified files well below the architectural line ceiling.
