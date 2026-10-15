---
id: TASK-0379
title: "Add assert_first_event and assert_last_event to EventAssertions"
status: Complete
target_bc: testing
dependencies: [TASK-0370]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0379: Add assert_first_event and assert_last_event to EventAssertions

- **Status**: Complete
- **Target Bounded Context**: `testing`

## Objective

Add assert_first_event and assert_last_event to EventAssertions, preserving all behavioral contracts and keeping modified files well below the architectural line ceiling.
