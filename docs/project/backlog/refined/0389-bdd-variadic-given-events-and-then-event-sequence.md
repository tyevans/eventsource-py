---
id: TASK-0389
title: "Support variadic event arguments in given_events and then_event_sequence"
status: Refined
target_bc: testing
dependencies: [TASK-0381]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0389: Support variadic event arguments in given_events and then_event_sequence

- **Status**: Refined
- **Target Bounded Context**: `testing`

## Objective

Support variadic event arguments in given_events and then_event_sequence, preserving all behavioral contracts and keeping modified files well below the architectural line ceiling.
