---
id: TASK-0390
title: "Add clone(**overrides) helper to EventBuilder"
status: Complete
target_bc: testing
dependencies: [TASK-0381]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0390: Add clone(**overrides) helper to EventBuilder

- **Status**: Complete
- **Target Bounded Context**: `testing`

## Objective

Add clone(**overrides) helper to EventBuilder, preserving all behavioral contracts and keeping modified files well below the architectural line ceiling.
