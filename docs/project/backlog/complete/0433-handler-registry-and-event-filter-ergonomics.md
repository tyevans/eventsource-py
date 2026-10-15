---
id: TASK-0433
title: "Add HandlerRegistry container dunders and EventFilter callable/filter() ergonomics"
status: Complete
target_bc: application
dependencies: [TASK-0425]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0433: Add HandlerRegistry container dunders and EventFilter callable/filter() ergonomics

- **Status**: Refined
- **Target Bounded Context**: `application`

## Objective

Add `__len__`, `__bool__`, `__contains__`, `__iter__`, and instance-tolerant `has_handler`/`get_handler` to `HandlerRegistry`, and `__call__`, `filter(items)`, and iterable-tolerant collection fields to `EventFilter`, while compacting `handlers.py` and `filtering.py` below 160 lines.
