---
id: TASK-0422
title: "Add Query.where(), variadic/keyword with_filter(), and Iterable Filter.in_/not_in"
status: Refined
target_bc: ports
dependencies: [TASK-0414]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0422: Add Query.where(), variadic/keyword with_filter(), and Iterable Filter.in_/not_in

- **Status**: Refined
- **Target Bounded Context**: `ports`

## Objective

Enhance `Query` and `Filter` in `src/eventsource/ports/readmodels/query.py` with `Query.where(*filters, **eq_filters)`, variadic/keyword `with_filter(*filters, **eq_filters)`, and `Iterable` support on `Filter.in_` and `Filter.not_in`.
