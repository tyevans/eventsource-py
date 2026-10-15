---
id: TASK-0424
title: "Sync Diataxis readmodel documentation for Wave 26 query and repository ergonomics"
status: Complete
target_bc: documentation
dependencies: [TASK-0414]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0424: Sync Diataxis readmodel documentation for Wave 26 query and repository ergonomics

- **Status**: Complete
- **Target Bounded Context**: `documentation`

## Objective

Update `docs/guides/read-models.md` and `docs/api/readmodels.md` to document `Query.where(...)`, variadic/keyword `with_filter(...)`, `find_one(...)`, keyword filter shortcuts on `find`/`count`/`find_one`, and `Iterable` batch/filter parameters.
