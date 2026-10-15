---
id: TASK-0423
title: "Add find_one(), keyword filter shortcuts, and Iterable get_many/save_many to ReadModelRepository"
status: Refined
target_bc: adapters
dependencies: [TASK-0414]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0423: Add find_one(), keyword filter shortcuts, and Iterable get_many/save_many to ReadModelRepository

- **Status**: Refined
- **Target Bounded Context**: `adapters`

## Objective

Add `find_one(query=None, **eq_filters)`, keyword equality shortcuts on `find` and `count`, and `Iterable` support on `get_many`/`save_many` across `ReadModelRepository`, `InMemoryReadModelRepository`, `SQLiteReadModelRepository`, and `PostgreSQLReadModelRepository`, while compacting `ports/readmodels/repository.py`, `sqlite/readmodels.py`, and `postgresql/readmodels.py` below 165 lines.
