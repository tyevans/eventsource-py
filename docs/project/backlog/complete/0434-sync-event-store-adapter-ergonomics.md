---
id: TASK-0434
title: "Add stream_exists(), read_events(), and single-event/default-expected append() to SyncEventStoreAdapter"
status: Complete
target_bc: adapters
dependencies: [TASK-0425]
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
adrs: [ADR-0002, ADR-0003, ADR-0005]
---

# TASK-0434: Add stream_exists(), read_events(), and single-event/default-expected append() to SyncEventStoreAdapter

- **Status**: Refined
- **Target Bounded Context**: `adapters`

## Objective

Enhance `SyncEventStoreAdapter` in `src/eventsource/adapters/sync/adapter.py` with `stream_exists()`, `read_events()`, and `append(stream, events: DomainEvent | Sequence[DomainEvent], expected: ExpectedVersion = ExpectedVersion.ANY)`, while compacting `adapter.py` below 160 lines.
