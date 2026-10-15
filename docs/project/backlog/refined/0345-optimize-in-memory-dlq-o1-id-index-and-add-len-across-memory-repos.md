---
id: TASK-0345
title: "Optimize InMemoryDLQRepository O(1) ID lookup and add __len__ to in-memory repositories"
status: Refined
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0345: Optimize InMemoryDLQRepository O(1) ID lookup and add __len__ to in-memory repositories

## Summary
Index DLQ entries by `id` in `InMemoryDLQRepository` (`src/eventsource/adapters/memory/dlq.py`) for O(1) lookup in `get_failed_event_by_id`, `mark_resolved`, and `mark_retrying`, and add `__len__` across `InMemoryDLQRepository`, `InMemoryCheckpointRepository`, `InMemoryOutboxRepository`, and `InMemorySnapshotStore`.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
