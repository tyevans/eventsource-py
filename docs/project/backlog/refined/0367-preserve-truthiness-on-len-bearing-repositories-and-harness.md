---
id: TASK-0367
title: "Ensure __bool__ returns True across all __len__-bearing stores, repositories, and testing helpers"
status: Refined
target_bc: adapters
prd: docs/project/product/accepted/PRD-0008-ergonomic-aggregate-testing-and-compact-adapters.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0008-ergonomic-aggregate-testing-and-compact-adapters.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-500-line-file-limit.md
  - docs/project/adrs/accepted/ADR-0003-blackbox-testing.md
  - docs/project/adrs/accepted/ADR-0009-property-and-mutation-testing.md
dependencies: []
---

# TASK-0367: Ensure __bool__ returns True across all __len__-bearing stores, repositories, and testing helpers

## Summary
Define `def __bool__(self) -> bool: return True` on `InMemoryCheckpointRepository`, `InMemoryDLQRepository`, `InMemoryOutboxRepository`, `InMemoryReadModelRepository`, `InMemorySnapshotStore`, `EventAssertions`, `InMemoryTestHarness`, and `RecordingEventBus`, and use explicit `is not None` checks in `coordinator_copy.py` and `recording.py`.
