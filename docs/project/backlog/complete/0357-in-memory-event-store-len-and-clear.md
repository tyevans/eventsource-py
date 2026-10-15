---
id: TASK-0357
title: "Add __len__ and synchronous clear() to InMemoryEventStore"
status: Complete
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

# TASK-0357: Add __len__ and synchronous clear() to InMemoryEventStore

## Summary
Add `__len__(self) -> int` (returning `len(self._global_events)` in O(1)) and `clear() -> None` to `InMemoryEventStore` in `src/eventsource/adapters/memory/store.py`, keeping `store.py` strictly below 210 lines.
