---
id: TASK-0356
title: "Add get_or_raise(id) and clear() to InMemoryReadModelRepository"
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

# TASK-0356: Add get_or_raise(id) and clear() to InMemoryReadModelRepository

## Summary
Add `async def get_or_raise(self, id: UUID) -> TModel` (raising `ReadModelNotFoundError` when missing) and `async def clear(self) -> int` (alias delegating to `truncate()`) plus `__len__` (`len(repo)`) to `InMemoryReadModelRepository` in `src/eventsource/adapters/memory/readmodels.py`.
