---
id: TASK-0358
title: "Sync Diataxis testing guide for InMemoryEventStore and InMemoryReadModelRepository helpers"
status: Complete
target_bc: documentation
prd: docs/project/product/accepted/PRD-0008-ergonomic-aggregate-testing-and-compact-adapters.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0008-ergonomic-aggregate-testing-and-compact-adapters.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-500-line-file-limit.md
  - docs/project/adrs/accepted/ADR-0003-blackbox-testing.md
  - docs/project/adrs/accepted/ADR-0009-property-and-mutation-testing.md
dependencies: []
---

# TASK-0358: Sync Diataxis testing guide for InMemoryEventStore and InMemoryReadModelRepository helpers

## Summary
Update `docs/guides/testing.md` to document `len(store)`, `store.clear()`, `len(read_model_repo)`, `await read_model_repo.get_or_raise(id)`, and `await read_model_repo.clear()`.
