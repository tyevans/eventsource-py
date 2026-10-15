---
id: TASK-0283
title: "Optimize InMemoryEventStore append and read_stream to O(1) and remove _by_category shim"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0283: Optimize InMemoryEventStore append and read_stream to O(1) and remove _by_category shim

## Summary
Optimize `InMemoryEventStore.append` to extend stream/category index lists in-place (`O(1)` amortized instead of `O(N)` list copies per append), replace `bisect` in `read_stream` with `O(1)` contiguous version arithmetic, remove the unused `_by_category` alias shim, and compact `src/eventsource/adapters/memory/store.py` below 265 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
