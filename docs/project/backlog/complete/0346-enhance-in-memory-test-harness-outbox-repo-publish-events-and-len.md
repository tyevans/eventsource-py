---
id: TASK-0346
title: "Add outbox_repo, publish_events(*events, persist=...), and __len__ to InMemoryTestHarness"
status: Complete
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0346: Add outbox_repo, publish_events(*events, persist=...), and __len__ to InMemoryTestHarness

## Summary
Add `outbox_repo: InMemoryOutboxRepository`, `await harness.publish_events(*events, persist=False)`, and `__len__` (`len(harness)`) to `InMemoryTestHarness` (`src/eventsource/testing/harness.py`).

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
