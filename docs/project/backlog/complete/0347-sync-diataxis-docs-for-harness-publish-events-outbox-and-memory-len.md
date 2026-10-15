---
id: TASK-0347
title: "Sync Diataxis testing guide for InMemoryTestHarness.outbox_repo, publish_events(), and len()"
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
dependencies:
  - TASK-0345
  - TASK-0346
---

# TASK-0347: Sync Diataxis testing guide for InMemoryTestHarness.outbox_repo, publish_events(), and len()

## Summary
Update `docs/guides/testing.md` to document `harness.outbox_repo`, `await harness.publish_events(*events, persist=...)`, `len(harness)`, and `len(...)` across in-memory repositories.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
