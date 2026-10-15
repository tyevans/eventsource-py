---
id: TASK-0240
title: "Add ergonomic exists(aggregate_id) helper to AggregateRepository and TenantAwareRepository"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0240: Add ergonomic exists(aggregate_id) helper to AggregateRepository and TenantAwareRepository

## Summary
Add `async def exists(self, aggregate_id: UUID) -> bool:` to `AggregateRepository` and `TenantAwareRepository` with unit tests in `tests/unit/application/aggregates/`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
