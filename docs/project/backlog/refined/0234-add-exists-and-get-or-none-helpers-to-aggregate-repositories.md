---
id: TASK-0234
title: "Add ergonomic get_or_none(aggregate_id) helper to AggregateRepository and TenantAwareRepository"
status: Refined
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

# TASK-0234: Add ergonomic get_or_none(aggregate_id) helper to AggregateRepository and TenantAwareRepository

## Summary
Add an ergonomic `await repo.get_or_none(aggregate_id)` helper to `AggregateRepository` and `TenantAwareRepository` that returns `AggregateRoot[TState] | None` (returning `None` instead of raising `AggregateNotFoundError` when the stream has no events) with unit tests in `tests/unit/application/aggregates/`.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
