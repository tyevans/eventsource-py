---
id: TASK-0273
title: "Add save_all and load_many batch helpers to AggregateRepository and TenantAwareRepository"
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

# TASK-0273: Add save_all and load_many batch helpers to AggregateRepository and TenantAwareRepository

## Summary
Add `save_all(*aggregates)` and `load_many(aggregate_ids, *, ignore_missing=False)` to `AggregateRepository` and `TenantAwareRepository` with unit tests.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
