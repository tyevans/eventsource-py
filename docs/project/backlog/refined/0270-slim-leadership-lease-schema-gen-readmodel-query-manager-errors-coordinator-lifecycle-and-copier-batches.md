---
id: TASK-0270
title: "Slim leadership_lease, schema_generation, test_query, manager_error_handling, coordinator_lifecycle, and bulk_copier_batches tests below 295 lines"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0270: Slim leadership_lease, schema_generation, test_query, manager_error_handling, coordinator_lifecycle, and bulk_copier_batches tests below 295 lines

## Summary
Compact `tests/unit/application/subscriptions/test_leadership_lease.py`, `tests/unit/readmodels/test_schema/test_schema_generation.py`, `tests/unit/readmodels/test_query.py`, `tests/unit/application/subscriptions/test_subscription_manager/test_manager_error_handling.py`, `tests/unit/application/migration/test_coordinator/test_coordinator_lifecycle.py`, and `tests/unit/application/migration/test_bulk_copier/test_bulk_copier_batches.py` strictly below 295 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
