---
id: TASK-0218
title: "Slim unit migration test modules (subscription migrator, dual write, bulk copier) below 340 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0218: Slim unit migration test modules (subscription migrator, dual write, bulk copier) below 340 lines

## Summary
Compact `tests/unit/application/migration/test_subscription_migrator/test_subscription_migrator_models_and_workflows.py` (390 lines), `tests/unit/application/migration/test_coordinator_dual_write/test_dual_write_transition_and_cutover.py` (378 lines), `tests/unit/application/migration/test_subscription_migrator/test_subscription_migrator_migrate.py` (375 lines), and `tests/unit/application/migration/test_bulk_copier/test_bulk_copier_run.py` (375 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
