---
id: TASK-0271
title: "Slim coordinator subscriptions verify/migrate, position mapping lifecycle, module structure, sqlite construction, memory checkpoints, and subscriptions conftest below 295 lines"
status: Complete
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

# TASK-0271: Slim coordinator subscriptions verify/migrate, position mapping lifecycle, module structure, sqlite construction, memory checkpoints, and subscriptions conftest below 295 lines

## Summary
Compact `tests/unit/application/migration/test_coordinator_subscriptions/test_coordinator_subscriptions_verify.py`, `tests/unit/application/migration/test_coordinator_subscriptions/test_coordinator_subscriptions_migrate.py`, `tests/unit/adapters/sql/migration/test_position_mapping_repository/test_protocol_and_lifecycle.py`, `tests/unit/testing/test_module_structure.py`, `tests/unit/readmodels/test_sqlite/test_sqlite_construction.py`, `tests/unit/adapters/test_memory_checkpoints/test_memory.py`, and `tests/integration/subscriptions/conftest.py` strictly below 295 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
