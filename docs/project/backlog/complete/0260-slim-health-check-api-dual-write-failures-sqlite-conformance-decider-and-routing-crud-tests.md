---
id: TASK-0260
title: "Slim health_check_api, dual_write_failures, sqlite_conformance, decider_aggregate, and routing_repository crud tests below 300 lines"
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

# TASK-0260: Slim health_check_api, dual_write_failures, sqlite_conformance, decider_aggregate, and routing_repository crud tests below 300 lines

## Summary
Compact `tests/integration/subscriptions/test_advanced_features/test_health_check_api.py`, `tests/unit/application/migration/test_dual_write/test_dual_write_failures.py`, `tests/unit/adapters/test_sqlite_conformance.py`, `tests/unit/domain/test_decider_aggregate.py`, and `tests/unit/adapters/sql/migration/test_routing_repository/test_protocol_and_crud.py` strictly below 300 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
