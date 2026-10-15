---
id: TASK-0333
title: "Compact 8 test modules at 268-269 lines below 245 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0333: Compact 8 test modules at 268-269 lines below 245 lines

## Summary
Compact `tests/unit/application/subscriptions/test_subscription_state/test_subscription_state_transitions.py` (269 lines), `tests/unit/application/migration/test_exceptions/test_exceptions_lifecycle_errors.py` (269 lines), `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_state_and_readiness.py` (269 lines), `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_init_and_flow.py` (269 lines), `tests/unit/application/migration/test_coordinator_resync.py` (269 lines), `tests/unit/application/aggregates/test_repository_store_port.py` (269 lines), `tests/unit/adapters/sql/migration/test_routing_repository/test_state_and_listing.py` (269 lines), and `tests/unit/application/subscriptions/test_coordination/test_in_memory_leader_elector.py` (268 lines) strictly below 245 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
