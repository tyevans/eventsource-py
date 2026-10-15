---
id: TASK-0288
title: "Compact top 7 test modules at 301-303 lines below 275 lines"
status: Refined
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0288: Compact top 7 test modules at 301-303 lines below 275 lines

## Summary
Compact `tests/unit/adapters/test_sqlite_outbox/test_sqlite_outbox_standalone.py` (303 lines), `tests/unit/adapters/serialization/test_json/test_contract.py` (303 lines), `tests/unit/application/migration/test_router/test_router_routing_and_concurrency.py` (302 lines), `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_locks_and_lag.py` (302 lines), `tests/unit/application/migration/test_chaos/_repositories.py` (302 lines), `tests/unit/adapters/sql/migration/test_audit_log_repository/test_helpers_and_factories.py` (302 lines), and `tests/unit/test_rabbitmq_event_bus/__init__.py` (301 lines) strictly below 275 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
