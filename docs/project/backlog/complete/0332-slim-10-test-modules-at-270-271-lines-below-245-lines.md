---
id: TASK-0332
title: "Compact 10 test modules at 270-271 lines below 245 lines"
status: Complete
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

# TASK-0332: Compact 10 test modules at 270-271 lines below 245 lines

## Summary
Compact `tests/unit/test_rabbitmq_event_bus/__init__.py` (271 lines), `tests/unit/application/migration/test_coordinator_subscriptions/test_coordinator_subscriptions_migrate.py` (271 lines), `tests/unit/adapters/sync/test_adapter/test_queries_and_lifecycle.py` (271 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_message_processing.py` (270 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_exchanges_fanout.py` (270 lines), `tests/unit/readmodels/test_projection/test_projection_construction.py` (270 lines), `tests/unit/application/subscriptions/test_processing_timeout.py` (270 lines), `tests/unit/application/projections/test_projection_base/test_projection_unregistered_events.py` (270 lines), `tests/unit/application/migration/test_chaos/test_locks_and_timeouts.py` (270 lines), and `tests/unit/adapters/sql/migration/test_position_mapping_repository/test_queries.py` (270 lines) strictly below 245 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
