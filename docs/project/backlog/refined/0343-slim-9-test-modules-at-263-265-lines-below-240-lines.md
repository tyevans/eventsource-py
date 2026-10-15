---
id: TASK-0343
title: "Compact 9 test modules at 263-265 lines below 240 lines"
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

# TASK-0343: Compact 9 test modules at 263-265 lines below 240 lines

## Summary
Compact `tests/unit/application/migration/test_coordinator_dual_write/test_dual_write_transition_and_cutover.py` (265 lines), `tests/unit/adapters/kafka/test_facade_wiring.py` (265 lines), `tests/repositories/test_sqlite_repos/test_dlq_crud.py` (265 lines), `tests/unit/application/subscriptions/test_subscription_manager/test_manager_lifecycle.py` (264 lines), `tests/unit/application/subscriptions/test_live_batch_dispatch.py` (264 lines), `tests/unit/application/migration/test_migration_metrics/test_metrics_trackers_and_timers.py` (264 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_dlq_retry.py` (263 lines), `tests/unit/testing/test_builder/test_builder_methods.py` (263 lines), and `tests/unit/application/subscriptions/test_coordination/test_messages_and_peers.py` (263 lines) strictly below 240 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
