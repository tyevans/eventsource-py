---
id: TASK-0311
title: "Compact 8 test modules at 281-282 lines below 260 lines"
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

# TASK-0311: Compact 8 test modules at 281-282 lines below 260 lines

## Summary
Compact `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_connection.py` (282 lines), `tests/unit/readmodels/test_optimistic_locking.py` (282 lines), `tests/unit/domain/test_aggregate_root/test_unregistered_events.py` (282 lines), `tests/unit/application/subscriptions/test_shutdown/test_shutdown_models.py` (282 lines), `tests/unit/application/subscriptions/test_manager_pause_resume/test_pause_resume_single.py` (282 lines), `tests/unit/application/migration/test_load_benchmarks/test_memory_and_concurrent.py` (282 lines), `tests/unit/test_kafka_event_bus/test_kafka_event_bus_config.py` (281 lines), and `tests/unit/test_event_bus/test_event_bus_subscriptions.py` (281 lines) strictly below 260 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
