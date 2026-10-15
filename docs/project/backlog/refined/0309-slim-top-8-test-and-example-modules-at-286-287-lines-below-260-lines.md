---
id: TASK-0309
title: "Compact top 8 test and example modules at 286-287 lines below 260 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0006
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0309: Compact top 8 test and example modules at 286-287 lines below 260 lines

## Summary
Compact `tests/unit/application/migration/test_position_mapper/test_position_mapper_init_and_record.py` (287 lines), `tests/unit/application/migration/test_bulk_copier/test_bulk_copier_run.py` (287 lines), `examples/aggregate_example.py` (287 lines), `tests/unit/testing/test_multi_aggregate_bdd.py` (286 lines), `tests/unit/test_kafka_event_bus/_fixtures.py` (286 lines), `tests/unit/domain/test_aggregate_root/_fixtures.py` (286 lines), `tests/unit/application/migration/test_phase3_integration/test_phase3_subscriptions.py` (286 lines), and `tests/unit/application/aggregates/test_repository_snapshot/test_repository_snapshot_load.py` (286 lines) strictly below 260 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
