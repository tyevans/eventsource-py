---
id: TASK-0237
title: "Slim broker metrics, reconnection, shutdown metrics, load benchmark, and reliability tests below 310 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0237: Slim broker metrics, reconnection, shutdown metrics, load benchmark, and reliability tests below 310 lines

## Summary
Compact `tests/unit/test_kafka_event_bus/test_kafka_event_bus_metrics.py` (349 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_reconnection.py` (348 lines), `tests/unit/application/subscriptions/test_shutdown/test_shutdown_metrics.py` (348 lines), `tests/unit/application/migration/test_load_benchmarks/test_bulk_copy_and_dual_write.py` (348 lines), and `tests/integration/bus/test_rabbitmq/test_reliability.py` (348 lines) below 310 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
