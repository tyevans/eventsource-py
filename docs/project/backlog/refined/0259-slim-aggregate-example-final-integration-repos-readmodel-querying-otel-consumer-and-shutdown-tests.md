---
id: TASK-0259
title: "Slim aggregate_example.py, final_integration/_repositories.py, test_querying.py, otel consumer, and pre_shutdown_hooks tests below 300 lines"
status: Refined
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

# TASK-0259: Slim aggregate_example.py, final_integration/_repositories.py, test_querying.py, otel consumer, and pre_shutdown_hooks tests below 300 lines

## Summary
Compact `examples/aggregate_example.py`, `tests/unit/application/migration/test_final_integration/_repositories.py`, `tests/integration/readmodels/test_repositories/test_querying.py`, `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_opentelemetry_consumer.py`, and `tests/unit/application/subscriptions/test_shutdown/test_pre_shutdown_hooks.py` strictly below 300 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
