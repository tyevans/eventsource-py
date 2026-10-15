---
id: TASK-0228
title: "Slim redis consumer, rabbitmq publish, and rabbitmq exchange type tests below 320 lines"
status: Refined
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

# TASK-0228: Slim redis consumer, rabbitmq publish, and rabbitmq exchange type tests below 320 lines

## Summary
Compact `tests/unit/test_redis_event_bus/test_redis_event_bus_consumer.py` (370 lines), `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_publish.py` (369 lines), and `tests/integration/bus/test_rabbitmq/test_exchange_types.py` (367 lines) below 320 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
