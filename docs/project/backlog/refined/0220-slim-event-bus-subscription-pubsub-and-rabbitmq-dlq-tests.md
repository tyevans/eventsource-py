---
id: TASK-0220
title: "Slim event bus subscription, redis pubsub, and rabbitmq dlq test modules below 340 lines"
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

# TASK-0220: Slim event bus subscription, redis pubsub, and rabbitmq dlq test modules below 340 lines

## Summary
Compact `tests/unit/test_event_bus/test_event_bus_subscriptions.py` (388 lines), `tests/unit/test_redis_event_bus/test_redis_event_bus_pubsub.py` (377 lines), and `tests/integration/bus/test_rabbitmq/test_dlq.py` (383 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
