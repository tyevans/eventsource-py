---
id: TASK-0256
title: "Slim sql/projection.py, redis/bus_dlq.py, coordinator_copy.py, consumer_dispatch.py, and live_drain.py below 295 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0256: Slim sql/projection.py, redis/bus_dlq.py, coordinator_copy.py, consumer_dispatch.py, and live_drain.py below 295 lines

## Summary
Compact `src/eventsource/adapters/sql/projection.py`, `src/eventsource/adapters/redis/bus_dlq.py`, `src/eventsource/application/migration/coordinator_copy.py`, `src/eventsource/adapters/rabbitmq/consumer_dispatch.py`, and `src/eventsource/application/subscriptions/runners/live_drain.py` strictly below 295 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
