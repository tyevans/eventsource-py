---
id: TASK-0232
title: "Slim rabbitmq/consumer.py, rabbitmq/bus_health.py, rabbitmq/consumer_dispatch.py, and kafka/dlq.py below 315 lines"
status: Refined
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0232: Slim rabbitmq/consumer.py, rabbitmq/bus_health.py, rabbitmq/consumer_dispatch.py, and kafka/dlq.py below 315 lines

## Summary
Compact `src/eventsource/adapters/rabbitmq/consumer.py` (337 lines), `src/eventsource/adapters/rabbitmq/bus_health.py` (336 lines), `src/eventsource/adapters/rabbitmq/consumer_dispatch.py` (335 lines), and `src/eventsource/adapters/kafka/dlq.py` (333 lines) below 315 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
