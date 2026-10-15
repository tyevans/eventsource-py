---
id: TASK-0266
title: "Slim redis/bus_consume.py, consistency_stream_checker.py, rabbitmq/serialization.py, pause_resume.py, and sql/checkpoints.py below 275 lines"
status: Refined
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

# TASK-0266: Slim redis/bus_consume.py, consistency_stream_checker.py, rabbitmq/serialization.py, pause_resume.py, and sql/checkpoints.py below 275 lines

## Summary
Compact `src/eventsource/adapters/redis/bus_consume.py`, `src/eventsource/application/migration/consistency_stream_checker.py`, `src/eventsource/adapters/rabbitmq/serialization.py`, `src/eventsource/application/subscriptions/pause_resume.py`, and `src/eventsource/adapters/sql/checkpoints.py` strictly below 275 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 295` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
