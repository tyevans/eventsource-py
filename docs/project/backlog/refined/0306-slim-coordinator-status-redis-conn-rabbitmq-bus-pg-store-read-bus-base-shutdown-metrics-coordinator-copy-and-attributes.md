---
id: TASK-0306
title: "Compact top 8 src/eventsource modules at 257-259 lines below 235 lines"
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

# TASK-0306: Compact top 8 src/eventsource modules at 257-259 lines below 235 lines

## Summary
Compact `src/eventsource/application/migration/coordinator_status.py` (259 lines), `src/eventsource/adapters/redis/bus_connection.py` (259 lines), `src/eventsource/adapters/rabbitmq/bus.py` (259 lines), `src/eventsource/adapters/postgresql/store_read.py` (259 lines), `src/eventsource/adapters/_bus/base.py` (259 lines), `src/eventsource/application/subscriptions/shutdown_metrics.py` (258 lines), `src/eventsource/application/migration/coordinator_copy.py` (258 lines), and `src/eventsource/observability/attributes.py` (257 lines) strictly below 235 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
