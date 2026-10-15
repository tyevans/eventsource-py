---
id: TASK-0339
title: "Compact 10 src modules at 232-234 lines below 215 lines"
status: Complete
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

# TASK-0339: Compact 10 src modules at 232-234 lines below 215 lines

## Summary
Compact `src/eventsource/ports/subscribers.py` (234 lines), `src/eventsource/ports/migration/repositories/position.py` (234 lines), `src/eventsource/application/subscriptions/lifecycle.py` (234 lines), `src/eventsource/application/subscriptions/runners/catchup_runner.py` (233 lines), `src/eventsource/adapters/kafka/connection.py` (233 lines), `src/eventsource/testing/conformance_ports/readmodels.py` (232 lines), `src/eventsource/application/subscriptions/runners/catchup_batch.py` (232 lines), `src/eventsource/application/migration/coordinator_copy.py` (232 lines), `src/eventsource/adapters/rabbitmq/consumer_dispatch.py` (232 lines), and `src/eventsource/adapters/kafka/consumer_dispatch.py` (232 lines) strictly below 215 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
