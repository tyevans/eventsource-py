---
id: TASK-0328
title: "Compact 8 src modules at 240-241 lines below 220 lines"
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

# TASK-0328: Compact 8 src modules at 240-241 lines below 220 lines

## Summary
Compact `src/eventsource/application/projections/handlers.py` (241 lines), `src/eventsource/adapters/sql/schemas/__init__.py` (241 lines), `src/eventsource/adapters/rabbitmq/publisher_batch.py` (241 lines), `src/eventsource/adapters/rabbitmq/consumer_retry.py` (241 lines), `src/eventsource/adapters/postgresql/readmodels_query.py` (241 lines), `src/eventsource/testing/conformance_ports/category.py` (240 lines), `src/eventsource/adapters/sql/checkpoints.py` (240 lines), and `src/eventsource/adapters/kafka/dlq.py` (240 lines) strictly below 220 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
