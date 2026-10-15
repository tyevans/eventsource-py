---
id: TASK-0340
title: "Compact 10 src modules at 230-231 lines below 215 lines"
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

# TASK-0340: Compact 10 src modules at 230-231 lines below 215 lines

## Summary
Compact `src/eventsource/application/projections/base_checkpoint.py` (231 lines), `src/eventsource/application/migration/subscription_migrator_execution.py` (231 lines), `src/eventsource/application/migration/bulk_copier_writer.py` (231 lines), `src/eventsource/adapters/sqlite/store.py` (231 lines), `src/eventsource/application/migration/metrics_recorder.py` (230 lines), `src/eventsource/application/migration/coordinator_cutover.py` (230 lines), `src/eventsource/adapters/sql/migration/audit_log.py` (230 lines), `src/eventsource/adapters/rabbitmq/publisher.py` (230 lines), `src/eventsource/adapters/postgresql/store_append.py` (230 lines), and `src/eventsource/adapters/kafka/consumer_message.py` (230 lines) strictly below 215 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
