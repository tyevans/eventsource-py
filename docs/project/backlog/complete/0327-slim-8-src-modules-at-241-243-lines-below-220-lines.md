---
id: TASK-0327
title: "Compact 8 src modules at 241-243 lines below 220 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0327: Compact 8 src modules at 241-243 lines below 220 lines

## Summary
Compact `src/eventsource/application/subscriptions/shutdown_runner.py` (243 lines), `src/eventsource/application/subscriptions/runners/live_drain.py` (243 lines), `src/eventsource/adapters/sql/migration/position_mapping_query.py` (243 lines), `src/eventsource/adapters/memory/checkpoints.py` (243 lines), `src/eventsource/adapters/kafka/bus.py` (243 lines), `src/eventsource/ports/coordination.py` (242 lines), `src/eventsource/application/migration/cutover_execution.py` (242 lines), and `src/eventsource/adapters/postgresql/outbox.py` (242 lines) strictly below 220 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
