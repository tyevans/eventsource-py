---
id: TASK-0329
title: "Compact 9 src modules at 237-239 lines below 220 lines"
status: Complete
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0329: Compact 9 src modules at 237-239 lines below 220 lines

## Summary
Compact `src/eventsource/ports/handlers.py` (239 lines), `src/eventsource/adapters/sql/dlq.py` (239 lines), `src/eventsource/application/subscriptions/runners/live_runner.py` (238 lines), `src/eventsource/adapters/sql/migration/routing_mutation.py` (238 lines), `src/eventsource/testing/conformance_ports/dlq.py` (237 lines), `src/eventsource/domain/exceptions/snapshots.py` (237 lines), `src/eventsource/domain/event.py` (237 lines), `src/eventsource/application/subscriptions/runners/live_delivery.py` (237 lines), and `src/eventsource/adapters/redis/bus_dlq.py` (237 lines) strictly below 220 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
