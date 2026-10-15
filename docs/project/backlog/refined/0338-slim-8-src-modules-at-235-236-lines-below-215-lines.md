---
id: TASK-0338
title: "Compact 8 src modules at 235-236 lines below 215 lines"
status: Refined
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

# TASK-0338: Compact 8 src modules at 235-236 lines below 215 lines

## Summary
Compact `src/eventsource/application/subscriptions/transition_executor.py` (236 lines), `src/eventsource/application/migration/error_handling.py` (236 lines), `src/eventsource/application/migration/consistency.py` (236 lines), `src/eventsource/application/background_tasks.py` (236 lines), `src/eventsource/adapters/serialization/delta.py` (236 lines), `src/eventsource/application/migration/status_streamer_core.py` (235 lines), `src/eventsource/application/aggregates/tenant_repository.py` (235 lines), and `src/eventsource/adapters/redis/bus_consume.py` (235 lines) strictly below 215 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
