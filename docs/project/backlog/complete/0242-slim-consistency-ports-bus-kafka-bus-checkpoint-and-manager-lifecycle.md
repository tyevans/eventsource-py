---
id: TASK-0242
title: "Slim migration/consistency.py, ports/bus.py, kafka/bus.py, base_checkpoint.py, and manager_lifecycle.py below 305 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0242: Slim migration/consistency.py, ports/bus.py, kafka/bus.py, base_checkpoint.py, and manager_lifecycle.py below 305 lines

## Summary
Compact `src/eventsource/application/migration/consistency.py` (328 lines), `src/eventsource/ports/bus.py` (327 lines), `src/eventsource/adapters/kafka/bus.py` (326 lines), `src/eventsource/application/projections/base_checkpoint.py` (324 lines), and `src/eventsource/application/subscriptions/manager_lifecycle.py` (323 lines) below 305 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
