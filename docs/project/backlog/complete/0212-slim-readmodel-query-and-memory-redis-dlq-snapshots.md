---
id: TASK-0212
title: "Slim ports/readmodels/query.py, memory/dlq.py, memory/snapshots.py, and redis/bus_dlq.py below 340 lines"
status: Complete
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

# TASK-0212: Slim ports/readmodels/query.py, memory/dlq.py, memory/snapshots.py, and redis/bus_dlq.py below 340 lines

## Summary
Compact `src/eventsource/ports/readmodels/query.py` (365 lines), `src/eventsource/adapters/memory/dlq.py` (360 lines), `src/eventsource/adapters/memory/snapshots.py` (350 lines), and `src/eventsource/adapters/redis/bus_dlq.py` (355 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
