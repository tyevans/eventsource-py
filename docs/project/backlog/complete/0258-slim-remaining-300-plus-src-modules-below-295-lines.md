---
id: TASK-0258
title: "Slim coordinator_control.py, postgresql/locks.py, manager_lifecycle.py, snapshotting.py, and lifecycle.py below 295 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0258: Slim coordinator_control.py, postgresql/locks.py, manager_lifecycle.py, snapshotting.py, and lifecycle.py below 295 lines

## Summary
Compact the final 5 `src/eventsource/` modules at >= 300 lines (`coordinator_control.py`, `postgresql/locks.py`, `manager_lifecycle.py`, `snapshotting.py`, and `lifecycle.py`) strictly below 295 lines so 100% of `src/eventsource/` is < 300 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
