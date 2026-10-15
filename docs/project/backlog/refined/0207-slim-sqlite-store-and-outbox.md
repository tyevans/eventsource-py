---
id: TASK-0207
title: "Slim sqlite/store.py and sqlite/outbox.py below 340 lines"
status: Refined
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

# TASK-0207: Slim sqlite/store.py and sqlite/outbox.py below 340 lines

## Summary
Compact `src/eventsource/adapters/sqlite/store.py` (389 lines) and `src/eventsource/adapters/sqlite/outbox.py` (359 lines) below 340 lines by trimming verbose docstrings and extracting focused helpers where appropriate.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
