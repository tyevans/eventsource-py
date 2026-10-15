---
id: TASK-0247
title: "Slim sync/adapter.py, subscription_migrator_execution.py, memory/store.py, cutover_execution.py, and position_mapping_query.py below 300 lines"
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

# TASK-0247: Slim sync/adapter.py, subscription_migrator_execution.py, memory/store.py, cutover_execution.py, and position_mapping_query.py below 300 lines

## Summary
Compact `src/eventsource/adapters/sync/adapter.py`, `src/eventsource/application/migration/subscription_migrator_execution.py`, `src/eventsource/adapters/memory/store.py`, `src/eventsource/application/migration/cutover_execution.py`, and `src/eventsource/adapters/sql/migration/position_mapping_query.py` strictly below 300 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
