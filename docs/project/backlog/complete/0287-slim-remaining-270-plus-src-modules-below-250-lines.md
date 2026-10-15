---
id: TASK-0287
title: "Compact remaining 270+ line src/eventsource modules below 250 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0006
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0287: Compact remaining 270+ line src/eventsource modules below 250 lines

## Summary
Compact `src/eventsource/application/projections/coordinator_registry.py` (274 lines), `src/eventsource/application/subscriptions/runners/catchup_batch.py` (273 lines), `src/eventsource/adapters/redis/bus_dlq.py` (273 lines), `src/eventsource/application/migration/sync_lag_tracker_analysis.py` (271 lines), `src/eventsource/application/migration/error_handling.py` (271 lines), and `src/eventsource/adapters/memory/outbox.py` (270 lines) strictly below 250 lines so 100% of `src/eventsource/` is strictly `< 270` lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
