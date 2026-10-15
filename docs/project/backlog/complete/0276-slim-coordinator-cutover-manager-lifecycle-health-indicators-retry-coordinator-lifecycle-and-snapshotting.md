---
id: TASK-0276
title: "Slim coordinator_cutover, manager_lifecycle, health_subscription_indicators, retry, coordinator_lifecycle, and snapshotting below 265 lines"
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

# TASK-0276: Slim coordinator_cutover, manager_lifecycle, health_subscription_indicators, retry, coordinator_lifecycle, and snapshotting below 265 lines

## Summary
Compact `src/eventsource/application/migration/coordinator_cutover.py`, `src/eventsource/application/subscriptions/manager_lifecycle.py`, `src/eventsource/application/subscriptions/health_subscription_indicators.py`, `src/eventsource/application/projections/retry.py`, `src/eventsource/application/migration/coordinator_lifecycle.py`, and `src/eventsource/application/aggregates/snapshotting.py` strictly below 265 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
