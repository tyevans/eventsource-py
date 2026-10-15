---
id: TASK-0200
title: "Slim Migration Coordinator Cutover and Subscription Migrator Execution Modules"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0007
dependencies: []
---

# TASK-0200: Slim Migration Coordinator Cutover and Subscription Migrator Execution Modules

## Summary
Slim coordinator_cutover.py and subscription_migrator_execution.py below 350 lines for additional headroom below the 400-line warning threshold.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
