---
id: TASK-0201
title: "Slim Subscription Lifecycle Module and Clean Up Re-Export Facades"
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

# TASK-0201: Slim Subscription Lifecycle Module and Clean Up Re-Export Facades

## Summary
Slim src/eventsource/application/subscriptions/lifecycle.py below 350 lines and clean up legacy facade docstrings in subscription.py, transition.py, and runners/live.py.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
