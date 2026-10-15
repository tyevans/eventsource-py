---
id: TASK-0231
title: "Slim subscription error models, in-memory leader elector, and post-shutdown hook tests below 320 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0231: Slim subscription error models, in-memory leader elector, and post-shutdown hook tests below 320 lines

## Summary
Compact `tests/unit/application/subscriptions/test_error_handling/test_error_models.py` (362 lines), `tests/unit/application/subscriptions/test_coordination/test_in_memory_leader_elector.py` (360 lines), and `tests/unit/application/subscriptions/test_shutdown/test_post_shutdown_hooks.py` (354 lines) below 320 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
