---
id: TASK-0222
title: "Slim projection checkpoints, leadership lease, and tenant repository save tests below 340 lines"
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

# TASK-0222: Slim projection checkpoints, leadership lease, and tenant repository save tests below 340 lines

## Summary
Compact `tests/unit/application/projections/test_projection_base/test_projection_checkpoints_and_declarative.py` (382 lines), `tests/unit/application/subscriptions/test_leadership_lease.py` (380 lines), and `tests/unit/application/aggregates/test_tenant_repository/test_tenant_repository_save.py` (376 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
