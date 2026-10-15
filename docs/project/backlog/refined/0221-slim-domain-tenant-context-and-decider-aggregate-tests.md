---
id: TASK-0221
title: "Slim test_tenant_context.py and test_decider_aggregate.py below 340 lines"
status: Refined
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0221: Slim test_tenant_context.py and test_decider_aggregate.py below 340 lines

## Summary
Compact `tests/unit/domain/test_tenant_context.py` (386 lines) and `tests/unit/domain/test_decider_aggregate.py` (382 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
