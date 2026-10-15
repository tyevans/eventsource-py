---
id: TASK-0229
title: "Slim integration subscriptions conftest, checkpoint repository, and optimistic locking tests below 320 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0229: Slim integration subscriptions conftest, checkpoint repository, and optimistic locking tests below 320 lines

## Summary
Compact `tests/integration/subscriptions/conftest.py` (365 lines), `tests/integration/repositories/test_checkpoint.py` (362 lines), and `tests/unit/readmodels/test_optimistic_locking.py` (356 lines) below 320 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
