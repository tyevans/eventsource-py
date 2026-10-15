---
id: TASK-0284
title: "Slim test_tenant_repository_save, rabbitmq/test_stats, and test_version_validation below 280 lines"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0284: Slim test_tenant_repository_save, rabbitmq/test_stats, and test_version_validation below 280 lines

## Summary
Compact `tests/unit/application/aggregates/test_tenant_repository/test_tenant_repository_save.py`, `tests/integration/bus/test_rabbitmq/test_stats.py`, and `tests/unit/domain/test_aggregate_root/test_version_validation.py` strictly below 280 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
