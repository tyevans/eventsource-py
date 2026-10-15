---
id: TASK-0250
title: "Slim subscription health, position mapping queries, migration helpers, chaos locks, and repository loading tests below 310 lines"
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

# TASK-0250: Slim subscription health, position mapping queries, migration helpers, chaos locks, and repository loading tests below 310 lines

## Summary
Compact `tests/unit/application/subscriptions/test_health/test_subscription_health.py`, `tests/unit/adapters/sql/migration/test_position_mapping_repository/test_queries.py`, `tests/unit/adapters/sql/migration/test_migration_repository/test_helpers_and_transitions.py`, `tests/unit/application/migration/test_chaos/test_locks_and_timeouts.py`, and `tests/unit/application/aggregates/test_repository/test_repository_loading.py` strictly below 310 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
