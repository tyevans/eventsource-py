---
id: TASK-0217
title: "Slim unit adapter test modules (sqlite outbox, memory dlq, sql lag, position lookups) below 340 lines"
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

# TASK-0217: Slim unit adapter test modules (sqlite outbox, memory dlq, sql lag, position lookups) below 340 lines

## Summary
Compact `tests/unit/adapters/test_sqlite_outbox/test_sqlite_outbox_standalone.py` (397 lines), `tests/unit/adapters/test_memory_dlq/test_memory.py` (397 lines), `tests/unit/adapters/test_memory_checkpoints/test_sql_lag.py` (397 lines), and `tests/unit/adapters/sql/migration/test_position_mapping_repository/test_lookups.py` (387 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
