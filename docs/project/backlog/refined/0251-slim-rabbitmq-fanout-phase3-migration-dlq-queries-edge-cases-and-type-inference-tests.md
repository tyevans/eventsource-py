---
id: TASK-0251
title: "Slim rabbitmq fanout, phase3 full migration, memory dlq sql queries, phase3 edge cases, and aggregate type inference tests below 310 lines"
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

# TASK-0251: Slim rabbitmq fanout, phase3 full migration, memory dlq sql queries, phase3 edge cases, and aggregate type inference tests below 310 lines

## Summary
Compact `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_exchanges_fanout.py`, `tests/unit/application/migration/test_phase3_integration/test_phase3_full_migration.py`, `tests/unit/adapters/test_memory_dlq/test_sql_queries.py`, `tests/unit/application/migration/test_phase3_integration/test_phase3_edge_cases.py`, and `tests/unit/application/aggregates/test_aggregate_type_inference.py` strictly below 310 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
