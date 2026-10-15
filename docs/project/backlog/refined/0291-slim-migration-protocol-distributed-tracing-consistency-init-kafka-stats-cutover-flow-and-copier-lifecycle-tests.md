---
id: TASK-0291
title: "Compact test modules at 296-298 lines below 275 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0006
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0291: Compact test modules at 296-298 lines below 275 lines

## Summary
Compact `tests/unit/adapters/sql/migration/test_migration_repository/test_protocol_and_lifecycle.py` (298 lines), `tests/unit/adapters/_bus/test_eventbus_tracing_patterns/test_distributed_buses_and_consistency.py` (298 lines), `tests/unit/application/migration/test_consistency_verifier/test_consistency_verifier_init_and_tenant.py` (297 lines), `tests/integration/bus/test_kafka/test_stats_edge_cases.py` (297 lines), `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_init_and_flow.py` (296 lines), and `tests/unit/application/migration/test_bulk_copier/test_bulk_copier_lifecycle.py` (296 lines) strictly below 275 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
