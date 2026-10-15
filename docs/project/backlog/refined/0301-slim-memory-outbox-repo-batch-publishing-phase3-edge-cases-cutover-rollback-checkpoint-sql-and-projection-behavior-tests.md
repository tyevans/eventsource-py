---
id: TASK-0301
title: "Compact 6 test modules at 287-289 lines below 265 lines"
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

# TASK-0301: Compact 6 test modules at 287-289 lines below 265 lines

## Summary
Compact `tests/unit/adapters/test_memory_outbox/test_repository.py` (289 lines), `tests/integration/bus/test_rabbitmq/test_batch_publishing.py` (289 lines), `tests/unit/application/migration/test_phase3_integration/test_phase3_edge_cases.py` (288 lines), `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_rollback_and_pause.py` (288 lines), `tests/unit/adapters/test_checkpoint_position/test_sql.py` (288 lines), and `tests/unit/readmodels/test_projection/test_projection_behavior.py` (287 lines) strictly below 265 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
