---
id: TASK-0249
title: "Slim snapshot load, soft delete, phase3 repositories, error classification, and audit log query test modules below 310 lines"
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

# TASK-0249: Slim snapshot load, soft delete, phase3 repositories, error classification, and audit log query test modules below 310 lines

## Summary
Compact `tests/unit/application/aggregates/test_repository_snapshot/test_repository_snapshot_load.py`, `tests/unit/readmodels/test_soft_delete/test_soft_delete_queries.py`, `tests/unit/application/migration/test_phase3_integration/_repositories.py`, `tests/unit/application/migration/test_error_classification.py`, and `tests/unit/adapters/sql/migration/test_audit_log_repository/test_queries_and_counts.py` strictly below 310 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
