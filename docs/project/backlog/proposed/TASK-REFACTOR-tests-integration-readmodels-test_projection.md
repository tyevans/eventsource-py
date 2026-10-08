---
id: REFACTOR-tests-integration-readmodels-test_projection
title: Refactor and Decompose Legacy File test_projection.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-readmodels-test_projection: Refactor Legacy File test_projection.py

## Summary
The grandfathered debt file `tests/integration/readmodels/test_projection.py` contains 1236 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_projection_model.py, test_projection_sq.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/readmodels/test_projection/` with submodules:
- `test_projection_model.py`: TestSQLiteReadModelProjectionCRUD, TestSQLiteReadModelProjectionCheckpoint, TestSQLiteReadModelProjectionReset, TestSQLiteReadModelProjectionEventRouting, TestSQLiteReadModelProjectionErrorHandling, TestSQLiteReadModelProjectionWorkflow, TestPostgreSQLReadModelProjectionCRUD, TestPostgreSQLReadModelProjectionCheckpoint, TestPostgreSQLReadModelProjectionReset, TestPostgreSQLReadModelProjectionEventRouting, TestPostgreSQLReadModelProjectionErrorHandling, TestPostgreSQLReadModelProjectionWorkflow, TestReadModelProjectionProperties, OrderCreated, OrderShipped, OrderCancelled, OrderCompleted, OrderSummary, OrderProjection, MixedHandlerProjection, FailingProjection, postgresql_projection, postgresql_mixed_projection, postgresql_failing_projection, sqlite_projection, sqlite_mixed_projection, sqlite_failing_projection, TestPostgreSQLMixedHandlerProjection
- `test_projection_sq.py`: TestSQLiteMixedHandlerProjection

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/readmodels/test_projection.py (1236 lines):
  Submodule 'test_projection_model.py' (~986 lines):
    - [class] TestSQLiteReadModelProjectionCRUD (lines 371-458)
    - [class] TestSQLiteReadModelProjectionCheckpoint (lines 466-513)
    - [class] TestSQLiteReadModelProjectionReset (lines 521-577)
    - [class] TestSQLiteReadModelProjectionEventRouting (lines 585-646)
    - [class] TestSQLiteReadModelProjectionErrorHandling (lines 714-755)
    - [class] TestSQLiteReadModelProjectionWorkflow (lines 763-868)
    - [class] TestPostgreSQLReadModelProjectionCRUD (lines 878-965)
    - [class] TestPostgreSQLReadModelProjectionCheckpoint (lines 970-989)
    - [class] TestPostgreSQLReadModelProjectionReset (lines 994-1027)
    - [class] TestPostgreSQLReadModelProjectionEventRouting (lines 1032-1078)
    - [class] TestPostgreSQLReadModelProjectionErrorHandling (lines 1123-1144)
    - [class] TestPostgreSQLReadModelProjectionWorkflow (lines 1149-1202)
    - [class] TestReadModelProjectionProperties (lines 1210-1236)
    - [class] OrderCreated (lines 57-63)
    - [class] OrderShipped (lines 66-70)
    - [class] OrderCancelled (lines 73-77)
    - [class] OrderCompleted (lines 80-83)
    - [class] OrderSummary (lines 91-98)
    - [class] OrderProjection (lines 106-134)
    - [class] MixedHandlerProjection (lines 137-163)
    - [class] FailingProjection (lines 166-175)
    - [function] postgresql_projection (lines 184-208)
    - [function] postgresql_mixed_projection (lines 212-236)
    - [function] postgresql_failing_projection (lines 240-268)
    - [function] sqlite_projection (lines 277-302)
    - [function] sqlite_mixed_projection (lines 306-331)
    - [function] sqlite_failing_projection (lines 335-363)
    - [class] TestPostgreSQLMixedHandlerProjection (lines 1083-1118)
  Submodule 'test_projection_sq.py' (~53 lines):
    - [class] TestSQLiteMixedHandlerProjection (lines 654-706)
  Suggested barrel exports:
    from .test_projection_model import TestSQLiteReadModelProjectionCRUD, TestSQLiteReadModelProjectionCheckpoint, TestSQLiteReadModelProjectionReset, TestSQLiteReadModelProjectionEventRouting, TestSQLiteReadModelProjectionErrorHandling, TestSQLiteReadModelProjectionWorkflow, TestPostgreSQLReadModelProjectionCRUD, TestPostgreSQLReadModelProjectionCheckpoint, TestPostgreSQLReadModelProjectionReset, TestPostgreSQLReadModelProjectionEventRouting, TestPostgreSQLReadModelProjectionErrorHandling, TestPostgreSQLReadModelProjectionWorkflow, TestReadModelProjectionProperties, OrderCreated, OrderShipped, OrderCancelled, OrderCompleted, OrderSummary, OrderProjection, MixedHandlerProjection, FailingProjection, postgresql_projection, postgresql_mixed_projection, postgresql_failing_projection, sqlite_projection, sqlite_mixed_projection, sqlite_failing_projection, TestPostgreSQLMixedHandlerProjection
    from .test_projection_sq import TestSQLiteMixedHandlerProjection

    __all__ = ["TestSQLiteReadModelProjectionCRUD", "TestSQLiteReadModelProjectionCheckpoint", "TestSQLiteReadModelProjectionReset", "TestSQLiteReadModelProjectionEventRouting", "TestSQLiteReadModelProjectionErrorHandling", "TestSQLiteReadModelProjectionWorkflow", "TestPostgreSQLReadModelProjectionCRUD", "TestPostgreSQLReadModelProjectionCheckpoint", "TestPostgreSQLReadModelProjectionReset", "TestPostgreSQLReadModelProjectionEventRouting", "TestPostgreSQLReadModelProjectionErrorHandling", "TestPostgreSQLReadModelProjectionWorkflow", "TestReadModelProjectionProperties", "OrderCreated", "OrderShipped", "OrderCancelled", "OrderCompleted", "OrderSummary", "OrderProjection", "MixedHandlerProjection", "FailingProjection", "postgresql_projection", "postgresql_mixed_projection", "postgresql_failing_projection", "sqlite_projection", "sqlite_mixed_projection", "sqlite_failing_projection", "TestPostgreSQLMixedHandlerProjection", "TestSQLiteMixedHandlerProjection"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
