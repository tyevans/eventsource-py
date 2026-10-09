---
id: REFACTOR-tests-unit-readmodels-test_projection
title: Refactor and Decompose Legacy File test_projection.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-readmodels-test_projection: Refactor Legacy File test_projection.py

## Summary
The grandfathered debt file `tests/unit/readmodels/test_projection.py` contains 868 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_projection_order.py, test_projection_model.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/readmodels/test_projection/` with submodules:
- `test_projection_order.py`: OrderCreated, OrderShipped, OrderCancelled, OrderSummary, checkpoint_repo, dlq_repo, mock_postgresql_session_factory, mock_sqlite_session_factory, TestDialectDetection, TestTruncateReadModels, TestRuntimeErrors, TestCheckpointBehavior, TestRepositoryCleanup, TestUnregisteredEventHandling, TestTracingConfiguration, TestImports
- `test_projection_model.py`: CustomTableModel, TestReadModelProjectionConstruction, TestReadModelProjectionHandlerRouting

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/readmodels/test_projection.py (868 lines):
  Submodule 'test_projection_order.py' (~524 lines):
    - [class] OrderCreated (lines 33-37)
    - [class] OrderShipped (lines 40-44)
    - [class] OrderCancelled (lines 47-51)
    - [class] OrderSummary (lines 59-64)
    - [function] checkpoint_repo (lines 80-82)
    - [function] dlq_repo (lines 86-88)
    - [function] mock_postgresql_session_factory (lines 92-119)
    - [function] mock_sqlite_session_factory (lines 123-158)
    - [class] TestDialectDetection (lines 422-527)
    - [class] TestTruncateReadModels (lines 530-593)
    - [class] TestRuntimeErrors (lines 596-621)
    - [class] TestCheckpointBehavior (lines 624-683)
    - [class] TestRepositoryCleanup (lines 686-746)
    - [class] TestUnregisteredEventHandling (lines 749-813)
    - [class] TestTracingConfiguration (lines 816-850)
    - [class] TestImports (lines 853-868)
  Submodule 'test_projection_model.py' (~257 lines):
    - [class] CustomTableModel (lines 67-71)
    - [class] TestReadModelProjectionConstruction (lines 166-272)
    - [class] TestReadModelProjectionHandlerRouting (lines 275-419)
  Suggested barrel exports:
    from .test_projection_order import OrderCreated, OrderShipped, OrderCancelled, OrderSummary, checkpoint_repo, dlq_repo, mock_postgresql_session_factory, mock_sqlite_session_factory, TestDialectDetection, TestTruncateReadModels, TestRuntimeErrors, TestCheckpointBehavior, TestRepositoryCleanup, TestUnregisteredEventHandling, TestTracingConfiguration, TestImports
    from .test_projection_model import CustomTableModel, TestReadModelProjectionConstruction, TestReadModelProjectionHandlerRouting

    __all__ = ["OrderCreated", "OrderShipped", "OrderCancelled", "OrderSummary", "checkpoint_repo", "dlq_repo", "mock_postgresql_session_factory", "mock_sqlite_session_factory", "TestDialectDetection", "TestTruncateReadModels", "TestRuntimeErrors", "TestCheckpointBehavior", "TestRepositoryCleanup", "TestUnregisteredEventHandling", "TestTracingConfiguration", "TestImports", "CustomTableModel", "TestReadModelProjectionConstruction", "TestReadModelProjectionHandlerRouting"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
