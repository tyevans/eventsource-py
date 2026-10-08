---
id: REFACTOR-tests-integration-projections-test_database_projection
title: Refactor and Decompose Legacy File test_database_projection.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-projections-test_database_projection: Refactor Legacy File test_database_projection.py

## Summary
The grandfathered debt file `tests/integration/projections/test_database_projection.py` contains 583 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_database_projection_order.py, test_database_projection_table.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/projections/test_database_projection/` with submodules:
- `test_database_projection_order.py`: OrderCreated, OrderShipped, OrderCancelled, TestDatabaseProjectionIntegration
- `test_database_projection_table.py`: orders_table

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/projections/test_database_projection.py (583 lines):
  Submodule 'test_database_projection_order.py' (~512 lines):
    - [class] OrderCreated (lines 35-40)
    - [class] OrderShipped (lines 43-47)
    - [class] OrderCancelled (lines 50-54)
    - [class] TestDatabaseProjectionIntegration (lines 88-583)
  Submodule 'test_database_projection_table.py' (~10 lines):
    - [function] orders_table (lines 73-82)
  Suggested barrel exports:
    from .test_database_projection_order import OrderCreated, OrderShipped, OrderCancelled, TestDatabaseProjectionIntegration
    from .test_database_projection_table import orders_table

    __all__ = ["OrderCreated", "OrderShipped", "OrderCancelled", "TestDatabaseProjectionIntegration", "orders_table"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
