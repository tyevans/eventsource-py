---
id: REFACTOR-tests-unit-application-projections-test_projection_base
title: Refactor and Decompose Legacy File test_projection_base.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-projections-test_projection_base: Refactor Legacy File test_projection_base.py

## Summary
The grandfathered debt file `tests/unit/application/projections/test_projection_base.py` contains 1537 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_projection_base_order.py, test_projection_base_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/projections/test_projection_base/` with submodules:
- `test_projection_base_order.py`: OrderCreated, OrderShipped, OrderCancelled, TestProjectionAbstract, TestSyncProjection, TestCheckpointTrackingProjection, TestDeclarativeProjection, TestDeclarativeProjectionWithDLQ, TestLagMetrics, TestDatabaseProjection
- `test_projection_base_event.py`: TestEventHandlerBase, TestProjectionUnregisteredEventHandling, TestDatabaseProjectionUnregisteredEventHandling

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/projections/test_projection_base.py (1537 lines):
  Submodule 'test_projection_base_order.py' (~1122 lines):
    - [class] OrderCreated (lines 33-37)
    - [class] OrderShipped (lines 40-44)
    - [class] OrderCancelled (lines 47-51)
    - [class] TestProjectionAbstract (lines 54-99)
    - [class] TestSyncProjection (lines 102-125)
    - [class] TestCheckpointTrackingProjection (lines 162-501)
    - [class] TestDeclarativeProjection (lines 504-672)
    - [class] TestDeclarativeProjectionWithDLQ (lines 675-714)
    - [class] TestLagMetrics (lines 717-765)
    - [class] TestDatabaseProjection (lines 768-1206)
  Submodule 'test_projection_base_event.py' (~354 lines):
    - [class] TestEventHandlerBase (lines 128-159)
    - [class] TestProjectionUnregisteredEventHandling (lines 1214-1435)
    - [class] TestDatabaseProjectionUnregisteredEventHandling (lines 1438-1537)
  Suggested barrel exports:
    from .test_projection_base_order import OrderCreated, OrderShipped, OrderCancelled, TestProjectionAbstract, TestSyncProjection, TestCheckpointTrackingProjection, TestDeclarativeProjection, TestDeclarativeProjectionWithDLQ, TestLagMetrics, TestDatabaseProjection
    from .test_projection_base_event import TestEventHandlerBase, TestProjectionUnregisteredEventHandling, TestDatabaseProjectionUnregisteredEventHandling

    __all__ = ["OrderCreated", "OrderShipped", "OrderCancelled", "TestProjectionAbstract", "TestSyncProjection", "TestCheckpointTrackingProjection", "TestDeclarativeProjection", "TestDeclarativeProjectionWithDLQ", "TestLagMetrics", "TestDatabaseProjection", "TestEventHandlerBase", "TestProjectionUnregisteredEventHandling", "TestDatabaseProjectionUnregisteredEventHandling"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
