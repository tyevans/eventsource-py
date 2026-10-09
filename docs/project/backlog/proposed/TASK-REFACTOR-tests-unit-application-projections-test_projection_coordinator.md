---
id: REFACTOR-tests-unit-application-projections-test_projection_coordinator
title: Refactor and Decompose Legacy File test_projection_coordinator.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-projections-test_projection_coordinator: Refactor Legacy File test_projection_coordinator.py

## Summary
The grandfathered debt file `tests/unit/application/projections/test_projection_coordinator.py` contains 729 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_projection_coordinator_order.py, test_projection_coordinator_registry.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/projections/test_projection_coordinator/` with submodules:
- `test_projection_coordinator_order.py`: OrderCreated, OrderShipped, TestProjectionCoordinator, TestConcurrentExecution
- `test_projection_coordinator_registry.py`: TestProjectionRegistry, TestSubscriberRegistry

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/projections/test_projection_coordinator.py (729 lines):
  Submodule 'test_projection_coordinator_order.py' (~300 lines):
    - [class] OrderCreated (lines 29-33)
    - [class] OrderShipped (lines 36-40)
    - [class] TestProjectionCoordinator (lines 276-456)
    - [class] TestConcurrentExecution (lines 621-729)
  Submodule 'test_projection_coordinator_registry.py' (~391 lines):
    - [class] TestProjectionRegistry (lines 43-273)
    - [class] TestSubscriberRegistry (lines 459-618)
  Suggested barrel exports:
    from .test_projection_coordinator_order import OrderCreated, OrderShipped, TestProjectionCoordinator, TestConcurrentExecution
    from .test_projection_coordinator_registry import TestProjectionRegistry, TestSubscriberRegistry

    __all__ = ["OrderCreated", "OrderShipped", "TestProjectionCoordinator", "TestConcurrentExecution", "TestProjectionRegistry", "TestSubscriberRegistry"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
