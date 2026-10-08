---
id: REFACTOR-tests-unit-observability-test_projection_tracing
title: Refactor and Decompose Legacy File test_projection_tracing.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-observability-test_projection_tracing: Refactor Legacy File test_projection_tracing.py

## Summary
The grandfathered debt file `tests/unit/observability/test_projection_tracing.py` contains 591 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_projection_tracing_order.py, test_projection_tracing_checkpoint.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/observability/test_projection_tracing/` with submodules:
- `test_projection_tracing_order.py`: OrderCreated, OrderShipped, TestDeclarativeProjectionTracing, TestDatabaseProjectionTracing, TestProjectionRegistryTracing, TestProjectionCoordinatorTracing, TestBackwardCompatibility
- `test_projection_tracing_checkpoint.py`: TestCheckpointTrackingProjectionTracing

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/observability/test_projection_tracing.py (591 lines):
  Submodule 'test_projection_tracing_order.py' (~341 lines):
    - [class] OrderCreated (lines 30-34)
    - [class] OrderShipped (lines 37-41)
    - [class] TestDeclarativeProjectionTracing (lines 253-312)
    - [class] TestDatabaseProjectionTracing (lines 315-358)
    - [class] TestProjectionRegistryTracing (lines 361-442)
    - [class] TestProjectionCoordinatorTracing (lines 445-502)
    - [class] TestBackwardCompatibility (lines 505-591)
  Submodule 'test_projection_tracing_checkpoint.py' (~207 lines):
    - [class] TestCheckpointTrackingProjectionTracing (lines 44-250)
  Suggested barrel exports:
    from .test_projection_tracing_order import OrderCreated, OrderShipped, TestDeclarativeProjectionTracing, TestDatabaseProjectionTracing, TestProjectionRegistryTracing, TestProjectionCoordinatorTracing, TestBackwardCompatibility
    from .test_projection_tracing_checkpoint import TestCheckpointTrackingProjectionTracing

    __all__ = ["OrderCreated", "OrderShipped", "TestDeclarativeProjectionTracing", "TestDatabaseProjectionTracing", "TestProjectionRegistryTracing", "TestProjectionCoordinatorTracing", "TestBackwardCompatibility", "TestCheckpointTrackingProjectionTracing"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
