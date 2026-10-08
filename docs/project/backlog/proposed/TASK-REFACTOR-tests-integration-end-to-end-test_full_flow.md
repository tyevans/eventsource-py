---
id: REFACTOR-tests-integration-end-to-end-test_full_flow
title: Refactor and Decompose Legacy File test_full_flow.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-e2e-test_full_flow: Refactor Legacy File test_full_flow.py

## Summary
The grandfathered debt file `tests/integration/e2e/test_full_flow.py` contains 540 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_full_flow_projection.py, test_full_flow_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/e2e/test_full_flow/` with submodules:
- `test_full_flow_projection.py`: TestOrderProjection, TestCommandToProjectionFlow, TestProjectionRebuilding, TestAggregateRehydration, TestOptimisticLocking, TestMultiAggregateScenarios, TestEventStreamOperations, TestOutboxIntegration
- `test_full_flow_order.py`: OrderSummary

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/e2e/test_full_flow.py (540 lines):
  Submodule 'test_full_flow_projection.py' (~453 lines):
    - [class] TestOrderProjection (lines 69-111)
    - [class] TestCommandToProjectionFlow (lines 119-241)
    - [class] TestProjectionRebuilding (lines 473-509)
    - [class] TestAggregateRehydration (lines 244-317)
    - [class] TestOptimisticLocking (lines 320-354)
    - [class] TestMultiAggregateScenarios (lines 357-398)
    - [class] TestEventStreamOperations (lines 401-470)
    - [class] TestOutboxIntegration (lines 512-540)
  Submodule 'test_full_flow_order.py' (~9 lines):
    - [class] OrderSummary (lines 58-66)
  Suggested barrel exports:
    from .test_full_flow_projection import TestOrderProjection, TestCommandToProjectionFlow, TestProjectionRebuilding, TestAggregateRehydration, TestOptimisticLocking, TestMultiAggregateScenarios, TestEventStreamOperations, TestOutboxIntegration
    from .test_full_flow_order import OrderSummary

    __all__ = ["TestOrderProjection", "TestCommandToProjectionFlow", "TestProjectionRebuilding", "TestAggregateRehydration", "TestOptimisticLocking", "TestMultiAggregateScenarios", "TestEventStreamOperations", "TestOutboxIntegration", "OrderSummary"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
