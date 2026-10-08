---
id: REFACTOR-tests-unit-application-subscriptions-test_drain
title: Refactor and Decompose Legacy File test_drain.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_drain: Refactor Legacy File test_drain.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_drain.py` contains 744 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_drain_controller.py, test_drain_mock.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_drain/` with submodules:
- `test_drain_controller.py`: TestFlowControllerWaitForDrain, MockFlowController, TestFlowControllerDrainEdgeCases, TestDrainInFlightEvents, TestGracefulShutdownWithDrain, TestModuleImports
- `test_drain_mock.py`: MockCoordinator

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_drain.py (744 lines):
  Submodule 'test_drain_controller.py' (~674 lines):
    - [class] TestFlowControllerWaitForDrain (lines 25-251)
    - [class] MockFlowController (lines 259-275)
    - [class] TestFlowControllerDrainEdgeCases (lines 494-611)
    - [class] TestDrainInFlightEvents (lines 294-486)
    - [class] TestGracefulShutdownWithDrain (lines 619-713)
    - [class] TestModuleImports (lines 721-744)
  Submodule 'test_drain_mock.py' (~9 lines):
    - [class] MockCoordinator (lines 278-286)
  Suggested barrel exports:
    from .test_drain_controller import TestFlowControllerWaitForDrain, MockFlowController, TestFlowControllerDrainEdgeCases, TestDrainInFlightEvents, TestGracefulShutdownWithDrain, TestModuleImports
    from .test_drain_mock import MockCoordinator

    __all__ = ["TestFlowControllerWaitForDrain", "MockFlowController", "TestFlowControllerDrainEdgeCases", "TestDrainInFlightEvents", "TestGracefulShutdownWithDrain", "TestModuleImports", "MockCoordinator"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
