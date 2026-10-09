---
id: REFACTOR-tests-unit-application-subscriptions-test_shutdown
title: Refactor and Decompose Legacy File test_shutdown.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_shutdown: Refactor Legacy File test_shutdown.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_shutdown.py` contains 2720 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_shutdown_imports.py, test_shutdown_metrics.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_shutdown/` with submodules:
- `test_shutdown_imports.py`: TestModuleImports, TestMetricsModuleImports, TestPreShutdownHookImports, TestPostShutdownHookImports, TestShutdownPhaseEnum, TestShutdownReasonEnum, TestShutdownReasonTracking, TestShutdownResultNewFields, TestShutdownResult, TestShutdownCoordinatorCreation, TestShutdownCoordinatorProperties, TestRequestShutdown, TestShutdownCallbacks, TestShutdownExecution, TestTimeoutHandling, TestErrorHandling, TestSignalHandling, TestReset, TestWaitForShutdown, TestIntegration, TestPeriodicCheckpointDuringDrain, TestPreShutdownHooks, TestPostShutdownHooks, TestShutdownDeadline, TestShutdownDeadlineIntegration
- `test_shutdown_metrics.py`: TestShutdownMetricsSnapshot, TestShutdownMetricsFunctions, TestShutdownCoordinatorMetrics

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_shutdown.py (2720 lines):
  Submodule 'test_shutdown_imports.py' (~2258 lines):
    - [class] TestModuleImports (lines 1035-1068)
    - [class] TestMetricsModuleImports (lines 1633-1665)
    - [class] TestPreShutdownHookImports (lines 1977-1990)
    - [class] TestPostShutdownHookImports (lines 2325-2338)
    - [class] TestShutdownPhaseEnum (lines 40-76)
    - [class] TestShutdownReasonEnum (lines 82-103)
    - [class] TestShutdownReasonTracking (lines 109-218)
    - [class] TestShutdownResultNewFields (lines 224-304)
    - [class] TestShutdownResult (lines 310-394)
    - [class] TestShutdownCoordinatorCreation (lines 400-424)
    - [class] TestShutdownCoordinatorProperties (lines 430-446)
    - [class] TestRequestShutdown (lines 452-489)
    - [class] TestShutdownCallbacks (lines 495-519)
    - [class] TestShutdownExecution (lines 525-630)
    - [class] TestTimeoutHandling (lines 636-683)
    - [class] TestErrorHandling (lines 689-755)
    - [class] TestSignalHandling (lines 765-852)
    - [class] TestReset (lines 858-899)
    - [class] TestWaitForShutdown (lines 905-943)
    - [class] TestIntegration (lines 949-1029)
    - [class] TestPeriodicCheckpointDuringDrain (lines 1074-1325)
    - [class] TestPreShutdownHooks (lines 1671-1971)
    - [class] TestPostShutdownHooks (lines 1996-2319)
    - [class] TestShutdownDeadline (lines 2344-2624)
    - [class] TestShutdownDeadlineIntegration (lines 2627-2720)
  Submodule 'test_shutdown_metrics.py' (~287 lines):
    - [class] TestShutdownMetricsSnapshot (lines 1331-1381)
    - [class] TestShutdownMetricsFunctions (lines 1387-1429)
    - [class] TestShutdownCoordinatorMetrics (lines 1435-1627)
  Suggested barrel exports:
    from .test_shutdown_imports import TestModuleImports, TestMetricsModuleImports, TestPreShutdownHookImports, TestPostShutdownHookImports, TestShutdownPhaseEnum, TestShutdownReasonEnum, TestShutdownReasonTracking, TestShutdownResultNewFields, TestShutdownResult, TestShutdownCoordinatorCreation, TestShutdownCoordinatorProperties, TestRequestShutdown, TestShutdownCallbacks, TestShutdownExecution, TestTimeoutHandling, TestErrorHandling, TestSignalHandling, TestReset, TestWaitForShutdown, TestIntegration, TestPeriodicCheckpointDuringDrain, TestPreShutdownHooks, TestPostShutdownHooks, TestShutdownDeadline, TestShutdownDeadlineIntegration
    from .test_shutdown_metrics import TestShutdownMetricsSnapshot, TestShutdownMetricsFunctions, TestShutdownCoordinatorMetrics

    __all__ = ["TestModuleImports", "TestMetricsModuleImports", "TestPreShutdownHookImports", "TestPostShutdownHookImports", "TestShutdownPhaseEnum", "TestShutdownReasonEnum", "TestShutdownReasonTracking", "TestShutdownResultNewFields", "TestShutdownResult", "TestShutdownCoordinatorCreation", "TestShutdownCoordinatorProperties", "TestRequestShutdown", "TestShutdownCallbacks", "TestShutdownExecution", "TestTimeoutHandling", "TestErrorHandling", "TestSignalHandling", "TestReset", "TestWaitForShutdown", "TestIntegration", "TestPeriodicCheckpointDuringDrain", "TestPreShutdownHooks", "TestPostShutdownHooks", "TestShutdownDeadline", "TestShutdownDeadlineIntegration", "TestShutdownMetricsSnapshot", "TestShutdownMetricsFunctions", "TestShutdownCoordinatorMetrics"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
