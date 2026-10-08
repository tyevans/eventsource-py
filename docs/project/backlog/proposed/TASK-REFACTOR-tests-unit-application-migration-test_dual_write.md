---
id: REFACTOR-tests-unit-application-migration-test_dual_write
title: Refactor and Decompose Legacy File test_dual_write.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_dual_write: Refactor Legacy File test_dual_write.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_dual_write.py` contains 951 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_dual_write_failure.py, test_dual_write_store.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_dual_write/` with submodules:
- `test_dual_write_failure.py`: TestFailureStats, TestAppendSourceFailure, TestAppendTargetFailure, TestFailureTracking, TestEvent, sid, async_generator_mock, tenant_id, interceptor, TestFailedWrite, TestDualWriteInterceptorInit, TestAppendSuccess, TestReadOperations, TestRouterIntegration, TestConcurrentOperations, TestEdgeCases
- `test_dual_write_store.py`: create_mock_store, source_store, target_store

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_dual_write.py (951 lines):
  Submodule 'test_dual_write_failure.py' (~781 lines):
    - [class] TestFailureStats (lines 173-212)
    - [class] TestAppendSourceFailure (lines 367-387)
    - [class] TestAppendTargetFailure (lines 395-522)
    - [class] TestFailureTracking (lines 530-632)
    - [class] TestEvent (lines 36-40)
    - [function] sid (lines 43-45)
    - [function] async_generator_mock (lines 84-87)
    - [function] tenant_id (lines 103-105)
    - [function] interceptor (lines 109-120)
    - [class] TestFailedWrite (lines 128-165)
    - [class] TestDualWriteInterceptorInit (lines 220-283)
    - [class] TestAppendSuccess (lines 291-359)
    - [class] TestReadOperations (lines 640-739)
    - [class] TestRouterIntegration (lines 747-788)
    - [class] TestConcurrentOperations (lines 796-874)
    - [class] TestEdgeCases (lines 882-951)
  Submodule 'test_dual_write_store.py' (~35 lines):
    - [function] create_mock_store (lines 53-81)
    - [function] source_store (lines 91-93)
    - [function] target_store (lines 97-99)
  Suggested barrel exports:
    from .test_dual_write_failure import TestFailureStats, TestAppendSourceFailure, TestAppendTargetFailure, TestFailureTracking, TestEvent, sid, async_generator_mock, tenant_id, interceptor, TestFailedWrite, TestDualWriteInterceptorInit, TestAppendSuccess, TestReadOperations, TestRouterIntegration, TestConcurrentOperations, TestEdgeCases
    from .test_dual_write_store import create_mock_store, source_store, target_store

    __all__ = ["TestFailureStats", "TestAppendSourceFailure", "TestAppendTargetFailure", "TestFailureTracking", "TestEvent", "sid", "async_generator_mock", "tenant_id", "interceptor", "TestFailedWrite", "TestDualWriteInterceptorInit", "TestAppendSuccess", "TestReadOperations", "TestRouterIntegration", "TestConcurrentOperations", "TestEdgeCases", "create_mock_store", "source_store", "target_store"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
