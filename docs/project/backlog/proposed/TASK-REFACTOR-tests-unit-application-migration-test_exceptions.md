---
id: REFACTOR-tests-unit-application-migration-test_exceptions
title: Refactor and Decompose Legacy File test_exceptions.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_exceptions: Refactor Legacy File test_exceptions.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_exceptions.py` contains 582 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_exceptions_error.py, test_exceptions_hierarchy.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_exceptions/` with submodules:
- `test_exceptions_error.py`: TestMigrationError, TestMigrationNotFoundError, TestMigrationAlreadyExistsError, TestMigrationStateError, TestInvalidPhaseTransitionError, TestCutoverError, TestCutoverTimeoutError, TestCutoverLagError, TestConsistencyError, TestBulkCopyError, TestDualWriteError, TestPositionMappingError, TestCircuitBreakerOpenError, TestExceptionClassification
- `test_exceptions_hierarchy.py`: TestExceptionHierarchy

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/migration/test_exceptions.py (582 lines):
  Submodule 'test_exceptions_error.py' (~475 lines):
    - [class] TestMigrationError (lines 39-95)
    - [class] TestMigrationNotFoundError (lines 98-113)
    - [class] TestMigrationAlreadyExistsError (lines 116-136)
    - [class] TestMigrationStateError (lines 139-167)
    - [class] TestInvalidPhaseTransitionError (lines 170-196)
    - [class] TestCutoverError (lines 199-222)
    - [class] TestCutoverTimeoutError (lines 225-249)
    - [class] TestCutoverLagError (lines 252-276)
    - [class] TestConsistencyError (lines 279-321)
    - [class] TestBulkCopyError (lines 324-348)
    - [class] TestDualWriteError (lines 351-372)
    - [class] TestPositionMappingError (lines 375-407)
    - [class] TestCircuitBreakerOpenError (lines 566-582)
    - [class] TestExceptionClassification (lines 453-563)
  Submodule 'test_exceptions_hierarchy.py' (~41 lines):
    - [class] TestExceptionHierarchy (lines 410-450)
  Suggested barrel exports:
    from .test_exceptions_error import TestMigrationError, TestMigrationNotFoundError, TestMigrationAlreadyExistsError, TestMigrationStateError, TestInvalidPhaseTransitionError, TestCutoverError, TestCutoverTimeoutError, TestCutoverLagError, TestConsistencyError, TestBulkCopyError, TestDualWriteError, TestPositionMappingError, TestCircuitBreakerOpenError, TestExceptionClassification
    from .test_exceptions_hierarchy import TestExceptionHierarchy

    __all__ = ["TestMigrationError", "TestMigrationNotFoundError", "TestMigrationAlreadyExistsError", "TestMigrationStateError", "TestInvalidPhaseTransitionError", "TestCutoverError", "TestCutoverTimeoutError", "TestCutoverLagError", "TestConsistencyError", "TestBulkCopyError", "TestDualWriteError", "TestPositionMappingError", "TestCircuitBreakerOpenError", "TestExceptionClassification", "TestExceptionHierarchy"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
