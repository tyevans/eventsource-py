---
id: REFACTOR-tests-unit-application-subscriptions-test_retry
title: Refactor and Decompose Legacy File test_retry.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_retry: Refactor Legacy File test_retry.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_retry.py` contains 889 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_retry_breaker.py, test_retry_creation.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_retry/` with submodules:
- `test_retry_breaker.py`: TestCircuitBreakerConfigCreation, TestCircuitBreakerCreation, TestCircuitBreakerState, TestCircuitBreakerExecution, TestCircuitBreakerReset, TestCircuitBreakerOpenError, TestRetryStats, TestRetryError, TestCalculateBackoff, TestIsRetryableException, TestRetryAsync, TestRetryableOperation, TestTransientExceptions, TestRetryIntegration, TestModuleImports
- `test_retry_creation.py`: TestRetryConfigCreation

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_retry.py (889 lines):
  Submodule 'test_retry_breaker.py' (~694 lines):
    - [class] TestCircuitBreakerConfigCreation (lines 415-452)
    - [class] TestCircuitBreakerCreation (lines 458-471)
    - [class] TestCircuitBreakerState (lines 474-570)
    - [class] TestCircuitBreakerExecution (lines 573-636)
    - [class] TestCircuitBreakerReset (lines 639-664)
    - [class] TestCircuitBreakerOpenError (lines 670-683)
    - [class] TestRetryStats (lines 133-166)
    - [class] TestRetryError (lines 172-189)
    - [class] TestCalculateBackoff (lines 195-263)
    - [class] TestIsRetryableException (lines 269-305)
    - [class] TestRetryAsync (lines 311-409)
    - [class] TestRetryableOperation (lines 689-735)
    - [class] TestTransientExceptions (lines 741-758)
    - [class] TestRetryIntegration (lines 764-820)
    - [class] TestModuleImports (lines 828-889)
  Submodule 'test_retry_creation.py' (~89 lines):
    - [class] TestRetryConfigCreation (lines 39-127)
  Suggested barrel exports:
    from .test_retry_breaker import TestCircuitBreakerConfigCreation, TestCircuitBreakerCreation, TestCircuitBreakerState, TestCircuitBreakerExecution, TestCircuitBreakerReset, TestCircuitBreakerOpenError, TestRetryStats, TestRetryError, TestCalculateBackoff, TestIsRetryableException, TestRetryAsync, TestRetryableOperation, TestTransientExceptions, TestRetryIntegration, TestModuleImports
    from .test_retry_creation import TestRetryConfigCreation

    __all__ = ["TestCircuitBreakerConfigCreation", "TestCircuitBreakerCreation", "TestCircuitBreakerState", "TestCircuitBreakerExecution", "TestCircuitBreakerReset", "TestCircuitBreakerOpenError", "TestRetryStats", "TestRetryError", "TestCalculateBackoff", "TestIsRetryableException", "TestRetryAsync", "TestRetryableOperation", "TestTransientExceptions", "TestRetryIntegration", "TestModuleImports", "TestRetryConfigCreation"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
