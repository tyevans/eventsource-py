---
id: REFACTOR-tests-unit-application-subscriptions-test_health
title: Refactor and Decompose Legacy File test_health.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_health: Refactor Legacy File test_health.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_health.py` contains 988 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_health_status.py, test_health_check.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_health/` with submodules:
- `test_health_status.py`: TestHealthStatus, TestReadinessStatus, TestLivenessStatus, TestHealthIndicator, TestSubscriptionHealthChecker, TestManagerHealthChecker, TestManagerHealth, TestSubscriptionHealth, TestModuleImports
- `test_health_check.py`: TestHealthCheckResult, TestHealthCheckConfig

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_health.py (988 lines):
  Submodule 'test_health_status.py' (~837 lines):
    - [class] TestHealthStatus (lines 33-42)
    - [class] TestReadinessStatus (lines 840-885)
    - [class] TestLivenessStatus (lines 888-933)
    - [class] TestHealthIndicator (lines 50-81)
    - [class] TestSubscriptionHealthChecker (lines 167-516)
    - [class] TestManagerHealthChecker (lines 524-663)
    - [class] TestManagerHealth (lines 671-759)
    - [class] TestSubscriptionHealth (lines 762-837)
    - [class] TestModuleImports (lines 941-988)
  Submodule 'test_health_check.py' (~64 lines):
    - [class] TestHealthCheckResult (lines 89-125)
    - [class] TestHealthCheckConfig (lines 133-159)
  Suggested barrel exports:
    from .test_health_status import TestHealthStatus, TestReadinessStatus, TestLivenessStatus, TestHealthIndicator, TestSubscriptionHealthChecker, TestManagerHealthChecker, TestManagerHealth, TestSubscriptionHealth, TestModuleImports
    from .test_health_check import TestHealthCheckResult, TestHealthCheckConfig

    __all__ = ["TestHealthStatus", "TestReadinessStatus", "TestLivenessStatus", "TestHealthIndicator", "TestSubscriptionHealthChecker", "TestManagerHealthChecker", "TestManagerHealth", "TestSubscriptionHealth", "TestModuleImports", "TestHealthCheckResult", "TestHealthCheckConfig"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
