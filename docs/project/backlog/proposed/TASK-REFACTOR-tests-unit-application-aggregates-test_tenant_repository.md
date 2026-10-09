---
id: REFACTOR-tests-unit-application-aggregates-test_tenant_repository
title: Refactor and Decompose Legacy File test_tenant_repository.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-aggregates-test_tenant_repository: Refactor Legacy File test_tenant_repository.py

## Summary
The grandfathered debt file `tests/unit/application/aggregates/test_tenant_repository.py` contains 758 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_tenant_repository_aware.py, test_tenant_repository_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/aggregates/test_tenant_repository/` with submodules:
- `test_tenant_repository_aware.py`: TestTenantAwareRepositorySave, TestTenantAwareRepositoryLoad, TestTenantAwareRepositoryExists, TestTenantAwareRepositoryLoadOrCreate, TestTenantAwareRepositoryCreateNew, TestTenantAwareRepositoryProperties, MockAggregate, TestTenantMismatchErrorDetails, TestReadsAreNotTenantIsolated
- `test_tenant_repository_order.py`: OrderCreated, NonTenantOrderCreated, TenantOrderState, TenantOrderAggregate

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/aggregates/test_tenant_repository.py (758 lines):
  Submodule 'test_tenant_repository_aware.py' (~661 lines):
    - [class] TestTenantAwareRepositorySave (lines 67-320)
    - [class] TestTenantAwareRepositoryLoad (lines 323-389)
    - [class] TestTenantAwareRepositoryExists (lines 392-457)
    - [class] TestTenantAwareRepositoryLoadOrCreate (lines 460-499)
    - [class] TestTenantAwareRepositoryCreateNew (lines 502-521)
    - [class] TestTenantAwareRepositoryProperties (lines 524-566)
    - [class] MockAggregate (lines 52-64)
    - [class] TestTenantMismatchErrorDetails (lines 569-658)
    - [class] TestReadsAreNotTenantIsolated (lines 691-758)
  Submodule 'test_tenant_repository_order.py' (~31 lines):
    - [class] OrderCreated (lines 37-41)
    - [class] NonTenantOrderCreated (lines 44-48)
    - [class] TenantOrderState (lines 666-670)
    - [class] TenantOrderAggregate (lines 673-688)
  Suggested barrel exports:
    from .test_tenant_repository_aware import TestTenantAwareRepositorySave, TestTenantAwareRepositoryLoad, TestTenantAwareRepositoryExists, TestTenantAwareRepositoryLoadOrCreate, TestTenantAwareRepositoryCreateNew, TestTenantAwareRepositoryProperties, MockAggregate, TestTenantMismatchErrorDetails, TestReadsAreNotTenantIsolated
    from .test_tenant_repository_order import OrderCreated, NonTenantOrderCreated, TenantOrderState, TenantOrderAggregate

    __all__ = ["TestTenantAwareRepositorySave", "TestTenantAwareRepositoryLoad", "TestTenantAwareRepositoryExists", "TestTenantAwareRepositoryLoadOrCreate", "TestTenantAwareRepositoryCreateNew", "TestTenantAwareRepositoryProperties", "MockAggregate", "TestTenantMismatchErrorDetails", "TestReadsAreNotTenantIsolated", "OrderCreated", "NonTenantOrderCreated", "TenantOrderState", "TenantOrderAggregate"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
