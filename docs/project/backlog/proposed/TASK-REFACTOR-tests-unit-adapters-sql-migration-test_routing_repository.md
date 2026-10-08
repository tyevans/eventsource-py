---
id: REFACTOR-tests-unit-adapters-sql-migration-test_routing_repository
title: Refactor and Decompose Legacy File test_routing_repository.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-sql-migration-test_routing_repository: Refactor Legacy File test_routing_repository.py

## Summary
The grandfathered debt file `tests/unit/adapters/sql/migration/test_routing_repository.py` contains 977 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_routing_repository_tenant.py, test_routing_repository_state.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/sql/migration/test_routing_repository/` with submodules:
- `test_routing_repository_tenant.py`: TestTenantRoutingRepositoryProtocol, TestPostgreSQLTenantRoutingRepositoryInit, TestPostgreSQLTenantRoutingRepositoryGetRouting, TestPostgreSQLTenantRoutingRepositoryGetOrDefault, TestPostgreSQLTenantRoutingRepositorySetRouting, TestPostgreSQLTenantRoutingRepositorySetMigrationState, TestPostgreSQLTenantRoutingRepositoryClearMigrationState, TestPostgreSQLTenantRoutingRepositoryListByState, TestPostgreSQLTenantRoutingRepositoryListByStore, TestPostgreSQLTenantRoutingRepositoryDeleteRouting, TestPostgreSQLTenantRoutingRepositoryCaching, TestPostgreSQLTenantRoutingRepositoryHelpers
- `test_routing_repository_state.py`: TestMigrationStateTransitionWorkflow

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/sql/migration/test_routing_repository.py (977 lines):
  Submodule 'test_routing_repository_tenant.py' (~827 lines):
    - [class] TestTenantRoutingRepositoryProtocol (lines 29-50)
    - [class] TestPostgreSQLTenantRoutingRepositoryInit (lines 53-91)
    - [class] TestPostgreSQLTenantRoutingRepositoryGetRouting (lines 94-159)
    - [class] TestPostgreSQLTenantRoutingRepositoryGetOrDefault (lines 162-246)
    - [class] TestPostgreSQLTenantRoutingRepositorySetRouting (lines 249-283)
    - [class] TestPostgreSQLTenantRoutingRepositorySetMigrationState (lines 286-377)
    - [class] TestPostgreSQLTenantRoutingRepositoryClearMigrationState (lines 380-413)
    - [class] TestPostgreSQLTenantRoutingRepositoryListByState (lines 416-476)
    - [class] TestPostgreSQLTenantRoutingRepositoryListByStore (lines 479-538)
    - [class] TestPostgreSQLTenantRoutingRepositoryDeleteRouting (lines 541-592)
    - [class] TestPostgreSQLTenantRoutingRepositoryCaching (lines 595-790)
    - [class] TestPostgreSQLTenantRoutingRepositoryHelpers (lines 793-877)
  Submodule 'test_routing_repository_state.py' (~98 lines):
    - [class] TestMigrationStateTransitionWorkflow (lines 880-977)
  Suggested barrel exports:
    from .test_routing_repository_tenant import TestTenantRoutingRepositoryProtocol, TestPostgreSQLTenantRoutingRepositoryInit, TestPostgreSQLTenantRoutingRepositoryGetRouting, TestPostgreSQLTenantRoutingRepositoryGetOrDefault, TestPostgreSQLTenantRoutingRepositorySetRouting, TestPostgreSQLTenantRoutingRepositorySetMigrationState, TestPostgreSQLTenantRoutingRepositoryClearMigrationState, TestPostgreSQLTenantRoutingRepositoryListByState, TestPostgreSQLTenantRoutingRepositoryListByStore, TestPostgreSQLTenantRoutingRepositoryDeleteRouting, TestPostgreSQLTenantRoutingRepositoryCaching, TestPostgreSQLTenantRoutingRepositoryHelpers
    from .test_routing_repository_state import TestMigrationStateTransitionWorkflow

    __all__ = ["TestTenantRoutingRepositoryProtocol", "TestPostgreSQLTenantRoutingRepositoryInit", "TestPostgreSQLTenantRoutingRepositoryGetRouting", "TestPostgreSQLTenantRoutingRepositoryGetOrDefault", "TestPostgreSQLTenantRoutingRepositorySetRouting", "TestPostgreSQLTenantRoutingRepositorySetMigrationState", "TestPostgreSQLTenantRoutingRepositoryClearMigrationState", "TestPostgreSQLTenantRoutingRepositoryListByState", "TestPostgreSQLTenantRoutingRepositoryListByStore", "TestPostgreSQLTenantRoutingRepositoryDeleteRouting", "TestPostgreSQLTenantRoutingRepositoryCaching", "TestPostgreSQLTenantRoutingRepositoryHelpers", "TestMigrationStateTransitionWorkflow"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
