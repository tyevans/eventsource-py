---
id: REFACTOR-tests-unit-application-migration-test_router
title: Refactor and Decompose Legacy File test_router.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_router: Refactor Legacy File test_router.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_router.py` contains 1398 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_router_store.py, test_router_mock.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_router/` with submodules:
- `test_router_store.py`: create_mock_store, mock_default_store, mock_dedicated_store, TestStoreNotFoundError, TestTenantStoreRouterInit, TestStoreRegistryManagement, TestTenantStoreResolution, TestEvent, sid, router, TestWritePausedError, TestDualWriteInterceptorManagement, TestWritePauseManagement, TestWaitIfPaused, TestAppendRouting, TestReadOperationsRouting, TestStreamOperationsRouting, TestHelperMethods, TestMigrationStateRouting, TestDualWriteStateWithoutInterceptor, TestRouterStructuralConformance, TestConcurrentOperations
- `test_router_mock.py`: mock_routing_repo, AsyncIteratorMock, async_generator_mock

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_router.py (1398 lines):
  Submodule 'test_router_store.py' (~1197 lines):
    - [function] create_mock_store (lines 62-89)
    - [function] mock_default_store (lines 93-95)
    - [function] mock_dedicated_store (lines 99-101)
    - [class] TestStoreNotFoundError (lines 174-187)
    - [class] TestTenantStoreRouterInit (lines 195-274)
    - [class] TestStoreRegistryManagement (lines 282-369)
    - [class] TestTenantStoreResolution (lines 1042-1157)
    - [class] TestEvent (lines 45-49)
    - [function] sid (lines 52-54)
    - [function] router (lines 113-122)
    - [class] TestWritePausedError (lines 154-171)
    - [class] TestDualWriteInterceptorManagement (lines 377-428)
    - [class] TestWritePauseManagement (lines 436-525)
    - [class] TestWaitIfPaused (lines 533-578)
    - [class] TestAppendRouting (lines 586-778)
    - [class] TestReadOperationsRouting (lines 786-924)
    - [class] TestStreamOperationsRouting (lines 932-1034)
    - [class] TestHelperMethods (lines 1165-1198)
    - [class] TestMigrationStateRouting (lines 1206-1286)
    - [class] TestDualWriteStateWithoutInterceptor (lines 1294-1324)
    - [class] TestRouterStructuralConformance (lines 1332-1354)
    - [class] TestConcurrentOperations (lines 1362-1398)
  Submodule 'test_router_mock.py' (~25 lines):
    - [function] mock_routing_repo (lines 105-109)
    - [class] AsyncIteratorMock (lines 125-140)
    - [function] async_generator_mock (lines 143-146)
  Suggested barrel exports:
    from .test_router_store import create_mock_store, mock_default_store, mock_dedicated_store, TestStoreNotFoundError, TestTenantStoreRouterInit, TestStoreRegistryManagement, TestTenantStoreResolution, TestEvent, sid, router, TestWritePausedError, TestDualWriteInterceptorManagement, TestWritePauseManagement, TestWaitIfPaused, TestAppendRouting, TestReadOperationsRouting, TestStreamOperationsRouting, TestHelperMethods, TestMigrationStateRouting, TestDualWriteStateWithoutInterceptor, TestRouterStructuralConformance, TestConcurrentOperations
    from .test_router_mock import mock_routing_repo, AsyncIteratorMock, async_generator_mock

    __all__ = ["create_mock_store", "mock_default_store", "mock_dedicated_store", "TestStoreNotFoundError", "TestTenantStoreRouterInit", "TestStoreRegistryManagement", "TestTenantStoreResolution", "TestEvent", "sid", "router", "TestWritePausedError", "TestDualWriteInterceptorManagement", "TestWritePauseManagement", "TestWaitIfPaused", "TestAppendRouting", "TestReadOperationsRouting", "TestStreamOperationsRouting", "TestHelperMethods", "TestMigrationStateRouting", "TestDualWriteStateWithoutInterceptor", "TestRouterStructuralConformance", "TestConcurrentOperations", "mock_routing_repo", "AsyncIteratorMock", "async_generator_mock"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
