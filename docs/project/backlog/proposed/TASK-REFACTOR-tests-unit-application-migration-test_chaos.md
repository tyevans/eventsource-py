---
id: REFACTOR-tests-unit-application-migration-test_chaos
title: Refactor and Decompose Legacy File test_chaos.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_chaos: Refactor Legacy File test_chaos.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_chaos.py` contains 1788 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_chaos_failure.py, test_chaos_store.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_chaos/` with submodules:
- `test_chaos_failure.py`: FailureMode, FailureConfig, InjectedFailureError, FailureInjectableStore, TestComprehensiveFailureScenarios, TestFailureInjectionInfrastructure, ChaosTestEvent, InMemoryMigrationRepository, InMemoryRoutingRepository, MockLockInfo, MockLockManager, migration_repo, routing_repo, lock_manager, write_pause_manager, tenant_id, router, create_test_events, count_tenant_events, TestNetworkPartitionSimulation, TestProcessCrashDuringDualWrite, TestLockContentionScenarios, TestTimeoutScenarios, TestRecoveryAfterFailures
- `test_chaos_store.py`: source_store, target_store, TestTargetStoreFailures

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_chaos.py (1788 lines):
  Submodule 'test_chaos_failure.py' (~1421 lines):
    - [class] FailureMode (lines 88-99)
    - [class] FailureConfig (lines 103-115)
    - [class] InjectedFailureError (lines 118-123)
    - [class] FailureInjectableStore (lines 126-315)
    - [class] TestComprehensiveFailureScenarios (lines 1569-1681)
    - [class] TestFailureInjectionInfrastructure (lines 1689-1788)
    - [class] ChaosTestEvent (lines 75-80)
    - [class] InMemoryMigrationRepository (lines 323-457)
    - [class] InMemoryRoutingRepository (lines 460-547)
    - [class] MockLockInfo (lines 551-555)
    - [class] MockLockManager (lines 558-633)
    - [function] migration_repo (lines 654-656)
    - [function] routing_repo (lines 660-662)
    - [function] lock_manager (lines 666-668)
    - [function] write_pause_manager (lines 672-674)
    - [function] tenant_id (lines 678-680)
    - [function] router (lines 684-697)
    - [function] create_test_events (lines 705-733)
    - [function] count_tenant_events (lines 736-747)
    - [class] TestNetworkPartitionSimulation (lines 755-863)
    - [class] TestProcessCrashDuringDualWrite (lines 871-970)
    - [class] TestLockContentionScenarios (lines 978-1144)
    - [class] TestTimeoutScenarios (lines 1324-1449)
    - [class] TestRecoveryAfterFailures (lines 1457-1561)
  Submodule 'test_chaos_store.py' (~171 lines):
    - [function] source_store (lines 642-644)
    - [function] target_store (lines 648-650)
    - [class] TestTargetStoreFailures (lines 1152-1316)
  Suggested barrel exports:
    from .test_chaos_failure import FailureMode, FailureConfig, InjectedFailureError, FailureInjectableStore, TestComprehensiveFailureScenarios, TestFailureInjectionInfrastructure, ChaosTestEvent, InMemoryMigrationRepository, InMemoryRoutingRepository, MockLockInfo, MockLockManager, migration_repo, routing_repo, lock_manager, write_pause_manager, tenant_id, router, create_test_events, count_tenant_events, TestNetworkPartitionSimulation, TestProcessCrashDuringDualWrite, TestLockContentionScenarios, TestTimeoutScenarios, TestRecoveryAfterFailures
    from .test_chaos_store import source_store, target_store, TestTargetStoreFailures

    __all__ = ["FailureMode", "FailureConfig", "InjectedFailureError", "FailureInjectableStore", "TestComprehensiveFailureScenarios", "TestFailureInjectionInfrastructure", "ChaosTestEvent", "InMemoryMigrationRepository", "InMemoryRoutingRepository", "MockLockInfo", "MockLockManager", "migration_repo", "routing_repo", "lock_manager", "write_pause_manager", "tenant_id", "router", "create_test_events", "count_tenant_events", "TestNetworkPartitionSimulation", "TestProcessCrashDuringDualWrite", "TestLockContentionScenarios", "TestTimeoutScenarios", "TestRecoveryAfterFailures", "source_store", "target_store", "TestTargetStoreFailures"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
