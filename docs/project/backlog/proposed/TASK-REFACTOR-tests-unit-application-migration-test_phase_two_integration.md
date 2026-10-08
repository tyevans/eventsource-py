---
id: REFACTOR-tests-unit-application-migration-test_phase_two_integration
title: Refactor and Decompose Legacy File test_phase2_integration.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_phase2_integration: Refactor Legacy File test_phase2_integration.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_phase2_integration.py` contains 2579 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_phase2_integration_write.py, test_phase2_integration_migration.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_phase2_integration/` with submodules:
- `test_phase2_integration_write.py`: write_pause_manager, TestDualWriteBehavior, TestSyncLagAnchorOnWriteActiveTenant, TestWriteActiveTenantFirstPassCutover, TestWritePauseManager, SampleTestEvent, OrderCreated, OrderConfirmed, InMemoryRoutingRepository, MockLockInfo, MockLockManager, source_store, target_store, routing_repo, lock_manager, tenant_id, router, coordinator, create_test_events, get_all_tenant_events, TestSyncLagTracking, FlakyTarget, drive_dual_writes, seed_copied_prefix, GatedFeedStore, TestCatchUpRoundsCap, TestCutoverSuccess, TestCutoverFailureAndRollback, TestAbortDuringDifferentPhases, TestErrorHandlingAndRecovery, TestConcurrentOperations
- `test_phase2_integration_migration.py`: InMemoryMigrationRepository, migration_repo, TestFullMigrationLifecycle, TestPauseResumeDuringMigration

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_phase2_integration.py (2579 lines):
  Submodule 'test_phase2_integration_write.py' (~2025 lines):
    - [function] write_pause_manager (lines 461-463)
    - [class] TestDualWriteBehavior (lines 676-829)
    - [class] TestSyncLagAnchorOnWriteActiveTenant (lines 1083-1614)
    - [class] TestWriteActiveTenantFirstPassCutover (lines 1667-1754)
    - [class] TestWritePauseManager (lines 2522-2579)
    - [class] SampleTestEvent (lines 67-71)
    - [class] OrderCreated (lines 75-80)
    - [class] OrderConfirmed (lines 84-87)
    - [class] InMemoryRoutingRepository (lines 254-352)
    - [class] MockLockInfo (lines 356-360)
    - [class] MockLockManager (lines 363-422)
    - [function] source_store (lines 431-433)
    - [function] target_store (lines 437-439)
    - [function] routing_repo (lines 449-451)
    - [function] lock_manager (lines 455-457)
    - [function] tenant_id (lines 467-469)
    - [function] router (lines 473-486)
    - [function] coordinator (lines 490-506)
    - [function] create_test_events (lines 514-540)
    - [function] get_all_tenant_events (lines 543-552)
    - [class] TestSyncLagTracking (lines 837-1005)
    - [class] FlakyTarget (lines 1008-1043)
    - [function] drive_dual_writes (lines 1046-1059)
    - [function] seed_copied_prefix (lines 1062-1080)
    - [class] GatedFeedStore (lines 1617-1664)
    - [class] TestCatchUpRoundsCap (lines 1757-1853)
    - [class] TestCutoverSuccess (lines 1861-1935)
    - [class] TestCutoverFailureAndRollback (lines 1938-2070)
    - [class] TestAbortDuringDifferentPhases (lines 2078-2164)
    - [class] TestErrorHandlingAndRecovery (lines 2258-2365)
    - [class] TestConcurrentOperations (lines 2373-2514)
  Submodule 'test_phase2_integration_migration.py' (~348 lines):
    - [class] InMemoryMigrationRepository (lines 95-251)
    - [function] migration_repo (lines 443-445)
    - [class] TestFullMigrationLifecycle (lines 560-668)
    - [class] TestPauseResumeDuringMigration (lines 2172-2250)
  Suggested barrel exports:
    from .test_phase2_integration_write import write_pause_manager, TestDualWriteBehavior, TestSyncLagAnchorOnWriteActiveTenant, TestWriteActiveTenantFirstPassCutover, TestWritePauseManager, SampleTestEvent, OrderCreated, OrderConfirmed, InMemoryRoutingRepository, MockLockInfo, MockLockManager, source_store, target_store, routing_repo, lock_manager, tenant_id, router, coordinator, create_test_events, get_all_tenant_events, TestSyncLagTracking, FlakyTarget, drive_dual_writes, seed_copied_prefix, GatedFeedStore, TestCatchUpRoundsCap, TestCutoverSuccess, TestCutoverFailureAndRollback, TestAbortDuringDifferentPhases, TestErrorHandlingAndRecovery, TestConcurrentOperations
    from .test_phase2_integration_migration import InMemoryMigrationRepository, migration_repo, TestFullMigrationLifecycle, TestPauseResumeDuringMigration

    __all__ = ["write_pause_manager", "TestDualWriteBehavior", "TestSyncLagAnchorOnWriteActiveTenant", "TestWriteActiveTenantFirstPassCutover", "TestWritePauseManager", "SampleTestEvent", "OrderCreated", "OrderConfirmed", "InMemoryRoutingRepository", "MockLockInfo", "MockLockManager", "source_store", "target_store", "routing_repo", "lock_manager", "tenant_id", "router", "coordinator", "create_test_events", "get_all_tenant_events", "TestSyncLagTracking", "FlakyTarget", "drive_dual_writes", "seed_copied_prefix", "GatedFeedStore", "TestCatchUpRoundsCap", "TestCutoverSuccess", "TestCutoverFailureAndRollback", "TestAbortDuringDifferentPhases", "TestErrorHandlingAndRecovery", "TestConcurrentOperations", "InMemoryMigrationRepository", "migration_repo", "TestFullMigrationLifecycle", "TestPauseResumeDuringMigration"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
