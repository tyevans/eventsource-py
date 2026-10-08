---
id: REFACTOR-tests-unit-application-migration-test_phase3_integration
title: Refactor and Decompose Legacy File test_phase3_integration.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_phase3_integration: Refactor Legacy File test_phase3_integration.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_phase3_integration.py` contains 2196 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_phase3_integration_position.py, test_phase3_integration_migration.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_phase3_integration/` with submodules:
- `test_phase3_integration_position.py`: InMemoryPositionMappingRepository, position_mapping_repo, position_mapper, copy_events_with_position_mapping, TestPositionMappingRecording, TestPositionTranslation, TestPositionMapperEdgeCases, source_pos, target_pos, SampleTestEvent, OrderCreated, OrderUpdated, InMemoryCheckpointRepository, InMemoryRoutingRepository, MockLockInfo, MockLockManager, source_store, target_store, routing_repo, checkpoint_repo, lock_manager, write_pause_manager, tenant_id, router, create_test_events, get_all_tenant_events, TestConsistencyVerificationLevels, TestConsistencyVerificationWithSampling, TestMissingMappingErrors, TestVerificationFailures, TestCoordinatorErrorHandling, TestVerificationReport
- `test_phase3_integration_migration.py`: InMemoryMigrationRepository, migration_repo, migration_id, TestSubscriptionCheckpointMigration, TestDryRunSubscriptionMigration, TestFullMigrationWithVerificationAndSubscriptions, TestMigrationSummary

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_phase3_integration.py (2196 lines):
  Submodule 'test_phase3_integration_position.py' (~1273 lines):
    - [class] InMemoryPositionMappingRepository (lines 105-227)
    - [function] position_mapping_repo (lines 641-643)
    - [function] position_mapper (lines 694-698)
    - [function] copy_events_with_position_mapping (lines 747-775)
    - [class] TestPositionMappingRecording (lines 783-867)
    - [class] TestPositionTranslation (lines 870-975)
    - [class] TestPositionMapperEdgeCases (lines 1916-2024)
    - [function] source_pos (lines 68-70)
    - [function] target_pos (lines 73-75)
    - [class] SampleTestEvent (lines 78-82)
    - [class] OrderCreated (lines 85-90)
    - [class] OrderUpdated (lines 93-97)
    - [class] InMemoryCheckpointRepository (lines 230-281)
    - [class] InMemoryRoutingRepository (lines 440-538)
    - [class] MockLockInfo (lines 542-546)
    - [class] MockLockManager (lines 549-608)
    - [function] source_store (lines 617-619)
    - [function] target_store (lines 623-625)
    - [function] routing_repo (lines 635-637)
    - [function] checkpoint_repo (lines 647-649)
    - [function] lock_manager (lines 653-655)
    - [function] write_pause_manager (lines 659-661)
    - [function] tenant_id (lines 665-667)
    - [function] router (lines 677-690)
    - [function] create_test_events (lines 706-732)
    - [function] get_all_tenant_events (lines 735-744)
    - [class] TestConsistencyVerificationLevels (lines 983-1111)
    - [class] TestConsistencyVerificationWithSampling (lines 1114-1170)
    - [class] TestMissingMappingErrors (lines 1652-1721)
    - [class] TestVerificationFailures (lines 1724-1792)
    - [class] TestCoordinatorErrorHandling (lines 1795-1908)
    - [class] TestVerificationReport (lines 2032-2095)
  Submodule 'test_phase3_integration_migration.py' (~712 lines):
    - [class] InMemoryMigrationRepository (lines 284-437)
    - [function] migration_repo (lines 629-631)
    - [function] migration_id (lines 671-673)
    - [class] TestSubscriptionCheckpointMigration (lines 1178-1349)
    - [class] TestDryRunSubscriptionMigration (lines 1352-1442)
    - [class] TestFullMigrationWithVerificationAndSubscriptions (lines 1450-1644)
    - [class] TestMigrationSummary (lines 2103-2196)
  Suggested barrel exports:
    from .test_phase3_integration_position import InMemoryPositionMappingRepository, position_mapping_repo, position_mapper, copy_events_with_position_mapping, TestPositionMappingRecording, TestPositionTranslation, TestPositionMapperEdgeCases, source_pos, target_pos, SampleTestEvent, OrderCreated, OrderUpdated, InMemoryCheckpointRepository, InMemoryRoutingRepository, MockLockInfo, MockLockManager, source_store, target_store, routing_repo, checkpoint_repo, lock_manager, write_pause_manager, tenant_id, router, create_test_events, get_all_tenant_events, TestConsistencyVerificationLevels, TestConsistencyVerificationWithSampling, TestMissingMappingErrors, TestVerificationFailures, TestCoordinatorErrorHandling, TestVerificationReport
    from .test_phase3_integration_migration import InMemoryMigrationRepository, migration_repo, migration_id, TestSubscriptionCheckpointMigration, TestDryRunSubscriptionMigration, TestFullMigrationWithVerificationAndSubscriptions, TestMigrationSummary

    __all__ = ["InMemoryPositionMappingRepository", "position_mapping_repo", "position_mapper", "copy_events_with_position_mapping", "TestPositionMappingRecording", "TestPositionTranslation", "TestPositionMapperEdgeCases", "source_pos", "target_pos", "SampleTestEvent", "OrderCreated", "OrderUpdated", "InMemoryCheckpointRepository", "InMemoryRoutingRepository", "MockLockInfo", "MockLockManager", "source_store", "target_store", "routing_repo", "checkpoint_repo", "lock_manager", "write_pause_manager", "tenant_id", "router", "create_test_events", "get_all_tenant_events", "TestConsistencyVerificationLevels", "TestConsistencyVerificationWithSampling", "TestMissingMappingErrors", "TestVerificationFailures", "TestCoordinatorErrorHandling", "TestVerificationReport", "InMemoryMigrationRepository", "migration_repo", "migration_id", "TestSubscriptionCheckpointMigration", "TestDryRunSubscriptionMigration", "TestFullMigrationWithVerificationAndSubscriptions", "TestMigrationSummary"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
