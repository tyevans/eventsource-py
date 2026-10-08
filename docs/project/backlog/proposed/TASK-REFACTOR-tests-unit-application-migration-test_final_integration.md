---
id: REFACTOR-tests-unit-application-migration-test_final_integration
title: Refactor and Decompose Legacy File test_final_integration.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_final_integration: Refactor Legacy File test_final_integration.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_final_integration.py` contains 2378 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_final_integration_migration.py, test_final_integration_repository.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_final_integration/` with submodules:
- `test_final_integration_migration.py`: InMemoryMigrationRepository, migration_repo, migration_id, TestCompleteMigrationLifecycleWithOperationalFeatures, TestAuditLoggingThroughoutMigration, TestMetricsCollectionDuringMigration, TestStatusStreamingDuringMigration, src_pos, tgt_pos, SampleTestEvent, OrderCreated, OrderUpdated, MockLockInfo, MockLockManager, source_store, target_store, routing_repo, audit_log_repo, position_mapping_repo, checkpoint_repo, lock_manager, write_pause_manager, tenant_id, router, position_mapper, cleanup_metrics, create_test_events, copy_events_between_stores, TestErrorHandlingAndClassification, TestRecoveryAfterSimulatedFailures, TestIntegrationOfAllPhase4Components
- `test_final_integration_repository.py`: InMemoryRoutingRepository, InMemoryAuditLogRepository, InMemoryPositionMappingRepository, InMemoryCheckpointRepository

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/migration/test_final_integration.py (2378 lines):
  Submodule 'test_final_integration_migration.py' (~1823 lines):
    - [class] InMemoryMigrationRepository (lines 141-285)
    - [function] migration_repo (lines 699-701)
    - [function] migration_id (lines 747-749)
    - [class] TestCompleteMigrationLifecycleWithOperationalFeatures (lines 843-1032)
    - [class] TestAuditLoggingThroughoutMigration (lines 1040-1249)
    - [class] TestMetricsCollectionDuringMigration (lines 1257-1408)
    - [class] TestStatusStreamingDuringMigration (lines 1416-1573)
    - [function] src_pos (lines 93-95)
    - [function] tgt_pos (lines 98-100)
    - [class] SampleTestEvent (lines 112-116)
    - [class] OrderCreated (lines 120-125)
    - [class] OrderUpdated (lines 129-133)
    - [class] MockLockInfo (lines 616-620)
    - [class] MockLockManager (lines 623-678)
    - [function] source_store (lines 687-689)
    - [function] target_store (lines 693-695)
    - [function] routing_repo (lines 705-707)
    - [function] audit_log_repo (lines 711-713)
    - [function] position_mapping_repo (lines 717-719)
    - [function] checkpoint_repo (lines 723-725)
    - [function] lock_manager (lines 729-731)
    - [function] write_pause_manager (lines 735-737)
    - [function] tenant_id (lines 741-743)
    - [function] router (lines 753-766)
    - [function] position_mapper (lines 770-774)
    - [function] cleanup_metrics (lines 778-782)
    - [function] create_test_events (lines 790-816)
    - [function] copy_events_between_stores (lines 819-835)
    - [class] TestErrorHandlingAndClassification (lines 1581-1778)
    - [class] TestRecoveryAfterSimulatedFailures (lines 1786-2110)
    - [class] TestIntegrationOfAllPhase4Components (lines 2118-2378)
  Submodule 'test_final_integration_repository.py' (~319 lines):
    - [class] InMemoryRoutingRepository (lines 288-382)
    - [class] InMemoryAuditLogRepository (lines 385-473)
    - [class] InMemoryPositionMappingRepository (lines 476-562)
    - [class] InMemoryCheckpointRepository (lines 565-612)
  Suggested barrel exports:
    from .test_final_integration_migration import InMemoryMigrationRepository, migration_repo, migration_id, TestCompleteMigrationLifecycleWithOperationalFeatures, TestAuditLoggingThroughoutMigration, TestMetricsCollectionDuringMigration, TestStatusStreamingDuringMigration, src_pos, tgt_pos, SampleTestEvent, OrderCreated, OrderUpdated, MockLockInfo, MockLockManager, source_store, target_store, routing_repo, audit_log_repo, position_mapping_repo, checkpoint_repo, lock_manager, write_pause_manager, tenant_id, router, position_mapper, cleanup_metrics, create_test_events, copy_events_between_stores, TestErrorHandlingAndClassification, TestRecoveryAfterSimulatedFailures, TestIntegrationOfAllPhase4Components
    from .test_final_integration_repository import InMemoryRoutingRepository, InMemoryAuditLogRepository, InMemoryPositionMappingRepository, InMemoryCheckpointRepository

    __all__ = ["InMemoryMigrationRepository", "migration_repo", "migration_id", "TestCompleteMigrationLifecycleWithOperationalFeatures", "TestAuditLoggingThroughoutMigration", "TestMetricsCollectionDuringMigration", "TestStatusStreamingDuringMigration", "src_pos", "tgt_pos", "SampleTestEvent", "OrderCreated", "OrderUpdated", "MockLockInfo", "MockLockManager", "source_store", "target_store", "routing_repo", "audit_log_repo", "position_mapping_repo", "checkpoint_repo", "lock_manager", "write_pause_manager", "tenant_id", "router", "position_mapper", "cleanup_metrics", "create_test_events", "copy_events_between_stores", "TestErrorHandlingAndClassification", "TestRecoveryAfterSimulatedFailures", "TestIntegrationOfAllPhase4Components", "InMemoryRoutingRepository", "InMemoryAuditLogRepository", "InMemoryPositionMappingRepository", "InMemoryCheckpointRepository"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
