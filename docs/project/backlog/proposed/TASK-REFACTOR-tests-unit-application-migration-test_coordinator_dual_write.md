---
id: REFACTOR-tests-unit-application-migration-test_coordinator_dual_write
title: Refactor and Decompose Legacy File test_coordinator_dual_write.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_coordinator_dual_write: Refactor Legacy File test_coordinator_dual_write.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_coordinator_dual_write.py` contains 1034 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_coordinator_dual_write_cutover.py, test_coordinator_dual_write_migration.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_coordinator_dual_write/` with submodules:
- `test_coordinator_dual_write_cutover.py`: TestTriggerCutover, TestIsCutoverReady, TestGetCutoverManager, TestRollbackCutover, TestCompleteCutover, TestTransitionToDualWrite, TestGetSyncLag, TestBuildStatusWithSyncLag
- `test_coordinator_dual_write_migration.py`: TestCleanupMigrationResources, TestAbortMigrationCleansUpP2Resources, TestFailMigrationCleansUpP2Resources, TestStartMigrationStoresTargetStore

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_coordinator_dual_write.py (1034 lines):
  Submodule 'test_coordinator_dual_write_cutover.py' (~797 lines):
    - [class] TestTriggerCutover (lines 156-353)
    - [class] TestIsCutoverReady (lines 449-561)
    - [class] TestGetCutoverManager (lines 764-816)
    - [class] TestRollbackCutover (lines 819-894)
    - [class] TestCompleteCutover (lines 897-982)
    - [class] TestTransitionToDualWrite (lines 43-153)
    - [class] TestGetSyncLag (lines 356-446)
    - [class] TestBuildStatusWithSyncLag (lines 564-632)
  Submodule 'test_coordinator_dual_write_migration.py' (~173 lines):
    - [class] TestCleanupMigrationResources (lines 635-672)
    - [class] TestAbortMigrationCleansUpP2Resources (lines 675-718)
    - [class] TestFailMigrationCleansUpP2Resources (lines 721-761)
    - [class] TestStartMigrationStoresTargetStore (lines 985-1034)
  Suggested barrel exports:
    from .test_coordinator_dual_write_cutover import TestTriggerCutover, TestIsCutoverReady, TestGetCutoverManager, TestRollbackCutover, TestCompleteCutover, TestTransitionToDualWrite, TestGetSyncLag, TestBuildStatusWithSyncLag
    from .test_coordinator_dual_write_migration import TestCleanupMigrationResources, TestAbortMigrationCleansUpP2Resources, TestFailMigrationCleansUpP2Resources, TestStartMigrationStoresTargetStore

    __all__ = ["TestTriggerCutover", "TestIsCutoverReady", "TestGetCutoverManager", "TestRollbackCutover", "TestCompleteCutover", "TestTransitionToDualWrite", "TestGetSyncLag", "TestBuildStatusWithSyncLag", "TestCleanupMigrationResources", "TestAbortMigrationCleansUpP2Resources", "TestFailMigrationCleansUpP2Resources", "TestStartMigrationStoresTargetStore"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
