---
id: REFACTOR-tests-unit-application-migration-test_coordinator
title: Refactor and Decompose Legacy File test_coordinator.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_coordinator: Refactor Legacy File test_coordinator.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_coordinator.py` contains 1097 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_coordinator_migration.py, test_coordinator_status.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_coordinator/` with submodules:
- `test_coordinator_migration.py`: TestMigrationCoordinatorInit, TestStartMigration, TestPauseMigration, TestResumeMigration, TestAbortMigration, TestGetMigration, TestGetMigrationForTenant, TestFailMigration, TestEvent, TestListActiveMigrations, TestWaitForPhase, TestCalculateDuration
- `test_coordinator_status.py`: TestGetStatus, TestBuildStatus, TestStatusQueueManagement

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/migration/test_coordinator.py (1097 lines):
  Submodule 'test_coordinator_migration.py' (~836 lines):
    - [class] TestMigrationCoordinatorInit (lines 51-101)
    - [class] TestStartMigration (lines 104-277)
    - [class] TestPauseMigration (lines 343-440)
    - [class] TestResumeMigration (lines 443-507)
    - [class] TestAbortMigration (lines 510-596)
    - [class] TestGetMigration (lines 780-826)
    - [class] TestGetMigrationForTenant (lines 829-857)
    - [class] TestFailMigration (lines 1061-1097)
    - [class] TestEvent (lines 44-48)
    - [class] TestListActiveMigrations (lines 599-656)
    - [class] TestWaitForPhase (lines 659-777)
    - [class] TestCalculateDuration (lines 993-1058)
  Submodule 'test_coordinator_status.py' (~190 lines):
    - [class] TestGetStatus (lines 280-340)
    - [class] TestBuildStatus (lines 860-911)
    - [class] TestStatusQueueManagement (lines 914-990)
  Suggested barrel exports:
    from .test_coordinator_migration import TestMigrationCoordinatorInit, TestStartMigration, TestPauseMigration, TestResumeMigration, TestAbortMigration, TestGetMigration, TestGetMigrationForTenant, TestFailMigration, TestEvent, TestListActiveMigrations, TestWaitForPhase, TestCalculateDuration
    from .test_coordinator_status import TestGetStatus, TestBuildStatus, TestStatusQueueManagement

    __all__ = ["TestMigrationCoordinatorInit", "TestStartMigration", "TestPauseMigration", "TestResumeMigration", "TestAbortMigration", "TestGetMigration", "TestGetMigrationForTenant", "TestFailMigration", "TestEvent", "TestListActiveMigrations", "TestWaitForPhase", "TestCalculateDuration", "TestGetStatus", "TestBuildStatus", "TestStatusQueueManagement"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
