---
id: REFACTOR-tests-unit-adapters-sql-migration-test_migration_repository
title: Refactor and Decompose Legacy File test_migration_repository.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-sql-migration-test_migration_repository: Refactor Legacy File test_migration_repository.py

## Summary
The grandfathered debt file `tests/unit/adapters/sql/migration/test_migration_repository.py` contains 1027 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_migration_repository_postgre.py, test_migration_repository_phase.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/sql/migration/test_migration_repository/` with submodules:
- `test_migration_repository_postgre.py`: TestPostgreSQLMigrationRepositoryInit, TestPostgreSQLMigrationRepositoryCreate, TestPostgreSQLMigrationRepositoryGet, TestPostgreSQLMigrationRepositoryGetByTenant, TestPostgreSQLMigrationRepositoryUpdatePhase, TestPostgreSQLMigrationRepositoryUpdateProgress, TestPostgreSQLMigrationRepositorySetEventsTotal, TestPostgreSQLMigrationRepositoryRecordError, TestPostgreSQLMigrationRepositorySetPaused, TestPostgreSQLMigrationRepositoryListActive, TestPostgreSQLMigrationRepositoryHelpers, TestValidTransitions, TestMigrationRepositoryProtocol
- `test_migration_repository_phase.py`: TestPhaseTransitionValidation

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/adapters/sql/migration/test_migration_repository.py (1027 lines):
  Submodule 'test_migration_repository_postgre.py' (~881 lines):
    - [class] TestPostgreSQLMigrationRepositoryInit (lines 109-128)
    - [class] TestPostgreSQLMigrationRepositoryCreate (lines 131-197)
    - [class] TestPostgreSQLMigrationRepositoryGet (lines 200-285)
    - [class] TestPostgreSQLMigrationRepositoryGetByTenant (lines 288-316)
    - [class] TestPostgreSQLMigrationRepositoryUpdatePhase (lines 319-391)
    - [class] TestPostgreSQLMigrationRepositoryUpdateProgress (lines 394-458)
    - [class] TestPostgreSQLMigrationRepositorySetEventsTotal (lines 461-492)
    - [class] TestPostgreSQLMigrationRepositoryRecordError (lines 495-547)
    - [class] TestPostgreSQLMigrationRepositorySetPaused (lines 550-602)
    - [class] TestPostgreSQLMigrationRepositoryListActive (lines 605-712)
    - [class] TestPostgreSQLMigrationRepositoryHelpers (lines 715-944)
    - [class] TestValidTransitions (lines 40-80)
    - [class] TestMigrationRepositoryProtocol (lines 83-106)
  Submodule 'test_migration_repository_phase.py' (~81 lines):
    - [class] TestPhaseTransitionValidation (lines 947-1027)
  Suggested barrel exports:
    from .test_migration_repository_postgre import TestPostgreSQLMigrationRepositoryInit, TestPostgreSQLMigrationRepositoryCreate, TestPostgreSQLMigrationRepositoryGet, TestPostgreSQLMigrationRepositoryGetByTenant, TestPostgreSQLMigrationRepositoryUpdatePhase, TestPostgreSQLMigrationRepositoryUpdateProgress, TestPostgreSQLMigrationRepositorySetEventsTotal, TestPostgreSQLMigrationRepositoryRecordError, TestPostgreSQLMigrationRepositorySetPaused, TestPostgreSQLMigrationRepositoryListActive, TestPostgreSQLMigrationRepositoryHelpers, TestValidTransitions, TestMigrationRepositoryProtocol
    from .test_migration_repository_phase import TestPhaseTransitionValidation

    __all__ = ["TestPostgreSQLMigrationRepositoryInit", "TestPostgreSQLMigrationRepositoryCreate", "TestPostgreSQLMigrationRepositoryGet", "TestPostgreSQLMigrationRepositoryGetByTenant", "TestPostgreSQLMigrationRepositoryUpdatePhase", "TestPostgreSQLMigrationRepositoryUpdateProgress", "TestPostgreSQLMigrationRepositorySetEventsTotal", "TestPostgreSQLMigrationRepositoryRecordError", "TestPostgreSQLMigrationRepositorySetPaused", "TestPostgreSQLMigrationRepositoryListActive", "TestPostgreSQLMigrationRepositoryHelpers", "TestValidTransitions", "TestMigrationRepositoryProtocol", "TestPhaseTransitionValidation"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
