---
id: REFACTOR-tests-unit-adapters-sql-migration-test_position_mapping_repository
title: Refactor and Decompose Legacy File test_position_mapping_repository.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-sql-migration-test_position_mapping_repository: Refactor Legacy File test_position_mapping_repository.py

## Summary
The grandfathered debt file `tests/unit/adapters/sql/migration/test_position_mapping_repository.py` contains 1217 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_position_mapping_repository_postgre.py, test_position_mapping_repository_protocol.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/sql/migration/test_position_mapping_repository/` with submodules:
- `test_position_mapping_repository_postgre.py`: TestPostgreSQLPositionMappingRepositoryInit, TestPostgreSQLPositionMappingRepositoryCreate, TestPostgreSQLPositionMappingRepositoryCreateBatch, TestPostgreSQLPositionMappingRepositoryGet, TestPostgreSQLPositionMappingRepositoryFindBySourcePosition, TestPostgreSQLPositionMappingRepositoryFindByTargetPosition, TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition, TestPostgreSQLPositionMappingRepositoryFindByEventId, TestPostgreSQLPositionMappingRepositoryListByMigration, TestPostgreSQLPositionMappingRepositoryListInSourceRange, TestPostgreSQLPositionMappingRepositoryCountByMigration, TestPostgreSQLPositionMappingRepositoryGetPositionBounds, TestPostgreSQLPositionMappingRepositoryDeleteByMigration, TestPostgreSQLPositionMappingRepositoryHelpers, pos, TestPositionMappingWorkflow
- `test_position_mapping_repository_protocol.py`: TestPositionMappingRepositoryProtocol

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/sql/migration/test_position_mapping_repository.py (1217 lines):
  Submodule 'test_position_mapping_repository_postgre.py' (~1130 lines):
    - [class] TestPostgreSQLPositionMappingRepositoryInit (lines 63-82)
    - [class] TestPostgreSQLPositionMappingRepositoryCreate (lines 85-159)
    - [class] TestPostgreSQLPositionMappingRepositoryCreateBatch (lines 162-258)
    - [class] TestPostgreSQLPositionMappingRepositoryGet (lines 261-329)
    - [class] TestPostgreSQLPositionMappingRepositoryFindBySourcePosition (lines 332-422)
    - [class] TestPostgreSQLPositionMappingRepositoryFindByTargetPosition (lines 425-483)
    - [class] TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition (lines 486-641)
    - [class] TestPostgreSQLPositionMappingRepositoryFindByEventId (lines 644-700)
    - [class] TestPostgreSQLPositionMappingRepositoryListByMigration (lines 703-792)
    - [class] TestPostgreSQLPositionMappingRepositoryListInSourceRange (lines 795-861)
    - [class] TestPostgreSQLPositionMappingRepositoryCountByMigration (lines 864-913)
    - [class] TestPostgreSQLPositionMappingRepositoryGetPositionBounds (lines 916-972)
    - [class] TestPostgreSQLPositionMappingRepositoryDeleteByMigration (lines 975-1024)
    - [class] TestPostgreSQLPositionMappingRepositoryHelpers (lines 1027-1110)
    - [function] pos (lines 30-32)
    - [class] TestPositionMappingWorkflow (lines 1113-1217)
  Submodule 'test_position_mapping_repository_protocol.py' (~26 lines):
    - [class] TestPositionMappingRepositoryProtocol (lines 35-60)
  Suggested barrel exports:
    from .test_position_mapping_repository_postgre import TestPostgreSQLPositionMappingRepositoryInit, TestPostgreSQLPositionMappingRepositoryCreate, TestPostgreSQLPositionMappingRepositoryCreateBatch, TestPostgreSQLPositionMappingRepositoryGet, TestPostgreSQLPositionMappingRepositoryFindBySourcePosition, TestPostgreSQLPositionMappingRepositoryFindByTargetPosition, TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition, TestPostgreSQLPositionMappingRepositoryFindByEventId, TestPostgreSQLPositionMappingRepositoryListByMigration, TestPostgreSQLPositionMappingRepositoryListInSourceRange, TestPostgreSQLPositionMappingRepositoryCountByMigration, TestPostgreSQLPositionMappingRepositoryGetPositionBounds, TestPostgreSQLPositionMappingRepositoryDeleteByMigration, TestPostgreSQLPositionMappingRepositoryHelpers, pos, TestPositionMappingWorkflow
    from .test_position_mapping_repository_protocol import TestPositionMappingRepositoryProtocol

    __all__ = ["TestPostgreSQLPositionMappingRepositoryInit", "TestPostgreSQLPositionMappingRepositoryCreate", "TestPostgreSQLPositionMappingRepositoryCreateBatch", "TestPostgreSQLPositionMappingRepositoryGet", "TestPostgreSQLPositionMappingRepositoryFindBySourcePosition", "TestPostgreSQLPositionMappingRepositoryFindByTargetPosition", "TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition", "TestPostgreSQLPositionMappingRepositoryFindByEventId", "TestPostgreSQLPositionMappingRepositoryListByMigration", "TestPostgreSQLPositionMappingRepositoryListInSourceRange", "TestPostgreSQLPositionMappingRepositoryCountByMigration", "TestPostgreSQLPositionMappingRepositoryGetPositionBounds", "TestPostgreSQLPositionMappingRepositoryDeleteByMigration", "TestPostgreSQLPositionMappingRepositoryHelpers", "pos", "TestPositionMappingWorkflow", "TestPositionMappingRepositoryProtocol"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
