---
id: REFACTOR-tests-unit-adapters-sql-migration-test_audit_log_repository
title: Refactor and Decompose Legacy File test_audit_log_repository.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-sql-migration-test_audit_log_repository: Refactor Legacy File test_audit_log_repository.py

## Summary
The grandfathered debt file `tests/unit/adapters/sql/migration/test_audit_log_repository.py` contains 853 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_audit_log_repository_migration.py, test_audit_log_repository_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/sql/migration/test_audit_log_repository/` with submodules:
- `test_audit_log_repository_migration.py`: TestMigrationAuditLogRepositoryProtocol, TestPostgreSQLMigrationAuditLogRepositoryInit, TestPostgreSQLMigrationAuditLogRepositoryRecord, TestPostgreSQLMigrationAuditLogRepositoryGetByMigration, TestPostgreSQLMigrationAuditLogRepositoryGetById, TestPostgreSQLMigrationAuditLogRepositoryGetLatest, TestPostgreSQLMigrationAuditLogRepositoryCountByMigration, TestPostgreSQLMigrationAuditLogRepositoryHelpers, TestMigrationAuditEntryFactoryMethods
- `test_audit_log_repository_event.py`: TestAuditEventTypeEnum

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/sql/migration/test_audit_log_repository.py (853 lines):
  Submodule 'test_audit_log_repository_migration.py' (~759 lines):
    - [class] TestMigrationAuditLogRepositoryProtocol (lines 32-49)
    - [class] TestPostgreSQLMigrationAuditLogRepositoryInit (lines 52-71)
    - [class] TestPostgreSQLMigrationAuditLogRepositoryRecord (lines 74-179)
    - [class] TestPostgreSQLMigrationAuditLogRepositoryGetByMigration (lines 182-337)
    - [class] TestPostgreSQLMigrationAuditLogRepositoryGetById (lines 340-405)
    - [class] TestPostgreSQLMigrationAuditLogRepositoryGetLatest (lines 408-494)
    - [class] TestPostgreSQLMigrationAuditLogRepositoryCountByMigration (lines 497-570)
    - [class] TestPostgreSQLMigrationAuditLogRepositoryHelpers (lines 573-689)
    - [class] TestMigrationAuditEntryFactoryMethods (lines 692-806)
  Submodule 'test_audit_log_repository_event.py' (~45 lines):
    - [class] TestAuditEventTypeEnum (lines 809-853)
  Suggested barrel exports:
    from .test_audit_log_repository_migration import TestMigrationAuditLogRepositoryProtocol, TestPostgreSQLMigrationAuditLogRepositoryInit, TestPostgreSQLMigrationAuditLogRepositoryRecord, TestPostgreSQLMigrationAuditLogRepositoryGetByMigration, TestPostgreSQLMigrationAuditLogRepositoryGetById, TestPostgreSQLMigrationAuditLogRepositoryGetLatest, TestPostgreSQLMigrationAuditLogRepositoryCountByMigration, TestPostgreSQLMigrationAuditLogRepositoryHelpers, TestMigrationAuditEntryFactoryMethods
    from .test_audit_log_repository_event import TestAuditEventTypeEnum

    __all__ = ["TestMigrationAuditLogRepositoryProtocol", "TestPostgreSQLMigrationAuditLogRepositoryInit", "TestPostgreSQLMigrationAuditLogRepositoryRecord", "TestPostgreSQLMigrationAuditLogRepositoryGetByMigration", "TestPostgreSQLMigrationAuditLogRepositoryGetById", "TestPostgreSQLMigrationAuditLogRepositoryGetLatest", "TestPostgreSQLMigrationAuditLogRepositoryCountByMigration", "TestPostgreSQLMigrationAuditLogRepositoryHelpers", "TestMigrationAuditEntryFactoryMethods", "TestAuditEventTypeEnum"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
