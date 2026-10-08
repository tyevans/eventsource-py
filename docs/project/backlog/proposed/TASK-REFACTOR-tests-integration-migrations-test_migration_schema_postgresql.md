---
id: REFACTOR-tests-integration-migrations-test_migration_schema_postgresql
title: Refactor and Decompose Legacy File test_migration_schema_postgresql.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-migrations-test_migration_schema_postgresql: Refactor Legacy File test_migration_schema_postgresql.py

## Summary
The grandfathered debt file `tests/integration/migrations/test_migration_schema_postgresql.py` contains 830 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_migration_schema_postgresql_table.py, test_migration_schema_postgresql_engine.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/migrations/test_migration_schema_postgresql/` with submodules:
- `test_migration_schema_postgresql_table.py`: TestTenantMigrationsTable, TestTenantRoutingTable, TestMigrationPositionMappingsTable, TestMigrationAuditLogTable, TestMigrationSchemaCreation
- `test_migration_schema_postgresql_engine.py`: migration_schema_engine

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/migrations/test_migration_schema_postgresql.py (830 lines):
  Submodule 'test_migration_schema_postgresql_table.py' (~763 lines):
    - [class] TestTenantMigrationsTable (lines 121-313)
    - [class] TestTenantRoutingTable (lines 316-464)
    - [class] TestMigrationPositionMappingsTable (lines 467-629)
    - [class] TestMigrationAuditLogTable (lines 632-830)
    - [class] TestMigrationSchemaCreation (lines 60-118)
  Submodule 'test_migration_schema_postgresql_engine.py' (~27 lines):
    - [function] migration_schema_engine (lines 31-57)
  Suggested barrel exports:
    from .test_migration_schema_postgresql_table import TestTenantMigrationsTable, TestTenantRoutingTable, TestMigrationPositionMappingsTable, TestMigrationAuditLogTable, TestMigrationSchemaCreation
    from .test_migration_schema_postgresql_engine import migration_schema_engine

    __all__ = ["TestTenantMigrationsTable", "TestTenantRoutingTable", "TestMigrationPositionMappingsTable", "TestMigrationAuditLogTable", "TestMigrationSchemaCreation", "migration_schema_engine"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
