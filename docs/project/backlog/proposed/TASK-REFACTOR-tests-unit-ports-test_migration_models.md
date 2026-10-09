---
id: REFACTOR-tests-unit-ports-test_migration_models
title: Refactor and Decompose Legacy File test_migration_models.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-ports-test_migration_models: Refactor Legacy File test_migration_models.py

## Summary
The grandfathered debt file `tests/unit/ports/test_migration_models.py` contains 1085 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_migration_models_tenant.py, test_migration_models_result.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/ports/test_migration_models/` with submodules:
- `test_migration_models_tenant.py`: TestTenantMigrationState, TestTenantRouting, TestMigrationPhase, TestMigrationConfig, TestMigration, TestPositionMapping, TestSyncLag, TestMigrationStatus, TestMigrationAuditEntry
- `test_migration_models_result.py`: TestCutoverResult, TestMigrationResult

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/ports/test_migration_models.py (1085 lines):
  Submodule 'test_migration_models_tenant.py' (~882 lines):
    - [class] TestTenantMigrationState (lines 140-213)
    - [class] TestTenantRouting (lines 518-581)
    - [class] TestMigrationPhase (lines 40-137)
    - [class] TestMigrationConfig (lines 216-323)
    - [class] TestMigration (lines 326-515)
    - [class] TestPositionMapping (lines 584-617)
    - [class] TestSyncLag (lines 620-715)
    - [class] TestMigrationStatus (lines 772-886)
    - [class] TestMigrationAuditEntry (lines 983-1085)
  Submodule 'test_migration_models_result.py' (~144 lines):
    - [class] TestCutoverResult (lines 718-769)
    - [class] TestMigrationResult (lines 889-980)
  Suggested barrel exports:
    from .test_migration_models_tenant import TestTenantMigrationState, TestTenantRouting, TestMigrationPhase, TestMigrationConfig, TestMigration, TestPositionMapping, TestSyncLag, TestMigrationStatus, TestMigrationAuditEntry
    from .test_migration_models_result import TestCutoverResult, TestMigrationResult

    __all__ = ["TestTenantMigrationState", "TestTenantRouting", "TestMigrationPhase", "TestMigrationConfig", "TestMigration", "TestPositionMapping", "TestSyncLag", "TestMigrationStatus", "TestMigrationAuditEntry", "TestCutoverResult", "TestMigrationResult"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
