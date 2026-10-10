---
id: REFACTOR-eventsource-ports-migration-models
title: Refactor and Decompose Legacy File models.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-ports-migration-models: Refactor Legacy File models.py

## Summary
The grandfathered debt file `src/eventsource/ports/migration/models.py` contains 1175 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (models_migration.py, models_audit.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/ports/migration/models/` with submodules:
- `models_migration.py`: MigrationPhase, TenantMigrationState, MigrationConfig, Migration, MigrationStatus, MigrationResult, MigrationAuditEntry, TenantRouting, PositionMapping, SyncLag, CutoverResult
- `models_audit.py`: AuditEventType

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/ports/migration/models.py (1175 lines):
  Submodule 'models_migration.py' (~1016 lines):
    - [class] MigrationPhase (lines 43-179)
    - [class] TenantMigrationState (lines 254-357)
    - [class] MigrationConfig (lines 361-452)
    - [class] Migration (lines 456-616)
    - [class] MigrationStatus (lines 824-933)
    - [class] MigrationResult (lines 937-1004)
    - [class] MigrationAuditEntry (lines 1008-1159)
    - [class] TenantRouting (lines 620-683)
    - [class] PositionMapping (lines 687-707)
    - [class] SyncLag (lines 711-785)
    - [class] CutoverResult (lines 789-820)
  Submodule 'models_audit.py' (~70 lines):
    - [class] AuditEventType (lines 182-251)
  Suggested barrel exports:
    from .models_migration import MigrationPhase, TenantMigrationState, MigrationConfig, Migration, MigrationStatus, MigrationResult, MigrationAuditEntry, TenantRouting, PositionMapping, SyncLag, CutoverResult
    from .models_audit import AuditEventType

    __all__ = ["MigrationPhase", "TenantMigrationState", "MigrationConfig", "Migration", "MigrationStatus", "MigrationResult", "MigrationAuditEntry", "TenantRouting", "PositionMapping", "SyncLag", "CutoverResult", "AuditEventType"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
