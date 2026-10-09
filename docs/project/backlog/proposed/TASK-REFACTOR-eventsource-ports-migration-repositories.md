---
id: REFACTOR-eventsource-ports-migration-repositories
title: Refactor and Decompose Legacy File repositories.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-ports-migration-repositories: Refactor Legacy File repositories.py

## Summary
The grandfathered debt file `src/eventsource/ports/migration/repositories.py` contains 611 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (repositories_migration.py, repositories_tenant.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/ports/migration/repositories/` with submodules:
- `repositories_migration.py`: MigrationRepository, MigrationAuditLogRepository, PositionMappingRepository
- `repositories_tenant.py`: TenantRoutingRepository

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/ports/migration/repositories.py (611 lines):
  Submodule 'repositories_migration.py' (~442 lines):
    - [class] MigrationRepository (lines 32-168)
    - [class] MigrationAuditLogRepository (lines 511-603)
    - [class] PositionMappingRepository (lines 296-507)
  Submodule 'repositories_tenant.py' (~121 lines):
    - [class] TenantRoutingRepository (lines 172-292)
  Suggested barrel exports:
    from .repositories_migration import MigrationRepository, MigrationAuditLogRepository, PositionMappingRepository
    from .repositories_tenant import TenantRoutingRepository

    __all__ = ["MigrationRepository", "MigrationAuditLogRepository", "PositionMappingRepository", "TenantRoutingRepository"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
