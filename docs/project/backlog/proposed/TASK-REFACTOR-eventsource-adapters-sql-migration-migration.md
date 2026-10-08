---
id: REFACTOR-eventsource-adapters-sql-migration-migration
title: Refactor and Decompose Legacy File migration.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-sql-migration-migration: Refactor Legacy File migration.py

## Summary
The grandfathered debt file `src/eventsource/adapters/sql/migration/migration.py` contains 705 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (migration_token.py, migration_position.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/sql/migration/migration/` with submodules:
- `migration_token.py`: _token, PostgreSQLMigrationRepository
- `migration_position.py`: _position

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/sql/migration/migration.py (705 lines):
  Submodule 'migration_token.py' (~602 lines):
    - [function] _token (lines 70-72)
    - [class] PostgreSQLMigrationRepository (lines 107-705)
  Submodule 'migration_position.py' (~3 lines):
    - [function] _position (lines 75-77)
  Suggested barrel exports:
    from .migration_token import _token, PostgreSQLMigrationRepository
    from .migration_position import _position

    __all__ = ["_token", "PostgreSQLMigrationRepository", "_position"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
