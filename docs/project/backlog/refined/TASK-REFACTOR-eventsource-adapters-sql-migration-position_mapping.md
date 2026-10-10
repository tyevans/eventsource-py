---
id: REFACTOR-eventsource-adapters-sql-migration-position_mapping
title: Refactor and Decompose Legacy File position_mapping.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-sql-migration-position_mapping: Refactor Legacy File position_mapping.py

## Summary
The grandfathered debt file `src/eventsource/adapters/sql/migration/position_mapping.py` contains 815 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (position_mapping_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/sql/migration/position_mapping/` with submodules:
- `position_mapping_core.py`: PostgreSQLPositionMappingRepository

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/sql/migration/position_mapping.py (815 lines):
  Submodule 'position_mapping_core.py' (~743 lines):
    - [class] PostgreSQLPositionMappingRepository (lines 73-815)
  Suggested barrel exports:
    from .position_mapping_core import PostgreSQLPositionMappingRepository

    __all__ = ["PostgreSQLPositionMappingRepository"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
