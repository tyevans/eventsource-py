---
id: REFACTOR-eventsource-adapters-sql-readmodel_schema
title: Refactor and Decompose Legacy File readmodel_schema.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-sql-readmodel_schema: Refactor Legacy File readmodel_schema.py

## Summary
The grandfathered debt file `src/eventsource/adapters/sql/readmodel_schema.py` contains 560 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (readmodel_schema_generate.py, readmodel_schema_type.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/sql/readmodel_schema/` with submodules:
- `readmodel_schema_generate.py`: generate_schema, generate_indexes, generate_full_schema, generate_additive_migration, _generate_column, _is_optional, _format_default
- `readmodel_schema_type.py`: _extract_type, _get_custom_sql_type

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/sql/readmodel_schema.py (560 lines):
  Submodule 'readmodel_schema_generate.py' (~373 lines):
    - [function] generate_schema (lines 80-144)
    - [function] generate_indexes (lines 147-216)
    - [function] generate_full_schema (lines 219-257)
    - [function] generate_additive_migration (lines 260-348)
    - [function] _generate_column (lines 351-409)
    - [function] _is_optional (lines 452-473)
    - [function] _format_default (lines 476-504)
  Submodule 'readmodel_schema_type.py' (~82 lines):
    - [function] _extract_type (lines 412-449)
    - [function] _get_custom_sql_type (lines 507-550)
  Suggested barrel exports:
    from .readmodel_schema_generate import generate_schema, generate_indexes, generate_full_schema, generate_additive_migration, _generate_column, _is_optional, _format_default
    from .readmodel_schema_type import _extract_type, _get_custom_sql_type

    __all__ = ["generate_schema", "generate_indexes", "generate_full_schema", "generate_additive_migration", "_generate_column", "_is_optional", "_format_default", "_extract_type", "_get_custom_sql_type"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
