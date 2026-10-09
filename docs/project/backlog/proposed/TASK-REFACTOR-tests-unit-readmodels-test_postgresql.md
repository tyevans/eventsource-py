---
id: REFACTOR-tests-unit-readmodels-test_postgresql
title: Refactor and Decompose Legacy File test_postgresql.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-readmodels-test_postgresql: Refactor Legacy File test_postgresql.py

## Summary
The grandfathered debt file `tests/unit/readmodels/test_postgresql.py` contains 508 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_postgresql_model.py, test_postgresql_query.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/readmodels/test_postgresql/` with submodules:
- `test_postgresql_model.py`: CustomTableModel, TestPostgreSQLReadModelRepositoryConstruction, TestRowToModel, TestModelClassProperty, OrderSummary, TestFilterToSQL, TestAsyncOperations, TestTracingConfiguration, TestSQLSecurityAnnotations
- `test_postgresql_query.py`: TestBuildSelectQuery, TestBuildCountQuery, TestQueryBuilderIntegration

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/readmodels/test_postgresql.py (508 lines):
  Submodule 'test_postgresql_model.py' (~246 lines):
    - [class] CustomTableModel (lines 26-30)
    - [class] TestPostgreSQLReadModelRepositoryConstruction (lines 33-92)
    - [class] TestRowToModel (lines 304-341)
    - [class] TestModelClassProperty (lines 374-389)
    - [class] OrderSummary (lines 18-23)
    - [class] TestFilterToSQL (lines 95-145)
    - [class] TestAsyncOperations (lines 344-371)
    - [class] TestTracingConfiguration (lines 392-414)
    - [class] TestSQLSecurityAnnotations (lines 417-435)
  Submodule 'test_postgresql_query.py' (~223 lines):
    - [class] TestBuildSelectQuery (lines 148-265)
    - [class] TestBuildCountQuery (lines 268-301)
    - [class] TestQueryBuilderIntegration (lines 438-508)
  Suggested barrel exports:
    from .test_postgresql_model import CustomTableModel, TestPostgreSQLReadModelRepositoryConstruction, TestRowToModel, TestModelClassProperty, OrderSummary, TestFilterToSQL, TestAsyncOperations, TestTracingConfiguration, TestSQLSecurityAnnotations
    from .test_postgresql_query import TestBuildSelectQuery, TestBuildCountQuery, TestQueryBuilderIntegration

    __all__ = ["CustomTableModel", "TestPostgreSQLReadModelRepositoryConstruction", "TestRowToModel", "TestModelClassProperty", "OrderSummary", "TestFilterToSQL", "TestAsyncOperations", "TestTracingConfiguration", "TestSQLSecurityAnnotations", "TestBuildSelectQuery", "TestBuildCountQuery", "TestQueryBuilderIntegration"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
