---
id: REFACTOR-tests-unit-readmodels-test_sqlite
title: Refactor and Decompose Legacy File test_sqlite.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-readmodels-test_sqlite: Refactor Legacy File test_sqlite.py

## Summary
The grandfathered debt file `tests/unit/readmodels/test_sqlite.py` contains 675 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_sqlite_model.py, test_sqlite_to.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/readmodels/test_sqlite/` with submodules:
- `test_sqlite_model.py`: CustomTableModel, TestSQLiteReadModelRepositoryConstruction, TestRowToModel, TestModelToValues, TestModelClassProperty, OrderSummary, TestBuildSelectQuery, TestBuildCountQuery, TestAsyncOperations, TestTracingConfiguration, TestSQLSecurityAnnotations, TestQueryBuilderIntegration, TestSQLiteSyntaxDifferences, TestExportFromModule
- `test_sqlite_to.py`: TestFilterToSQL

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/readmodels/test_sqlite.py (675 lines):
  Submodule 'test_sqlite_model.py' (~563 lines):
    - [class] CustomTableModel (lines 28-32)
    - [class] TestSQLiteReadModelRepositoryConstruction (lines 35-94)
    - [class] TestRowToModel (lines 313-397)
    - [class] TestModelToValues (lines 400-450)
    - [class] TestModelClassProperty (lines 483-498)
    - [class] OrderSummary (lines 20-25)
    - [class] TestBuildSelectQuery (lines 164-274)
    - [class] TestBuildCountQuery (lines 277-310)
    - [class] TestAsyncOperations (lines 453-480)
    - [class] TestTracingConfiguration (lines 501-523)
    - [class] TestSQLSecurityAnnotations (lines 526-544)
    - [class] TestQueryBuilderIntegration (lines 547-611)
    - [class] TestSQLiteSyntaxDifferences (lines 614-657)
    - [class] TestExportFromModule (lines 660-675)
  Submodule 'test_sqlite_to.py' (~65 lines):
    - [class] TestFilterToSQL (lines 97-161)
  Suggested barrel exports:
    from .test_sqlite_model import CustomTableModel, TestSQLiteReadModelRepositoryConstruction, TestRowToModel, TestModelToValues, TestModelClassProperty, OrderSummary, TestBuildSelectQuery, TestBuildCountQuery, TestAsyncOperations, TestTracingConfiguration, TestSQLSecurityAnnotations, TestQueryBuilderIntegration, TestSQLiteSyntaxDifferences, TestExportFromModule
    from .test_sqlite_to import TestFilterToSQL

    __all__ = ["CustomTableModel", "TestSQLiteReadModelRepositoryConstruction", "TestRowToModel", "TestModelToValues", "TestModelClassProperty", "OrderSummary", "TestBuildSelectQuery", "TestBuildCountQuery", "TestAsyncOperations", "TestTracingConfiguration", "TestSQLSecurityAnnotations", "TestQueryBuilderIntegration", "TestSQLiteSyntaxDifferences", "TestExportFromModule", "TestFilterToSQL"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
