---
id: REFACTOR-tests-integration-readmodels-test_repositories
title: Refactor and Decompose Legacy File test_repositories.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-readmodels-test_repositories: Refactor Legacy File test_repositories.py

## Summary
The grandfathered debt file `tests/integration/readmodels/test_repositories.py` contains 914 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_repositories_delete.py, test_repositories_basic.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/readmodels/test_repositories/` with submodules:
- `test_repositories_delete.py`: TestReadModelRepositoryDelete, TestReadModelRepositorySoftDeleteVisibility, TestReadModelRepositoryFind, TestReadModelRepositoryFilterOperators, TestReadModelRepositoryOrdering, TestReadModelRepositoryPagination, TestReadModelRepositoryCount, TestReadModelRepositoryEdgeCases, TestPostgreSQLReadModelRepository, TestSQLiteReadModelRepository, TestInMemoryReadModelRepository
- `test_repositories_basic.py`: TestReadModelRepositoryBasicOperations

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/readmodels/test_repositories.py (914 lines):
  Submodule 'test_repositories_delete.py' (~684 lines):
    - [class] TestReadModelRepositoryDelete (lines 160-259)
    - [class] TestReadModelRepositorySoftDeleteVisibility (lines 563-630)
    - [class] TestReadModelRepositoryFind (lines 267-288)
    - [class] TestReadModelRepositoryFilterOperators (lines 296-427)
    - [class] TestReadModelRepositoryOrdering (lines 435-490)
    - [class] TestReadModelRepositoryPagination (lines 498-555)
    - [class] TestReadModelRepositoryCount (lines 638-672)
    - [class] TestReadModelRepositoryEdgeCases (lines 680-804)
    - [class] TestPostgreSQLReadModelRepository (lines 813-835)
    - [class] TestSQLiteReadModelRepository (lines 843-876)
    - [class] TestInMemoryReadModelRepository (lines 884-914)
  Submodule 'test_repositories_basic.py' (~108 lines):
    - [class] TestReadModelRepositoryBasicOperations (lines 45-152)
  Suggested barrel exports:
    from .test_repositories_delete import TestReadModelRepositoryDelete, TestReadModelRepositorySoftDeleteVisibility, TestReadModelRepositoryFind, TestReadModelRepositoryFilterOperators, TestReadModelRepositoryOrdering, TestReadModelRepositoryPagination, TestReadModelRepositoryCount, TestReadModelRepositoryEdgeCases, TestPostgreSQLReadModelRepository, TestSQLiteReadModelRepository, TestInMemoryReadModelRepository
    from .test_repositories_basic import TestReadModelRepositoryBasicOperations

    __all__ = ["TestReadModelRepositoryDelete", "TestReadModelRepositorySoftDeleteVisibility", "TestReadModelRepositoryFind", "TestReadModelRepositoryFilterOperators", "TestReadModelRepositoryOrdering", "TestReadModelRepositoryPagination", "TestReadModelRepositoryCount", "TestReadModelRepositoryEdgeCases", "TestPostgreSQLReadModelRepository", "TestSQLiteReadModelRepository", "TestInMemoryReadModelRepository", "TestReadModelRepositoryBasicOperations"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
