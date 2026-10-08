---
id: REFACTOR-tests-integration-repositories-test_dlq
title: Refactor and Decompose Legacy File test_dlq.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-repositories-test_dlq: Refactor Legacy File test_dlq.py

## Summary
The grandfathered debt file `tests/integration/repositories/test_dlq.py` contains 547 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_dlq_basics.py, test_dlq_retrieval.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/repositories/test_dlq/` with submodules:
- `test_dlq_basics.py`: TestSQLDLQRepositoryBasics, TestSQLDLQRepositoryStatusTransitions, TestSQLDLQRepositoryStatistics, TestSQLDLQRepositoryCleanup, TestSQLDLQRepositoryCrossDialectAgreement
- `test_dlq_retrieval.py`: TestSQLDLQRepositoryRetrieval

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/repositories/test_dlq.py (547 lines):
  Submodule 'test_dlq_basics.py' (~392 lines):
    - [class] TestSQLDLQRepositoryBasics (lines 35-129)
    - [class] TestSQLDLQRepositoryStatusTransitions (lines 245-303)
    - [class] TestSQLDLQRepositoryStatistics (lines 306-384)
    - [class] TestSQLDLQRepositoryCleanup (lines 387-463)
    - [class] TestSQLDLQRepositoryCrossDialectAgreement (lines 466-547)
  Submodule 'test_dlq_retrieval.py' (~111 lines):
    - [class] TestSQLDLQRepositoryRetrieval (lines 132-242)
  Suggested barrel exports:
    from .test_dlq_basics import TestSQLDLQRepositoryBasics, TestSQLDLQRepositoryStatusTransitions, TestSQLDLQRepositoryStatistics, TestSQLDLQRepositoryCleanup, TestSQLDLQRepositoryCrossDialectAgreement
    from .test_dlq_retrieval import TestSQLDLQRepositoryRetrieval

    __all__ = ["TestSQLDLQRepositoryBasics", "TestSQLDLQRepositoryStatusTransitions", "TestSQLDLQRepositoryStatistics", "TestSQLDLQRepositoryCleanup", "TestSQLDLQRepositoryCrossDialectAgreement", "TestSQLDLQRepositoryRetrieval"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
