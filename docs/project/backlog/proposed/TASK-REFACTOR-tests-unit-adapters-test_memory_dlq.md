---
id: REFACTOR-tests-unit-adapters-test_memory_dlq
title: Refactor and Decompose Legacy File test_memory_dlq.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_memory_dlq: Refactor Legacy File test_memory_dlq.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_memory_dlq.py` contains 1208 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_memory_dlq_repository.py, test_memory_dlq_entry.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_memory_dlq/` with submodules:
- `test_memory_dlq_repository.py`: TestInMemoryDLQRepository, TestDLQRepositoryProtocol, TestInMemoryDLQRepositoryConcurrency, TestSQLDLQRepository, TestSQLDLQRepositoryProtocol
- `test_memory_dlq_entry.py`: TestDLQEntryTypedReturns

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/test_memory_dlq.py (1208 lines):
  Submodule 'test_memory_dlq_repository.py' (~1088 lines):
    - [class] TestInMemoryDLQRepository (lines 21-397)
    - [class] TestDLQRepositoryProtocol (lines 400-406)
    - [class] TestInMemoryDLQRepositoryConcurrency (lines 409-608)
    - [class] TestSQLDLQRepository (lines 702-1185)
    - [class] TestSQLDLQRepositoryProtocol (lines 1189-1208)
  Submodule 'test_memory_dlq_entry.py' (~69 lines):
    - [class] TestDLQEntryTypedReturns (lines 616-684)
  Suggested barrel exports:
    from .test_memory_dlq_repository import TestInMemoryDLQRepository, TestDLQRepositoryProtocol, TestInMemoryDLQRepositoryConcurrency, TestSQLDLQRepository, TestSQLDLQRepositoryProtocol
    from .test_memory_dlq_entry import TestDLQEntryTypedReturns

    __all__ = ["TestInMemoryDLQRepository", "TestDLQRepositoryProtocol", "TestInMemoryDLQRepositoryConcurrency", "TestSQLDLQRepository", "TestSQLDLQRepositoryProtocol", "TestDLQEntryTypedReturns"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
