---
id: REFACTOR-tests-unit-readmodels-test_in_memory
title: Refactor and Decompose Legacy File test_in_memory.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-readmodels-test_in_memory: Refactor Legacy File test_in_memory.py

## Summary
The grandfathered debt file `tests/unit/readmodels/test_in_memory.py` contains 778 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_in_memory_order.py, test_in_memory_repo.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/readmodels/test_in_memory/` with submodules:
- `test_in_memory_order.py`: OrderSummary, TestInMemoryReadModelRepository
- `test_in_memory_repo.py`: repo

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/readmodels/test_in_memory.py (778 lines):
  Submodule 'test_in_memory_order.py' (~758 lines):
    - [class] OrderSummary (lines 13-18)
    - [class] TestInMemoryReadModelRepository (lines 27-778)
  Submodule 'test_in_memory_repo.py' (~3 lines):
    - [function] repo (lines 22-24)
  Suggested barrel exports:
    from .test_in_memory_order import OrderSummary, TestInMemoryReadModelRepository
    from .test_in_memory_repo import repo

    __all__ = ["OrderSummary", "TestInMemoryReadModelRepository", "repo"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
