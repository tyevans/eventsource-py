---
id: REFACTOR-tests-unit-adapters-test_memory_checkpoints
title: Refactor and Decompose Legacy File test_memory_checkpoints.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_memory_checkpoints: Refactor Legacy File test_memory_checkpoints.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_memory_checkpoints.py` contains 936 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_memory_checkpoints_protocol.py, test_memory_checkpoints_sql.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_memory_checkpoints/` with submodules:
- `test_memory_checkpoints_protocol.py`: TestCheckpointRepositoryProtocol, TestSQLCheckpointRepositoryProtocol, TestInMemoryCheckpointRepository, TestInMemoryCheckpointRepositoryConcurrency
- `test_memory_checkpoints_sql.py`: TestSQLCheckpointRepository

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/test_memory_checkpoints.py (936 lines):
  Submodule 'test_memory_checkpoints_protocol.py' (~328 lines):
    - [class] TestCheckpointRepositoryProtocol (lines 214-221)
    - [class] TestSQLCheckpointRepositoryProtocol (lines 921-936)
    - [class] TestInMemoryCheckpointRepository (lines 22-211)
    - [class] TestInMemoryCheckpointRepositoryConcurrency (lines 224-337)
  Submodule 'test_memory_checkpoints_sql.py' (~574 lines):
    - [class] TestSQLCheckpointRepository (lines 345-918)
  Suggested barrel exports:
    from .test_memory_checkpoints_protocol import TestCheckpointRepositoryProtocol, TestSQLCheckpointRepositoryProtocol, TestInMemoryCheckpointRepository, TestInMemoryCheckpointRepositoryConcurrency
    from .test_memory_checkpoints_sql import TestSQLCheckpointRepository

    __all__ = ["TestCheckpointRepositoryProtocol", "TestSQLCheckpointRepositoryProtocol", "TestInMemoryCheckpointRepository", "TestInMemoryCheckpointRepositoryConcurrency", "TestSQLCheckpointRepository"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
