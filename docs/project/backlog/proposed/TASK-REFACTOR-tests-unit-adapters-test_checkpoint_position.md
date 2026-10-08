---
id: REFACTOR-tests-unit-adapters-test_checkpoint_position
title: Refactor and Decompose Legacy File test_checkpoint_position.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_checkpoint_position: Refactor Legacy File test_checkpoint_position.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_checkpoint_position.py` contains 538 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_checkpoint_position_repository.py, test_checkpoint_position_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_checkpoint_position/` with submodules:
- `test_checkpoint_position_repository.py`: TestInMemoryCheckpointRepositoryPosition, TestInMemoryCheckpointRepositoryPositionConcurrency, TestSQLCheckpointRepositoryPosition
- `test_checkpoint_position_core.py`: pos

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/adapters/test_checkpoint_position.py (538 lines):
  Submodule 'test_checkpoint_position_repository.py' (~483 lines):
    - [class] TestInMemoryCheckpointRepositoryPosition (lines 34-214)
    - [class] TestInMemoryCheckpointRepositoryPositionConcurrency (lines 217-270)
    - [class] TestSQLCheckpointRepositoryPosition (lines 291-538)
  Submodule 'test_checkpoint_position_core.py' (~3 lines):
    - [function] pos (lines 29-31)
  Suggested barrel exports:
    from .test_checkpoint_position_repository import TestInMemoryCheckpointRepositoryPosition, TestInMemoryCheckpointRepositoryPositionConcurrency, TestSQLCheckpointRepositoryPosition
    from .test_checkpoint_position_core import pos

    __all__ = ["TestInMemoryCheckpointRepositoryPosition", "TestInMemoryCheckpointRepositoryPositionConcurrency", "TestSQLCheckpointRepositoryPosition", "pos"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
