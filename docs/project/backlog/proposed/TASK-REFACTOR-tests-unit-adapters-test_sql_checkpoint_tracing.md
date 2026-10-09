---
id: REFACTOR-tests-unit-adapters-test_sql_checkpoint_tracing
title: Refactor and Decompose Legacy File test_sql_checkpoint_tracing.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_sql_checkpoint_tracing: Refactor Legacy File test_sql_checkpoint_tracing.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_sql_checkpoint_tracing.py` contains 562 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_sql_checkpoint_tracing_repository.py, test_sql_checkpoint_tracing_engine.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_sql_checkpoint_tracing/` with submodules:
- `test_sql_checkpoint_tracing_repository.py`: TestInMemoryCheckpointRepositoryTracerIntegration, TestInMemoryCheckpointRepositorySpanCreation, TestInMemoryCheckpointRepositoryTracingDisabled, TestSQLCheckpointRepositoryTracerIntegration, TestSQLCheckpointRepositorySpanCreation, TestSQLCheckpointRepositoryTracingDisabled, TestCheckpointRepositoryStandardAttributes
- `test_sql_checkpoint_tracing_engine.py`: _sqlite_checkpoint_engine

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/test_sql_checkpoint_tracing.py (562 lines):
  Submodule 'test_sql_checkpoint_tracing_repository.py' (~472 lines):
    - [class] TestInMemoryCheckpointRepositoryTracerIntegration (lines 30-66)
    - [class] TestInMemoryCheckpointRepositorySpanCreation (lines 74-199)
    - [class] TestInMemoryCheckpointRepositoryTracingDisabled (lines 207-279)
    - [class] TestSQLCheckpointRepositoryTracerIntegration (lines 314-361)
    - [class] TestSQLCheckpointRepositorySpanCreation (lines 365-457)
    - [class] TestSQLCheckpointRepositoryTracingDisabled (lines 461-505)
    - [class] TestCheckpointRepositoryStandardAttributes (lines 513-562)
  Submodule 'test_sql_checkpoint_tracing_engine.py' (~12 lines):
    - [function] _sqlite_checkpoint_engine (lines 299-310)
  Suggested barrel exports:
    from .test_sql_checkpoint_tracing_repository import TestInMemoryCheckpointRepositoryTracerIntegration, TestInMemoryCheckpointRepositorySpanCreation, TestInMemoryCheckpointRepositoryTracingDisabled, TestSQLCheckpointRepositoryTracerIntegration, TestSQLCheckpointRepositorySpanCreation, TestSQLCheckpointRepositoryTracingDisabled, TestCheckpointRepositoryStandardAttributes
    from .test_sql_checkpoint_tracing_engine import _sqlite_checkpoint_engine

    __all__ = ["TestInMemoryCheckpointRepositoryTracerIntegration", "TestInMemoryCheckpointRepositorySpanCreation", "TestInMemoryCheckpointRepositoryTracingDisabled", "TestSQLCheckpointRepositoryTracerIntegration", "TestSQLCheckpointRepositorySpanCreation", "TestSQLCheckpointRepositoryTracingDisabled", "TestCheckpointRepositoryStandardAttributes", "_sqlite_checkpoint_engine"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
