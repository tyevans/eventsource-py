---
id: REFACTOR-tests-unit-adapters-test_snapshots_tracing
title: Refactor and Decompose Legacy File test_snapshots_tracing.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_snapshots_tracing: Refactor Legacy File test_snapshots_tracing.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_snapshots_tracing.py` contains 515 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_snapshots_tracing_store.py, test_snapshots_tracing_tracer.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_snapshots_tracing/` with submodules:
- `test_snapshots_tracing_store.py`: TestInMemorySnapshotStoreTracingComposition, TestInMemorySnapshotStoreSpanCreation, TestInMemorySnapshotStoreTracingDisabled, TestSnapshotStoreStandardAttributes, TestPostgreSQLSnapshotStoreTracerComposition, TestSQLiteSnapshotStoreTracerComposition, sample_snapshot
- `test_snapshots_tracing_tracer.py`: mock_tracer

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/test_snapshots_tracing.py (515 lines):
  Submodule 'test_snapshots_tracing_store.py' (~433 lines):
    - [class] TestInMemorySnapshotStoreTracingComposition (lines 63-102)
    - [class] TestInMemorySnapshotStoreSpanCreation (lines 110-272)
    - [class] TestInMemorySnapshotStoreTracingDisabled (lines 280-373)
    - [class] TestSnapshotStoreStandardAttributes (lines 381-481)
    - [class] TestPostgreSQLSnapshotStoreTracerComposition (lines 489-500)
    - [class] TestSQLiteSnapshotStoreTracerComposition (lines 503-515)
    - [function] sample_snapshot (lines 40-49)
  Submodule 'test_snapshots_tracing_tracer.py' (~3 lines):
    - [function] mock_tracer (lines 53-55)
  Suggested barrel exports:
    from .test_snapshots_tracing_store import TestInMemorySnapshotStoreTracingComposition, TestInMemorySnapshotStoreSpanCreation, TestInMemorySnapshotStoreTracingDisabled, TestSnapshotStoreStandardAttributes, TestPostgreSQLSnapshotStoreTracerComposition, TestSQLiteSnapshotStoreTracerComposition, sample_snapshot
    from .test_snapshots_tracing_tracer import mock_tracer

    __all__ = ["TestInMemorySnapshotStoreTracingComposition", "TestInMemorySnapshotStoreSpanCreation", "TestInMemorySnapshotStoreTracingDisabled", "TestSnapshotStoreStandardAttributes", "TestPostgreSQLSnapshotStoreTracerComposition", "TestSQLiteSnapshotStoreTracerComposition", "sample_snapshot", "mock_tracer"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
