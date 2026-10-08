---
id: REFACTOR-tests-unit-adapters-test_sqlite_snapshots
title: Refactor and Decompose Legacy File test_sqlite_snapshots.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_sqlite_snapshots: Refactor Legacy File test_sqlite_snapshots.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_sqlite_snapshots.py` contains 509 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_sqlite_snapshots_configuration.py, test_sqlite_snapshots_operations.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_sqlite_snapshots/` with submodules:
- `test_sqlite_snapshots_configuration.py`: TestSQLiteSnapshotStoreConfiguration, TestSQLiteSnapshotStoreFilePersistence, TestSQLiteSnapshotStoreLifecycle, TestSQLiteSnapshotStoreConcurrency
- `test_sqlite_snapshots_operations.py`: TestSQLiteSnapshotStoreOperations

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/test_sqlite_snapshots.py (509 lines):
  Submodule 'test_sqlite_snapshots_configuration.py' (~177 lines):
    - [class] TestSQLiteSnapshotStoreConfiguration (lines 26-41)
    - [class] TestSQLiteSnapshotStoreFilePersistence (lines 345-377)
    - [class] TestSQLiteSnapshotStoreLifecycle (lines 380-469)
    - [class] TestSQLiteSnapshotStoreConcurrency (lines 472-509)
  Submodule 'test_sqlite_snapshots_operations.py' (~299 lines):
    - [class] TestSQLiteSnapshotStoreOperations (lines 44-342)
  Suggested barrel exports:
    from .test_sqlite_snapshots_configuration import TestSQLiteSnapshotStoreConfiguration, TestSQLiteSnapshotStoreFilePersistence, TestSQLiteSnapshotStoreLifecycle, TestSQLiteSnapshotStoreConcurrency
    from .test_sqlite_snapshots_operations import TestSQLiteSnapshotStoreOperations

    __all__ = ["TestSQLiteSnapshotStoreConfiguration", "TestSQLiteSnapshotStoreFilePersistence", "TestSQLiteSnapshotStoreLifecycle", "TestSQLiteSnapshotStoreConcurrency", "TestSQLiteSnapshotStoreOperations"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
