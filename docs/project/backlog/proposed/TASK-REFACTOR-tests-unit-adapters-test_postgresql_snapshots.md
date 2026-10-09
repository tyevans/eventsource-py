---
id: REFACTOR-tests-unit-adapters-test_postgresql_snapshots
title: Refactor and Decompose Legacy File test_postgresql_snapshots.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_postgresql_snapshots: Refactor Legacy File test_postgresql_snapshots.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_postgresql_snapshots.py` contains 762 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_postgresql_snapshots_mock.py, test_postgresql_snapshots_create.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_postgresql_snapshots/` with submodules:
- `test_postgresql_snapshots_mock.py`: mock_session, mock_session_factory, create_mock_result, store, aggregate_id, sample_snapshot, TestPostgreSQLSnapshotStoreBasic, TestUpsertSemantics, TestBulkDelete, TestProperties, TestOpenTelemetryTracing, TestImports, TestComplexState
- `test_postgresql_snapshots_create.py`: create_scalar_result, create_snapshot_row

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/test_postgresql_snapshots.py (762 lines):
  Submodule 'test_postgresql_snapshots_mock.py' (~660 lines):
    - [function] mock_session (lines 32-35)
    - [function] mock_session_factory (lines 39-55)
    - [function] create_mock_result (lines 89-97)
    - [function] store (lines 59-64)
    - [function] aggregate_id (lines 68-70)
    - [function] sample_snapshot (lines 74-83)
    - [class] TestPostgreSQLSnapshotStoreBasic (lines 122-321)
    - [class] TestUpsertSemantics (lines 327-400)
    - [class] TestBulkDelete (lines 406-456)
    - [class] TestProperties (lines 462-492)
    - [class] TestOpenTelemetryTracing (lines 498-641)
    - [class] TestImports (lines 647-660)
    - [class] TestComplexState (lines 666-762)
  Submodule 'test_postgresql_snapshots_create.py' (~15 lines):
    - [function] create_scalar_result (lines 100-104)
    - [function] create_snapshot_row (lines 107-116)
  Suggested barrel exports:
    from .test_postgresql_snapshots_mock import mock_session, mock_session_factory, create_mock_result, store, aggregate_id, sample_snapshot, TestPostgreSQLSnapshotStoreBasic, TestUpsertSemantics, TestBulkDelete, TestProperties, TestOpenTelemetryTracing, TestImports, TestComplexState
    from .test_postgresql_snapshots_create import create_scalar_result, create_snapshot_row

    __all__ = ["mock_session", "mock_session_factory", "create_mock_result", "store", "aggregate_id", "sample_snapshot", "TestPostgreSQLSnapshotStoreBasic", "TestUpsertSemantics", "TestBulkDelete", "TestProperties", "TestOpenTelemetryTracing", "TestImports", "TestComplexState", "create_scalar_result", "create_snapshot_row"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
