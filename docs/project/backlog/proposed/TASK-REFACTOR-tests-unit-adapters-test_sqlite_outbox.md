---
id: REFACTOR-tests-unit-adapters-test_sqlite_outbox
title: Refactor and Decompose Legacy File test_sqlite_outbox.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-test_sqlite_outbox: Refactor Legacy File test_sqlite_outbox.py

## Summary
The grandfathered debt file `tests/unit/adapters/test_sqlite_outbox.py` contains 712 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_sqlite_outbox_repository.py, test_sqlite_outbox_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/test_sqlite_outbox/` with submodules:
- `test_sqlite_outbox_repository.py`: TestSQLiteOutboxRepositoryProtocol, TestSQLiteOutboxRepositoryAddEvent, TestSQLiteOutboxRepositoryGetPendingEvents, TestSQLiteOutboxRepositoryMarkPublished, TestSQLiteOutboxRepositoryMarkFailed, TestSQLiteOutboxRepositoryIncrementRetry, TestSQLiteOutboxRepositoryCleanupPublished, TestSQLiteOutboxRepositoryGetStats, TestSQLiteOutboxRepositoryMultipleEvents, TestSQLiteOutboxRepositoryStandalone, TestSQLiteOutboxRepositoryStandaloneProtocol
- `test_sqlite_outbox_event.py`: TestSampleEvent, SampleEvent

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/adapters/test_sqlite_outbox.py (712 lines):
  Submodule 'test_sqlite_outbox_repository.py' (~638 lines):
    - [class] TestSQLiteOutboxRepositoryProtocol (lines 41-46)
    - [class] TestSQLiteOutboxRepositoryAddEvent (lines 49-94)
    - [class] TestSQLiteOutboxRepositoryGetPendingEvents (lines 97-137)
    - [class] TestSQLiteOutboxRepositoryMarkPublished (lines 140-157)
    - [class] TestSQLiteOutboxRepositoryMarkFailed (lines 160-177)
    - [class] TestSQLiteOutboxRepositoryIncrementRetry (lines 180-195)
    - [class] TestSQLiteOutboxRepositoryCleanupPublished (lines 198-261)
    - [class] TestSQLiteOutboxRepositoryGetStats (lines 264-295)
    - [class] TestSQLiteOutboxRepositoryMultipleEvents (lines 298-318)
    - [class] TestSQLiteOutboxRepositoryStandalone (lines 334-673)
    - [class] TestSQLiteOutboxRepositoryStandaloneProtocol (lines 677-712)
  Submodule 'test_sqlite_outbox_event.py' (~10 lines):
    - [class] TestSampleEvent (lines 29-33)
    - [class] SampleEvent (lines 326-330)
  Suggested barrel exports:
    from .test_sqlite_outbox_repository import TestSQLiteOutboxRepositoryProtocol, TestSQLiteOutboxRepositoryAddEvent, TestSQLiteOutboxRepositoryGetPendingEvents, TestSQLiteOutboxRepositoryMarkPublished, TestSQLiteOutboxRepositoryMarkFailed, TestSQLiteOutboxRepositoryIncrementRetry, TestSQLiteOutboxRepositoryCleanupPublished, TestSQLiteOutboxRepositoryGetStats, TestSQLiteOutboxRepositoryMultipleEvents, TestSQLiteOutboxRepositoryStandalone, TestSQLiteOutboxRepositoryStandaloneProtocol
    from .test_sqlite_outbox_event import TestSampleEvent, SampleEvent

    __all__ = ["TestSQLiteOutboxRepositoryProtocol", "TestSQLiteOutboxRepositoryAddEvent", "TestSQLiteOutboxRepositoryGetPendingEvents", "TestSQLiteOutboxRepositoryMarkPublished", "TestSQLiteOutboxRepositoryMarkFailed", "TestSQLiteOutboxRepositoryIncrementRetry", "TestSQLiteOutboxRepositoryCleanupPublished", "TestSQLiteOutboxRepositoryGetStats", "TestSQLiteOutboxRepositoryMultipleEvents", "TestSQLiteOutboxRepositoryStandalone", "TestSQLiteOutboxRepositoryStandaloneProtocol", "TestSampleEvent", "SampleEvent"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
