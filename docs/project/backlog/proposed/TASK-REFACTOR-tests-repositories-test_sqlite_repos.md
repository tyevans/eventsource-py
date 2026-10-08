---
id: REFACTOR-tests-repositories-test_sqlite_repos
title: Refactor and Decompose Legacy File test_sqlite_repos.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-repositories-test_sqlite_repos: Refactor Legacy File test_sqlite_repos.py

## Summary
The grandfathered debt file `tests/repositories/test_sqlite_repos.py` contains 679 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_sqlite_repos_repository.py, test_sqlite_repos_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/repositories/test_sqlite_repos/` with submodules:
- `test_sqlite_repos_repository.py`: TestSQLCheckpointRepositoryProtocol, TestSQLCheckpointRepositoryGetCheckpoint, TestSQLCheckpointRepositoryUpdateCheckpoint, TestSQLCheckpointRepositoryGetAllCheckpoints, TestSQLCheckpointRepositoryResetCheckpoint, TestSQLCheckpointRepositoryGetLagMetrics, TestSQLDLQRepositoryProtocol, TestSQLDLQRepositoryAddFailedEvent, TestSQLDLQRepositoryGetFailedEvents, TestSQLDLQRepositoryGetFailedEventById, TestSQLDLQRepositoryMarkResolved, TestSQLDLQRepositoryMarkRetrying, TestSQLDLQRepositoryGetFailureStats, TestSQLDLQRepositoryGetProjectionFailureCounts, TestSQLDLQRepositoryDeleteResolvedEvents, TestSQLDLQRepositoryEventDataSerialization
- `test_sqlite_repos_event.py`: TestSampleEvent

## AST Decomposition Blueprint
Decomposition Blueprint for tests/repositories/test_sqlite_repos.py (679 lines):
  Submodule 'test_sqlite_repos_repository.py' (~585 lines):
    - [class] TestSQLCheckpointRepositoryProtocol (lines 59-64)
    - [class] TestSQLCheckpointRepositoryGetCheckpoint (lines 67-93)
    - [class] TestSQLCheckpointRepositoryUpdateCheckpoint (lines 96-159)
    - [class] TestSQLCheckpointRepositoryGetAllCheckpoints (lines 162-189)
    - [class] TestSQLCheckpointRepositoryResetCheckpoint (lines 192-222)
    - [class] TestSQLCheckpointRepositoryGetLagMetrics (lines 226-253)
    - [class] TestSQLDLQRepositoryProtocol (lines 261-266)
    - [class] TestSQLDLQRepositoryAddFailedEvent (lines 269-342)
    - [class] TestSQLDLQRepositoryGetFailedEvents (lines 345-410)
    - [class] TestSQLDLQRepositoryGetFailedEventById (lines 413-442)
    - [class] TestSQLDLQRepositoryMarkResolved (lines 445-485)
    - [class] TestSQLDLQRepositoryMarkRetrying (lines 488-508)
    - [class] TestSQLDLQRepositoryGetFailureStats (lines 511-554)
    - [class] TestSQLDLQRepositoryGetProjectionFailureCounts (lines 557-579)
    - [class] TestSQLDLQRepositoryDeleteResolvedEvents (lines 582-636)
    - [class] TestSQLDLQRepositoryEventDataSerialization (lines 639-679)
  Submodule 'test_sqlite_repos_event.py' (~5 lines):
    - [class] TestSampleEvent (lines 47-51)
  Suggested barrel exports:
    from .test_sqlite_repos_repository import TestSQLCheckpointRepositoryProtocol, TestSQLCheckpointRepositoryGetCheckpoint, TestSQLCheckpointRepositoryUpdateCheckpoint, TestSQLCheckpointRepositoryGetAllCheckpoints, TestSQLCheckpointRepositoryResetCheckpoint, TestSQLCheckpointRepositoryGetLagMetrics, TestSQLDLQRepositoryProtocol, TestSQLDLQRepositoryAddFailedEvent, TestSQLDLQRepositoryGetFailedEvents, TestSQLDLQRepositoryGetFailedEventById, TestSQLDLQRepositoryMarkResolved, TestSQLDLQRepositoryMarkRetrying, TestSQLDLQRepositoryGetFailureStats, TestSQLDLQRepositoryGetProjectionFailureCounts, TestSQLDLQRepositoryDeleteResolvedEvents, TestSQLDLQRepositoryEventDataSerialization
    from .test_sqlite_repos_event import TestSampleEvent

    __all__ = ["TestSQLCheckpointRepositoryProtocol", "TestSQLCheckpointRepositoryGetCheckpoint", "TestSQLCheckpointRepositoryUpdateCheckpoint", "TestSQLCheckpointRepositoryGetAllCheckpoints", "TestSQLCheckpointRepositoryResetCheckpoint", "TestSQLCheckpointRepositoryGetLagMetrics", "TestSQLDLQRepositoryProtocol", "TestSQLDLQRepositoryAddFailedEvent", "TestSQLDLQRepositoryGetFailedEvents", "TestSQLDLQRepositoryGetFailedEventById", "TestSQLDLQRepositoryMarkResolved", "TestSQLDLQRepositoryMarkRetrying", "TestSQLDLQRepositoryGetFailureStats", "TestSQLDLQRepositoryGetProjectionFailureCounts", "TestSQLDLQRepositoryDeleteResolvedEvents", "TestSQLDLQRepositoryEventDataSerialization", "TestSampleEvent"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
