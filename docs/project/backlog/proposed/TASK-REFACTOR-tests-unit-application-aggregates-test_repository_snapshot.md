---
id: REFACTOR-tests-unit-application-aggregates-test_repository_snapshot
title: Refactor and Decompose Legacy File test_repository_snapshot.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-aggregates-test_repository_snapshot: Refactor Legacy File test_repository_snapshot.py

## Summary
The grandfathered debt file `tests/unit/application/aggregates/test_repository_snapshot.py` contains 1233 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_repository_snapshot_event.py, test_repository_snapshot_aggregate.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/aggregates/test_repository_snapshot/` with submodules:
- `test_repository_snapshot_event.py`: TestEvent, CountEvent, TestState, TestRepositoryConstructor, TestRepositorySnapshotLoad, TestRepositoryAutoSnapshot, TestRepositoryManualSnapshot, TestRepositoryBackgroundSnapshot, TestSnapshotPolicyLogic, TestFullCycleIntegration, TestEdgeCases
- `test_repository_snapshot_aggregate.py`: TestAggregate, TestAggregateV2

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/aggregates/test_repository_snapshot.py (1233 lines):
  Submodule 'test_repository_snapshot_event.py' (~1063 lines):
    - [class] TestEvent (lines 53-57)
    - [class] CountEvent (lines 61-64)
    - [class] TestState (lines 41-46)
    - [class] TestRepositoryConstructor (lines 137-200)
    - [class] TestRepositorySnapshotLoad (lines 208-461)
    - [class] TestRepositoryAutoSnapshot (lines 469-631)
    - [class] TestRepositoryManualSnapshot (lines 639-738)
    - [class] TestRepositoryBackgroundSnapshot (lines 746-864)
    - [class] TestSnapshotPolicyLogic (lines 872-997)
    - [class] TestFullCycleIntegration (lines 1005-1125)
    - [class] TestEdgeCases (lines 1133-1233)
  Submodule 'test_repository_snapshot_aggregate.py' (~61 lines):
    - [class] TestAggregate (lines 67-108)
    - [class] TestAggregateV2 (lines 111-129)
  Suggested barrel exports:
    from .test_repository_snapshot_event import TestEvent, CountEvent, TestState, TestRepositoryConstructor, TestRepositorySnapshotLoad, TestRepositoryAutoSnapshot, TestRepositoryManualSnapshot, TestRepositoryBackgroundSnapshot, TestSnapshotPolicyLogic, TestFullCycleIntegration, TestEdgeCases
    from .test_repository_snapshot_aggregate import TestAggregate, TestAggregateV2

    __all__ = ["TestEvent", "CountEvent", "TestState", "TestRepositoryConstructor", "TestRepositorySnapshotLoad", "TestRepositoryAutoSnapshot", "TestRepositoryManualSnapshot", "TestRepositoryBackgroundSnapshot", "TestSnapshotPolicyLogic", "TestFullCycleIntegration", "TestEdgeCases", "TestAggregate", "TestAggregateV2"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
