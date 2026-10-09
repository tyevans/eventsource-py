---
id: REFACTOR-tests-unit-test_edge_cases
title: Refactor and Decompose Legacy File test_edge_cases.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-test_edge_cases: Refactor Legacy File test_edge_cases.py

## Summary
The grandfathered debt file `tests/unit/test_edge_cases.py` contains 556 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_edge_cases_event.py, test_edge_cases_repository.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/test_edge_cases/` with submodules:
- `test_edge_cases_event.py`: EdgeTestEvent, SampleOrderEvent, TestInMemoryEventStoreEdgeCases, TestInMemoryEventBusEdgeCases, TestConcurrentAccess, TestSerializationEdgeCases
- `test_edge_cases_repository.py`: TestCheckpointRepositoryEdgeCases, TestDLQRepositoryEdgeCases, TestOutboxRepositoryEdgeCases

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/test_edge_cases.py (556 lines):
  Submodule 'test_edge_cases_event.py' (~380 lines):
    - [class] EdgeTestEvent (lines 41-45)
    - [class] SampleOrderEvent (lines 49-53)
    - [class] TestInMemoryEventStoreEdgeCases (lines 59-175)
    - [class] TestInMemoryEventBusEdgeCases (lines 181-255)
    - [class] TestConcurrentAccess (lines 374-503)
    - [class] TestSerializationEdgeCases (lines 509-556)
  Submodule 'test_edge_cases_repository.py' (~98 lines):
    - [class] TestCheckpointRepositoryEdgeCases (lines 261-280)
    - [class] TestDLQRepositoryEdgeCases (lines 286-345)
    - [class] TestOutboxRepositoryEdgeCases (lines 351-368)
  Suggested barrel exports:
    from .test_edge_cases_event import EdgeTestEvent, SampleOrderEvent, TestInMemoryEventStoreEdgeCases, TestInMemoryEventBusEdgeCases, TestConcurrentAccess, TestSerializationEdgeCases
    from .test_edge_cases_repository import TestCheckpointRepositoryEdgeCases, TestDLQRepositoryEdgeCases, TestOutboxRepositoryEdgeCases

    __all__ = ["EdgeTestEvent", "SampleOrderEvent", "TestInMemoryEventStoreEdgeCases", "TestInMemoryEventBusEdgeCases", "TestConcurrentAccess", "TestSerializationEdgeCases", "TestCheckpointRepositoryEdgeCases", "TestDLQRepositoryEdgeCases", "TestOutboxRepositoryEdgeCases"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
