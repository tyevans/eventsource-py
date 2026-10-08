---
id: REFACTOR-tests-unit-application-aggregates-test_repository_tracing
title: Refactor and Decompose Legacy File test_repository_tracing.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-aggregates-test_repository_tracing: Refactor Legacy File test_repository_tracing.py

## Summary
The grandfathered debt file `tests/unit/application/aggregates/test_repository_tracing.py` contains 683 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_repository_tracing_aggregate.py, test_repository_tracing_state.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/aggregates/test_repository_tracing/` with submodules:
- `test_repository_tracing_aggregate.py`: TracingTestAggregate, TestAggregateRepositoryTracingComposition, TestAggregateRepositorySpanCreation, TestAggregateRepositoryTracingDisabled, TestAggregateRepositorySpanDynamicAttributes, TestAggregateRepositoryStandardAttributes, TestAggregateRepositoryTracingMultipleEvents, TracingTestEvent
- `test_repository_tracing_state.py`: TracingTestState

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/aggregates/test_repository_tracing.py (683 lines):
  Submodule 'test_repository_tracing_aggregate.py' (~581 lines):
    - [class] TracingTestAggregate (lines 66-94)
    - [class] TestAggregateRepositoryTracingComposition (lines 102-161)
    - [class] TestAggregateRepositorySpanCreation (lines 169-365)
    - [class] TestAggregateRepositoryTracingDisabled (lines 373-459)
    - [class] TestAggregateRepositorySpanDynamicAttributes (lines 467-577)
    - [class] TestAggregateRepositoryStandardAttributes (lines 585-619)
    - [class] TestAggregateRepositoryTracingMultipleEvents (lines 627-683)
    - [class] TracingTestEvent (lines 59-63)
  Submodule 'test_repository_tracing_state.py' (~6 lines):
    - [class] TracingTestState (lines 47-52)
  Suggested barrel exports:
    from .test_repository_tracing_aggregate import TracingTestAggregate, TestAggregateRepositoryTracingComposition, TestAggregateRepositorySpanCreation, TestAggregateRepositoryTracingDisabled, TestAggregateRepositorySpanDynamicAttributes, TestAggregateRepositoryStandardAttributes, TestAggregateRepositoryTracingMultipleEvents, TracingTestEvent
    from .test_repository_tracing_state import TracingTestState

    __all__ = ["TracingTestAggregate", "TestAggregateRepositoryTracingComposition", "TestAggregateRepositorySpanCreation", "TestAggregateRepositoryTracingDisabled", "TestAggregateRepositorySpanDynamicAttributes", "TestAggregateRepositoryStandardAttributes", "TestAggregateRepositoryTracingMultipleEvents", "TracingTestEvent", "TracingTestState"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
