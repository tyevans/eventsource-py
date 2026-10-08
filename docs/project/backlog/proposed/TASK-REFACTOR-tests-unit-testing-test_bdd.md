---
id: REFACTOR-tests-unit-testing-test_bdd
title: Refactor and Decompose Legacy File test_bdd.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-testing-test_bdd: Refactor Legacy File test_bdd.py

## Summary
The grandfathered debt file `tests/unit/testing/test_bdd.py` contains 841 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_bdd_sample.py, test_bdd_aggregate.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/testing/test_bdd/` with submodules:
- `test_bdd_sample.py`: SampleCreated, SampleUpdated, SampleDeleted, SampleState, SampleAggregate, sample_aggregate, harness, TestGivenEvents, TestWhenCommand, TestThenEventPublished, TestThenNoEventsPublished, TestThenEventSequence, TestThenEventCount, TestBDDIntegration
- `test_bdd_aggregate.py`: OtherAggregateCreated, aggregate_id

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/testing/test_bdd.py (841 lines):
  Submodule 'test_bdd_sample.py' (~718 lines):
    - [class] SampleCreated (lines 35-39)
    - [class] SampleUpdated (lines 43-47)
    - [class] SampleDeleted (lines 51-54)
    - [class] SampleState (lines 70-74)
    - [class] SampleAggregate (lines 77-120)
    - [function] sample_aggregate (lines 141-143)
    - [function] harness (lines 129-131)
    - [class] TestGivenEvents (lines 151-297)
    - [class] TestWhenCommand (lines 305-356)
    - [class] TestThenEventPublished (lines 364-473)
    - [class] TestThenNoEventsPublished (lines 481-535)
    - [class] TestThenEventSequence (lines 543-674)
    - [class] TestThenEventCount (lines 682-751)
    - [class] TestBDDIntegration (lines 759-841)
  Submodule 'test_bdd_aggregate.py' (~8 lines):
    - [class] OtherAggregateCreated (lines 58-62)
    - [function] aggregate_id (lines 135-137)
  Suggested barrel exports:
    from .test_bdd_sample import SampleCreated, SampleUpdated, SampleDeleted, SampleState, SampleAggregate, sample_aggregate, harness, TestGivenEvents, TestWhenCommand, TestThenEventPublished, TestThenNoEventsPublished, TestThenEventSequence, TestThenEventCount, TestBDDIntegration
    from .test_bdd_aggregate import OtherAggregateCreated, aggregate_id

    __all__ = ["SampleCreated", "SampleUpdated", "SampleDeleted", "SampleState", "SampleAggregate", "sample_aggregate", "harness", "TestGivenEvents", "TestWhenCommand", "TestThenEventPublished", "TestThenNoEventsPublished", "TestThenEventSequence", "TestThenEventCount", "TestBDDIntegration", "OtherAggregateCreated", "aggregate_id"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
