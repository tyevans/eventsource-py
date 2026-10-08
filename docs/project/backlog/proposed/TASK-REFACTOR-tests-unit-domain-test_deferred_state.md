---
id: REFACTOR-tests-unit-domain-test_deferred_state
title: Refactor and Decompose Legacy File test_deferred_state.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_deferred_state: Refactor Legacy File test_deferred_state.py

## Summary
The grandfathered debt file `tests/unit/domain/test_deferred_state.py` contains 623 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_deferred_state_aggregate.py, test_deferred_state_extraction.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_deferred_state/` with submodules:
- `test_deferred_state_aggregate.py`: TestDeferredStateAggregate, TestDeferredStateAggregateWithReplay, TestTraditionalAggregate, TestAggregateNotCreatedError, TestAggregateNotCreatedErrorFromAggregate, OrderState, OrderCreated, OrderShipped, Order, TestBackwardCompatibility, TestEdgeCases, TestDeferredStateWithInheritance, TestStatePropertyConsistency
- `test_deferred_state_extraction.py`: ExtractionState, ExtractionRequested, ExtractionCompleted, ExtractionProcess

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/domain/test_deferred_state.py (623 lines):
  Submodule 'test_deferred_state_aggregate.py' (~481 lines):
    - [class] TestDeferredStateAggregate (lines 145-235)
    - [class] TestDeferredStateAggregateWithReplay (lines 238-269)
    - [class] TestTraditionalAggregate (lines 277-349)
    - [class] TestAggregateNotCreatedError (lines 357-389)
    - [class] TestAggregateNotCreatedErrorFromAggregate (lines 392-411)
    - [class] OrderState (lines 57-61)
    - [class] OrderCreated (lines 64-68)
    - [class] OrderShipped (lines 71-75)
    - [class] Order (lines 110-137)
    - [class] TestBackwardCompatibility (lines 419-477)
    - [class] TestEdgeCases (lines 485-523)
    - [class] TestDeferredStateWithInheritance (lines 526-568)
    - [class] TestStatePropertyConsistency (lines 576-623)
  Submodule 'test_deferred_state_extraction.py' (~41 lines):
    - [class] ExtractionState (lines 30-34)
    - [class] ExtractionRequested (lines 37-42)
    - [class] ExtractionCompleted (lines 45-49)
    - [class] ExtractionProcess (lines 83-107)
  Suggested barrel exports:
    from .test_deferred_state_aggregate import TestDeferredStateAggregate, TestDeferredStateAggregateWithReplay, TestTraditionalAggregate, TestAggregateNotCreatedError, TestAggregateNotCreatedErrorFromAggregate, OrderState, OrderCreated, OrderShipped, Order, TestBackwardCompatibility, TestEdgeCases, TestDeferredStateWithInheritance, TestStatePropertyConsistency
    from .test_deferred_state_extraction import ExtractionState, ExtractionRequested, ExtractionCompleted, ExtractionProcess

    __all__ = ["TestDeferredStateAggregate", "TestDeferredStateAggregateWithReplay", "TestTraditionalAggregate", "TestAggregateNotCreatedError", "TestAggregateNotCreatedErrorFromAggregate", "OrderState", "OrderCreated", "OrderShipped", "Order", "TestBackwardCompatibility", "TestEdgeCases", "TestDeferredStateWithInheritance", "TestStatePropertyConsistency", "ExtractionState", "ExtractionRequested", "ExtractionCompleted", "ExtractionProcess"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
