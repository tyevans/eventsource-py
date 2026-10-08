---
id: REFACTOR-tests-unit-application-aggregates-test_repository
title: Refactor and Decompose Legacy File test_repository.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-aggregates-test_repository: Refactor Legacy File test_repository.py

## Summary
The grandfathered debt file `tests/unit/application/aggregates/test_repository.py` contains 988 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_repository_counter.py, test_repository_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/aggregates/test_repository/` with submodules:
- `test_repository_counter.py`: CounterState, CounterIncremented, CounterDecremented, CounterAggregate, counter_repository, counter_repository_with_publisher, MockEventPublisher, event_store, mock_publisher, TestRepositoryInitialization, TestLoadAggregate, TestLoadOrCreate, TestGetOrRaise, TestSaveAggregate, TestOptimisticConcurrency, TestEventPublishing, TestExistsMethod, TestGetVersionMethod, TestCreateNewMethod, TestComplexScenarios, TestGenericTyping, TestImports, TestEdgeCases
- `test_repository_order.py`: OrderState, OrderCreated, OrderItemAdded, OrderShipped, OrderAggregate, order_repository

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/aggregates/test_repository.py (988 lines):
  Submodule 'test_repository_counter.py' (~781 lines):
    - [class] CounterState (lines 35-40)
    - [class] CounterIncremented (lines 53-57)
    - [class] CounterDecremented (lines 60-64)
    - [class] CounterAggregate (lines 94-134)
    - [function] counter_repository (lines 228-233)
    - [function] counter_repository_with_publisher (lines 252-260)
    - [class] MockEventPublisher (lines 206-213)
    - [function] event_store (lines 222-224)
    - [function] mock_publisher (lines 246-248)
    - [class] TestRepositoryInitialization (lines 268-304)
    - [class] TestLoadAggregate (lines 307-391)
    - [class] TestLoadOrCreate (lines 394-451)
    - [class] TestGetOrRaise (lines 454-491)
    - [class] TestSaveAggregate (lines 494-589)
    - [class] TestOptimisticConcurrency (lines 592-661)
    - [class] TestEventPublishing (lines 664-707)
    - [class] TestExistsMethod (lines 710-737)
    - [class] TestGetVersionMethod (lines 740-769)
    - [class] TestCreateNewMethod (lines 772-798)
    - [class] TestComplexScenarios (lines 801-877)
    - [class] TestGenericTyping (lines 880-913)
    - [class] TestImports (lines 916-935)
    - [class] TestEdgeCases (lines 938-988)
  Submodule 'test_repository_order.py' (~92 lines):
    - [class] OrderState (lines 43-50)
    - [class] OrderCreated (lines 67-71)
    - [class] OrderItemAdded (lines 74-79)
    - [class] OrderShipped (lines 82-86)
    - [class] OrderAggregate (lines 137-198)
    - [function] order_repository (lines 237-242)
  Suggested barrel exports:
    from .test_repository_counter import CounterState, CounterIncremented, CounterDecremented, CounterAggregate, counter_repository, counter_repository_with_publisher, MockEventPublisher, event_store, mock_publisher, TestRepositoryInitialization, TestLoadAggregate, TestLoadOrCreate, TestGetOrRaise, TestSaveAggregate, TestOptimisticConcurrency, TestEventPublishing, TestExistsMethod, TestGetVersionMethod, TestCreateNewMethod, TestComplexScenarios, TestGenericTyping, TestImports, TestEdgeCases
    from .test_repository_order import OrderState, OrderCreated, OrderItemAdded, OrderShipped, OrderAggregate, order_repository

    __all__ = ["CounterState", "CounterIncremented", "CounterDecremented", "CounterAggregate", "counter_repository", "counter_repository_with_publisher", "MockEventPublisher", "event_store", "mock_publisher", "TestRepositoryInitialization", "TestLoadAggregate", "TestLoadOrCreate", "TestGetOrRaise", "TestSaveAggregate", "TestOptimisticConcurrency", "TestEventPublishing", "TestExistsMethod", "TestGetVersionMethod", "TestCreateNewMethod", "TestComplexScenarios", "TestGenericTyping", "TestImports", "TestEdgeCases", "OrderState", "OrderCreated", "OrderItemAdded", "OrderShipped", "OrderAggregate", "order_repository"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
