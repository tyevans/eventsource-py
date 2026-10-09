---
id: REFACTOR-tests-unit-domain-test_aggregate_root
title: Refactor and Decompose Legacy File test_aggregate_root.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_aggregate_root: Refactor Legacy File test_aggregate_root.py

## Summary
The grandfathered debt file `tests/unit/domain/test_aggregate_root.py` contains 1745 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_aggregate_root_counter.py, test_aggregate_root_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_aggregate_root/` with submodules:
- `test_aggregate_root_counter.py`: CounterState, CounterIncremented, CounterDecremented, CounterNamed, CounterReset, CounterAggregate, DeclarativeCounterAggregate, LenientCounterAggregate, OrderState, OrderCreated, OrderItemAdded, OrderShipped, OrderAggregate, TestAggregateRootInitialization, TestVersionTracking, TestLoadFromHistory, TestCommandMethods, TestComplexStateManagement, TestDeclarativeAggregate, TestAggregateEquality, TestAggregateRepr, TestImmutableStatePatterns, TestEdgeCases, TestAggregateTypeRequired, TestStrictUnregisteredDefault
- `test_aggregate_root_event.py`: TestEventApplication, TestUncommittedEventManagement, TestRaiseEventMethod, TestIntegrationWithDomainEvent, TestEventVersionValidation, TestEventVersionErrorException, TestUnregisteredEventHandling, TestUnhandledEventErrorException

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/domain/test_aggregate_root.py (1745 lines):
  Submodule 'test_aggregate_root_counter.py' (~788 lines):
    - [class] CounterState (lines 38-43)
    - [class] CounterIncremented (lines 56-60)
    - [class] CounterDecremented (lines 63-67)
    - [class] CounterNamed (lines 70-74)
    - [class] CounterReset (lines 77-80)
    - [class] CounterAggregate (lines 110-175)
    - [class] DeclarativeCounterAggregate (lines 247-295)
    - [class] LenientCounterAggregate (lines 1020-1023)
    - [class] OrderState (lines 46-53)
    - [class] OrderCreated (lines 83-87)
    - [class] OrderItemAdded (lines 90-95)
    - [class] OrderShipped (lines 98-102)
    - [class] OrderAggregate (lines 178-239)
    - [class] TestAggregateRootInitialization (lines 303-334)
    - [class] TestVersionTracking (lines 422-448)
    - [class] TestLoadFromHistory (lines 503-589)
    - [class] TestCommandMethods (lines 592-629)
    - [class] TestComplexStateManagement (lines 632-694)
    - [class] TestDeclarativeAggregate (lines 711-826)
    - [class] TestAggregateEquality (lines 829-871)
    - [class] TestAggregateRepr (lines 874-896)
    - [class] TestImmutableStatePatterns (lines 899-933)
    - [class] TestEdgeCases (lines 936-981)
    - [class] TestAggregateTypeRequired (lines 1696-1718)
    - [class] TestStrictUnregisteredDefault (lines 1721-1745)
  Submodule 'test_aggregate_root_event.py' (~831 lines):
    - [class] TestEventApplication (lines 337-419)
    - [class] TestUncommittedEventManagement (lines 451-500)
    - [class] TestRaiseEventMethod (lines 697-708)
    - [class] TestIntegrationWithDomainEvent (lines 984-1012)
    - [class] TestEventVersionValidation (lines 1026-1308)
    - [class] TestEventVersionErrorException (lines 1311-1361)
    - [class] TestUnregisteredEventHandling (lines 1369-1629)
    - [class] TestUnhandledEventErrorException (lines 1632-1693)
  Suggested barrel exports:
    from .test_aggregate_root_counter import CounterState, CounterIncremented, CounterDecremented, CounterNamed, CounterReset, CounterAggregate, DeclarativeCounterAggregate, LenientCounterAggregate, OrderState, OrderCreated, OrderItemAdded, OrderShipped, OrderAggregate, TestAggregateRootInitialization, TestVersionTracking, TestLoadFromHistory, TestCommandMethods, TestComplexStateManagement, TestDeclarativeAggregate, TestAggregateEquality, TestAggregateRepr, TestImmutableStatePatterns, TestEdgeCases, TestAggregateTypeRequired, TestStrictUnregisteredDefault
    from .test_aggregate_root_event import TestEventApplication, TestUncommittedEventManagement, TestRaiseEventMethod, TestIntegrationWithDomainEvent, TestEventVersionValidation, TestEventVersionErrorException, TestUnregisteredEventHandling, TestUnhandledEventErrorException

    __all__ = ["CounterState", "CounterIncremented", "CounterDecremented", "CounterNamed", "CounterReset", "CounterAggregate", "DeclarativeCounterAggregate", "LenientCounterAggregate", "OrderState", "OrderCreated", "OrderItemAdded", "OrderShipped", "OrderAggregate", "TestAggregateRootInitialization", "TestVersionTracking", "TestLoadFromHistory", "TestCommandMethods", "TestComplexStateManagement", "TestDeclarativeAggregate", "TestAggregateEquality", "TestAggregateRepr", "TestImmutableStatePatterns", "TestEdgeCases", "TestAggregateTypeRequired", "TestStrictUnregisteredDefault", "TestEventApplication", "TestUncommittedEventManagement", "TestRaiseEventMethod", "TestIntegrationWithDomainEvent", "TestEventVersionValidation", "TestEventVersionErrorException", "TestUnregisteredEventHandling", "TestUnhandledEventErrorException"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
