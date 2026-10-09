---
id: REFACTOR-tests-unit-domain-test_create_event
title: Refactor and Decompose Legacy File test_create_event.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_create_event: Refactor Legacy File test_create_event.py

## Summary
The grandfathered debt file `tests/unit/domain/test_create_event.py` contains 587 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_create_event_order.py, test_create_event_aggregate.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_create_event/` with submodules:
- `test_create_event_order.py`: OrderState, OrderCreated, OrderShipped, OrderCancelled, OrderAggregate, DeclarativeOrderAggregate, TestCreateEventAutoPopulation, TestCreateEventReturnValue, TestCreateEventApplication, TestCreateEventOverrides, TestCreateEventTenantContext, TestBackwardCompatibility, TestCreateEventEdgeCases, TestCreateEventPerformance
- `test_create_event_aggregate.py`: TestCreateEventWithDeclarativeAggregate

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/domain/test_create_event.py (587 lines):
  Submodule 'test_create_event_order.py' (~463 lines):
    - [class] OrderState (lines 31-37)
    - [class] OrderCreated (lines 40-44)
    - [class] OrderShipped (lines 47-51)
    - [class] OrderCancelled (lines 54-58)
    - [class] OrderAggregate (lines 66-100)
    - [class] DeclarativeOrderAggregate (lines 103-130)
    - [class] TestCreateEventAutoPopulation (lines 138-169)
    - [class] TestCreateEventReturnValue (lines 172-194)
    - [class] TestCreateEventApplication (lines 197-231)
    - [class] TestCreateEventOverrides (lines 239-292)
    - [class] TestCreateEventTenantContext (lines 300-344)
    - [class] TestBackwardCompatibility (lines 352-411)
    - [class] TestCreateEventEdgeCases (lines 452-543)
    - [class] TestCreateEventPerformance (lines 551-587)
  Submodule 'test_create_event_aggregate.py' (~26 lines):
    - [class] TestCreateEventWithDeclarativeAggregate (lines 419-444)
  Suggested barrel exports:
    from .test_create_event_order import OrderState, OrderCreated, OrderShipped, OrderCancelled, OrderAggregate, DeclarativeOrderAggregate, TestCreateEventAutoPopulation, TestCreateEventReturnValue, TestCreateEventApplication, TestCreateEventOverrides, TestCreateEventTenantContext, TestBackwardCompatibility, TestCreateEventEdgeCases, TestCreateEventPerformance
    from .test_create_event_aggregate import TestCreateEventWithDeclarativeAggregate

    __all__ = ["OrderState", "OrderCreated", "OrderShipped", "OrderCancelled", "OrderAggregate", "DeclarativeOrderAggregate", "TestCreateEventAutoPopulation", "TestCreateEventReturnValue", "TestCreateEventApplication", "TestCreateEventOverrides", "TestCreateEventTenantContext", "TestBackwardCompatibility", "TestCreateEventEdgeCases", "TestCreateEventPerformance", "TestCreateEventWithDeclarativeAggregate"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
