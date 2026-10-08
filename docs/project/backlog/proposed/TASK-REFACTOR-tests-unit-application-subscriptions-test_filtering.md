---
id: REFACTOR-tests-unit-application-subscriptions-test_filtering
title: Refactor and Decompose Legacy File test_filtering.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_filtering: Refactor Legacy File test_filtering.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_filtering.py` contains 721 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_filtering_event.py, test_filtering_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_filtering/` with submodules:
- `test_filtering_event.py`: UserUpdatedEvent, TestEventFilterCreation, TestEventFilterExactMatching, TestEventFilterPatternMatching, TestEventFilterAggregateTypes, TestEventFilterCombined, TestEventFilterStatistics, TestEventFilterEventTypeNames, TestEventFilterRepr, TestEventFilterEdgeCases, TestEventFilterIntegration, PaymentReceived, UserRegistered, MockAllEventsSubscriber, payment_received, user_registered, TestFilterStats, TestUtilityFunctions
- `test_filtering_order.py`: OrderCreated, OrderShipped, OrderCancelled, MockOrderSubscriber, order_created, order_shipped, order_cancelled

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_filtering.py (721 lines):
  Submodule 'test_filtering_event.py' (~565 lines):
    - [class] UserUpdatedEvent (lines 65-69)
    - [class] TestEventFilterCreation (lines 194-267)
    - [class] TestEventFilterExactMatching (lines 273-304)
    - [class] TestEventFilterPatternMatching (lines 310-379)
    - [class] TestEventFilterAggregateTypes (lines 385-410)
    - [class] TestEventFilterCombined (lines 416-455)
    - [class] TestEventFilterStatistics (lines 461-507)
    - [class] TestEventFilterEventTypeNames (lines 513-543)
    - [class] TestEventFilterRepr (lines 549-583)
    - [class] TestEventFilterEdgeCases (lines 620-684)
    - [class] TestEventFilterIntegration (lines 690-721)
    - [class] PaymentReceived (lines 51-55)
    - [class] UserRegistered (lines 58-62)
    - [class] MockAllEventsSubscriber (lines 85-92)
    - [function] payment_received (lines 117-119)
    - [function] user_registered (lines 123-125)
    - [class] TestFilterStats (lines 131-188)
    - [class] TestUtilityFunctions (lines 589-614)
  Submodule 'test_filtering_order.py' (~32 lines):
    - [class] OrderCreated (lines 30-34)
    - [class] OrderShipped (lines 37-41)
    - [class] OrderCancelled (lines 44-48)
    - [class] MockOrderSubscriber (lines 75-82)
    - [function] order_created (lines 99-101)
    - [function] order_shipped (lines 105-107)
    - [function] order_cancelled (lines 111-113)
  Suggested barrel exports:
    from .test_filtering_event import UserUpdatedEvent, TestEventFilterCreation, TestEventFilterExactMatching, TestEventFilterPatternMatching, TestEventFilterAggregateTypes, TestEventFilterCombined, TestEventFilterStatistics, TestEventFilterEventTypeNames, TestEventFilterRepr, TestEventFilterEdgeCases, TestEventFilterIntegration, PaymentReceived, UserRegistered, MockAllEventsSubscriber, payment_received, user_registered, TestFilterStats, TestUtilityFunctions
    from .test_filtering_order import OrderCreated, OrderShipped, OrderCancelled, MockOrderSubscriber, order_created, order_shipped, order_cancelled

    __all__ = ["UserUpdatedEvent", "TestEventFilterCreation", "TestEventFilterExactMatching", "TestEventFilterPatternMatching", "TestEventFilterAggregateTypes", "TestEventFilterCombined", "TestEventFilterStatistics", "TestEventFilterEventTypeNames", "TestEventFilterRepr", "TestEventFilterEdgeCases", "TestEventFilterIntegration", "PaymentReceived", "UserRegistered", "MockAllEventsSubscriber", "payment_received", "user_registered", "TestFilterStats", "TestUtilityFunctions", "OrderCreated", "OrderShipped", "OrderCancelled", "MockOrderSubscriber", "order_created", "order_shipped", "order_cancelled"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
