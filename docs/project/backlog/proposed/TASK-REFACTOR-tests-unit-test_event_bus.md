---
id: REFACTOR-tests-unit-test_event_bus
title: Refactor and Decompose Legacy File test_event_bus.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-test_event_bus: Refactor Legacy File test_event_bus.py

## Summary
The grandfathered debt file `tests/unit/test_event_bus.py` contains 988 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_event_bus_in.py, test_event_bus_handler.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/test_event_bus/` with submodules:
- `test_event_bus_in.py`: TestInMemoryEventBusBasicPublishing, TestInMemoryEventBusMultipleHandlers, TestInMemoryEventBusErrorHandling, TestInMemoryEventBusSyncAsyncSupport, TestInMemoryEventBusWildcardSubscriptions, TestInMemoryEventBusSubscribeAll, TestInMemoryEventBusUnsubscribe, TestInMemoryEventBusBackgroundPublishing, TestInMemoryEventBusSubscriberManagement, TestInMemoryEventBusShutdown, TestInMemoryEventBusStatistics, TestInMemoryEventBusThreadSafety, OrderCreated, OrderShipped, OrderCancelled, SelectiveSubscriber, event_bus, sample_order_created, sample_order_shipped, sample_order_cancelled, TestEventSubscriberProtocol, TestEventOrdering
- `test_event_bus_handler.py`: RecordingHandler, SyncRecordingHandler, FailingHandler, SlowHandler, SampleAsyncEventHandler, TestEventHandlerProtocol, TestAsyncEventHandlerABC, TestInvalidHandlerTypes

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/test_event_bus.py (988 lines):
  Submodule 'test_event_bus_in.py' (~693 lines):
    - [class] TestInMemoryEventBusBasicPublishing (lines 264-314)
    - [class] TestInMemoryEventBusMultipleHandlers (lines 322-363)
    - [class] TestInMemoryEventBusErrorHandling (lines 371-415)
    - [class] TestInMemoryEventBusSyncAsyncSupport (lines 423-500)
    - [class] TestInMemoryEventBusWildcardSubscriptions (lines 508-578)
    - [class] TestInMemoryEventBusSubscribeAll (lines 586-621)
    - [class] TestInMemoryEventBusUnsubscribe (lines 629-664)
    - [class] TestInMemoryEventBusBackgroundPublishing (lines 672-729)
    - [class] TestInMemoryEventBusSubscriberManagement (lines 737-781)
    - [class] TestInMemoryEventBusShutdown (lines 789-833)
    - [class] TestInMemoryEventBusStatistics (lines 841-875)
    - [class] TestInMemoryEventBusThreadSafety (lines 883-926)
    - [class] OrderCreated (lines 27-32)
    - [class] OrderShipped (lines 35-40)
    - [class] OrderCancelled (lines 43-48)
    - [class] SelectiveSubscriber (lines 108-121)
    - [function] event_bus (lines 146-148)
    - [function] sample_order_created (lines 152-158)
    - [function] sample_order_shipped (lines 162-168)
    - [function] sample_order_cancelled (lines 172-178)
    - [class] TestEventSubscriberProtocol (lines 205-219)
    - [class] TestEventOrdering (lines 953-988)
  Submodule 'test_event_bus_handler.py' (~112 lines):
    - [class] RecordingHandler (lines 56-66)
    - [class] SyncRecordingHandler (lines 69-79)
    - [class] FailingHandler (lines 82-92)
    - [class] SlowHandler (lines 95-105)
    - [class] SampleAsyncEventHandler (lines 124-137)
    - [class] TestEventHandlerProtocol (lines 186-197)
    - [class] TestAsyncEventHandlerABC (lines 227-256)
    - [class] TestInvalidHandlerTypes (lines 934-945)
  Suggested barrel exports:
    from .test_event_bus_in import TestInMemoryEventBusBasicPublishing, TestInMemoryEventBusMultipleHandlers, TestInMemoryEventBusErrorHandling, TestInMemoryEventBusSyncAsyncSupport, TestInMemoryEventBusWildcardSubscriptions, TestInMemoryEventBusSubscribeAll, TestInMemoryEventBusUnsubscribe, TestInMemoryEventBusBackgroundPublishing, TestInMemoryEventBusSubscriberManagement, TestInMemoryEventBusShutdown, TestInMemoryEventBusStatistics, TestInMemoryEventBusThreadSafety, OrderCreated, OrderShipped, OrderCancelled, SelectiveSubscriber, event_bus, sample_order_created, sample_order_shipped, sample_order_cancelled, TestEventSubscriberProtocol, TestEventOrdering
    from .test_event_bus_handler import RecordingHandler, SyncRecordingHandler, FailingHandler, SlowHandler, SampleAsyncEventHandler, TestEventHandlerProtocol, TestAsyncEventHandlerABC, TestInvalidHandlerTypes

    __all__ = ["TestInMemoryEventBusBasicPublishing", "TestInMemoryEventBusMultipleHandlers", "TestInMemoryEventBusErrorHandling", "TestInMemoryEventBusSyncAsyncSupport", "TestInMemoryEventBusWildcardSubscriptions", "TestInMemoryEventBusSubscribeAll", "TestInMemoryEventBusUnsubscribe", "TestInMemoryEventBusBackgroundPublishing", "TestInMemoryEventBusSubscriberManagement", "TestInMemoryEventBusShutdown", "TestInMemoryEventBusStatistics", "TestInMemoryEventBusThreadSafety", "OrderCreated", "OrderShipped", "OrderCancelled", "SelectiveSubscriber", "event_bus", "sample_order_created", "sample_order_shipped", "sample_order_cancelled", "TestEventSubscriberProtocol", "TestEventOrdering", "RecordingHandler", "SyncRecordingHandler", "FailingHandler", "SlowHandler", "SampleAsyncEventHandler", "TestEventHandlerProtocol", "TestAsyncEventHandlerABC", "TestInvalidHandlerTypes"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
