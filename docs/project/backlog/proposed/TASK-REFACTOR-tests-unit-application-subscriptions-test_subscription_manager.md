---
id: REFACTOR-tests-unit-application-subscriptions-test_subscription_manager
title: Refactor and Decompose Legacy File test_subscription_manager.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_subscription_manager: Refactor Legacy File test_subscription_manager.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_subscription_manager.py` contains 1500 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_subscription_manager_event.py, test_subscription_manager_multiple.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_subscription_manager/` with submodules:
- `test_subscription_manager_event.py`: ManagerTestEvent, AnotherManagerEvent, event_store, event_bus, MockSubscriber, OrderProjection, CustomerProjection, checkpoint_repo, manager, subscriber, config, add_events_to_store, TestManagerInitialization, TestSubscriptionRegistration, TestSubscriptionRemoval, TestSubscriptionLookup, TestManagerStart, TestManagerStop, TestContextManager, TestHealthStatus, TestErrorHandling, TestLiveEvents, TestModuleImports, TestEdgeCases, TestSubscriptionIsolation, TestGetAllStatuses, TestConcurrentStartPerformance
- `test_subscription_manager_multiple.py`: TestMultipleSubscriptionsConcurrent, TestMultipleSubscriptionsHealth, TestMultipleSubscriptionsLifecycle

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_subscription_manager.py (1500 lines):
  Submodule 'test_subscription_manager_event.py' (~1091 lines):
    - [class] ManagerTestEvent (lines 44-48)
    - [class] AnotherManagerEvent (lines 52-56)
    - [function] event_store (lines 98-100)
    - [function] event_bus (lines 104-106)
    - [class] MockSubscriber (lines 62-79)
    - [class] OrderProjection (lines 82-85)
    - [class] CustomerProjection (lines 88-91)
    - [function] checkpoint_repo (lines 110-112)
    - [function] manager (lines 116-122)
    - [function] subscriber (lines 126-128)
    - [function] config (lines 132-137)
    - [function] add_events_to_store (lines 140-155)
    - [class] TestManagerInitialization (lines 161-183)
    - [class] TestSubscriptionRegistration (lines 189-262)
    - [class] TestSubscriptionRemoval (lines 268-314)
    - [class] TestSubscriptionLookup (lines 320-378)
    - [class] TestManagerStart (lines 384-473)
    - [class] TestManagerStop (lines 479-560)
    - [class] TestContextManager (lines 566-608)
    - [class] TestHealthStatus (lines 614-699)
    - [class] TestErrorHandling (lines 705-768)
    - [class] TestLiveEvents (lines 774-813)
    - [class] TestModuleImports (lines 819-832)
    - [class] TestEdgeCases (lines 838-945)
    - [class] TestSubscriptionIsolation (lines 1038-1160)
    - [class] TestGetAllStatuses (lines 1163-1258)
    - [class] TestConcurrentStartPerformance (lines 1436-1500)
  Submodule 'test_subscription_manager_multiple.py' (~256 lines):
    - [class] TestMultipleSubscriptionsConcurrent (lines 951-1035)
    - [class] TestMultipleSubscriptionsHealth (lines 1261-1345)
    - [class] TestMultipleSubscriptionsLifecycle (lines 1348-1433)
  Suggested barrel exports:
    from .test_subscription_manager_event import ManagerTestEvent, AnotherManagerEvent, event_store, event_bus, MockSubscriber, OrderProjection, CustomerProjection, checkpoint_repo, manager, subscriber, config, add_events_to_store, TestManagerInitialization, TestSubscriptionRegistration, TestSubscriptionRemoval, TestSubscriptionLookup, TestManagerStart, TestManagerStop, TestContextManager, TestHealthStatus, TestErrorHandling, TestLiveEvents, TestModuleImports, TestEdgeCases, TestSubscriptionIsolation, TestGetAllStatuses, TestConcurrentStartPerformance
    from .test_subscription_manager_multiple import TestMultipleSubscriptionsConcurrent, TestMultipleSubscriptionsHealth, TestMultipleSubscriptionsLifecycle

    __all__ = ["ManagerTestEvent", "AnotherManagerEvent", "event_store", "event_bus", "MockSubscriber", "OrderProjection", "CustomerProjection", "checkpoint_repo", "manager", "subscriber", "config", "add_events_to_store", "TestManagerInitialization", "TestSubscriptionRegistration", "TestSubscriptionRemoval", "TestSubscriptionLookup", "TestManagerStart", "TestManagerStop", "TestContextManager", "TestHealthStatus", "TestErrorHandling", "TestLiveEvents", "TestModuleImports", "TestEdgeCases", "TestSubscriptionIsolation", "TestGetAllStatuses", "TestConcurrentStartPerformance", "TestMultipleSubscriptionsConcurrent", "TestMultipleSubscriptionsHealth", "TestMultipleSubscriptionsLifecycle"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
