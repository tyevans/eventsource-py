---
id: REFACTOR-tests-unit-application-subscriptions-test_subscriber
title: Refactor and Decompose Legacy File test_subscriber.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_subscriber: Refactor Legacy File test_subscriber.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_subscriber.py` contains 829 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_subscriber_protocol.py, test_subscriber_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_subscriber/` with submodules:
- `test_subscriber_protocol.py`: TestSubscriberProtocol, TestSyncSubscriberProtocol, TestBatchSubscriberProtocol, TestSubscriberProtocolCompatibility, TestSupportsBatchHandling, TestGetSubscribedEventTypes, TestBaseSubscriber, TestBatchAwareSubscriber, TestFilteringSubscriber, TestSubscriberDuckTyping
- `test_subscriber_order.py`: OrderCreated, OrderShipped, OrderCancelled

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_subscriber.py (829 lines):
  Submodule 'test_subscriber_protocol.py' (~755 lines):
    - [class] TestSubscriberProtocol (lines 55-136)
    - [class] TestSyncSubscriberProtocol (lines 139-185)
    - [class] TestBatchSubscriberProtocol (lines 188-256)
    - [class] TestSubscriberProtocolCompatibility (lines 731-762)
    - [class] TestSupportsBatchHandling (lines 259-286)
    - [class] TestGetSubscribedEventTypes (lines 289-332)
    - [class] TestBaseSubscriber (lines 335-442)
    - [class] TestBatchAwareSubscriber (lines 445-603)
    - [class] TestFilteringSubscriber (lines 606-728)
    - [class] TestSubscriberDuckTyping (lines 767-829)
  Submodule 'test_subscriber_order.py' (~15 lines):
    - [class] OrderCreated (lines 34-38)
    - [class] OrderShipped (lines 41-45)
    - [class] OrderCancelled (lines 48-52)
  Suggested barrel exports:
    from .test_subscriber_protocol import TestSubscriberProtocol, TestSyncSubscriberProtocol, TestBatchSubscriberProtocol, TestSubscriberProtocolCompatibility, TestSupportsBatchHandling, TestGetSubscribedEventTypes, TestBaseSubscriber, TestBatchAwareSubscriber, TestFilteringSubscriber, TestSubscriberDuckTyping
    from .test_subscriber_order import OrderCreated, OrderShipped, OrderCancelled

    __all__ = ["TestSubscriberProtocol", "TestSyncSubscriberProtocol", "TestBatchSubscriberProtocol", "TestSubscriberProtocolCompatibility", "TestSupportsBatchHandling", "TestGetSubscribedEventTypes", "TestBaseSubscriber", "TestBatchAwareSubscriber", "TestFilteringSubscriber", "TestSubscriberDuckTyping", "OrderCreated", "OrderShipped", "OrderCancelled"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
