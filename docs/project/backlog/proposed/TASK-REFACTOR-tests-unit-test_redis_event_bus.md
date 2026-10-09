---
id: REFACTOR-tests-unit-test_redis_event_bus
title: Refactor and Decompose Legacy File test_redis_event_bus.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-test_redis_event_bus: Refactor Legacy File test_redis_event_bus.py

## Summary
The grandfathered debt file `tests/unit/test_redis_event_bus.py` contains 1248 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_redis_event_bus_sample.py, test_redis_event_bus_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/test_redis_event_bus/` with submodules:
- `test_redis_event_bus_sample.py`: SampleOrderCreated, SampleOrderShipped, SamplePaymentReceived, SampleSubscriber, SyncHandler, FailingHandler, event_registry, config, mock_redis, bus, TestRedisEventBusConfig, TestRedisEventBusStats, TestRedisEventBusConnection, TestRedisEventBusSubscription, TestRedisEventBusPublish, TestRedisEventBusDispatch, TestRedisEventBusDeserialization, TestRedisEventBusMessageProcessing, TestRedisEventBusDLQ, TestRedisEventBusPendingRecovery, TestRedisEventBusStreamInfo, TestRedisEventBusStatistics, TestRedisEventBusShutdown, TestRedisEventBusErrorHandling, TestRedisEventBusIntegration
- `test_redis_event_bus_order.py`: OrderHandler

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/test_redis_event_bus.py (1248 lines):
  Submodule 'test_redis_event_bus_sample.py' (~1105 lines):
    - [class] SampleOrderCreated (lines 34-39)
    - [class] SampleOrderShipped (lines 42-46)
    - [class] SamplePaymentReceived (lines 49-53)
    - [class] SampleSubscriber (lines 86-96)
    - [class] SyncHandler (lines 69-76)
    - [class] FailingHandler (lines 79-83)
    - [function] event_registry (lines 103-109)
    - [function] config (lines 113-126)
    - [function] mock_redis (lines 130-158)
    - [function] bus (lines 162-177)
    - [class] TestRedisEventBusConfig (lines 183-240)
    - [class] TestRedisEventBusStats (lines 243-266)
    - [class] TestRedisEventBusConnection (lines 272-349)
    - [class] TestRedisEventBusSubscription (lines 355-435)
    - [class] TestRedisEventBusPublish (lines 441-549)
    - [class] TestRedisEventBusDispatch (lines 555-704)
    - [class] TestRedisEventBusDeserialization (lines 710-747)
    - [class] TestRedisEventBusMessageProcessing (lines 753-833)
    - [class] TestRedisEventBusDLQ (lines 839-908)
    - [class] TestRedisEventBusPendingRecovery (lines 914-1043)
    - [class] TestRedisEventBusStreamInfo (lines 1049-1081)
    - [class] TestRedisEventBusStatistics (lines 1087-1106)
    - [class] TestRedisEventBusShutdown (lines 1112-1120)
    - [class] TestRedisEventBusErrorHandling (lines 1126-1145)
    - [class] TestRedisEventBusIntegration (lines 1151-1248)
  Submodule 'test_redis_event_bus_order.py' (~8 lines):
    - [class] OrderHandler (lines 59-66)
  Suggested barrel exports:
    from .test_redis_event_bus_sample import SampleOrderCreated, SampleOrderShipped, SamplePaymentReceived, SampleSubscriber, SyncHandler, FailingHandler, event_registry, config, mock_redis, bus, TestRedisEventBusConfig, TestRedisEventBusStats, TestRedisEventBusConnection, TestRedisEventBusSubscription, TestRedisEventBusPublish, TestRedisEventBusDispatch, TestRedisEventBusDeserialization, TestRedisEventBusMessageProcessing, TestRedisEventBusDLQ, TestRedisEventBusPendingRecovery, TestRedisEventBusStreamInfo, TestRedisEventBusStatistics, TestRedisEventBusShutdown, TestRedisEventBusErrorHandling, TestRedisEventBusIntegration
    from .test_redis_event_bus_order import OrderHandler

    __all__ = ["SampleOrderCreated", "SampleOrderShipped", "SamplePaymentReceived", "SampleSubscriber", "SyncHandler", "FailingHandler", "event_registry", "config", "mock_redis", "bus", "TestRedisEventBusConfig", "TestRedisEventBusStats", "TestRedisEventBusConnection", "TestRedisEventBusSubscription", "TestRedisEventBusPublish", "TestRedisEventBusDispatch", "TestRedisEventBusDeserialization", "TestRedisEventBusMessageProcessing", "TestRedisEventBusDLQ", "TestRedisEventBusPendingRecovery", "TestRedisEventBusStreamInfo", "TestRedisEventBusStatistics", "TestRedisEventBusShutdown", "TestRedisEventBusErrorHandling", "TestRedisEventBusIntegration", "OrderHandler"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
