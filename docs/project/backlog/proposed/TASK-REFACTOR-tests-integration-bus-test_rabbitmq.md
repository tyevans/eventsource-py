---
id: REFACTOR-tests-integration-bus-test_rabbitmq
title: Refactor and Decompose Legacy File test_rabbitmq.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-bus-test_rabbitmq: Refactor Legacy File test_rabbitmq.py

## Summary
The grandfathered debt file `tests/integration/bus/test_rabbitmq.py` contains 3114 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_rabbitmq_event.py, test_rabbitmq_reliability.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/bus/test_rabbitmq/` with submodules:
- `test_rabbitmq_event.py`: rabbitmq_event_bus_factory, rabbitmq_event_bus, TestRabbitMQEventBusConnection, TestRabbitMQEventBusPublishing, TestRabbitMQEventBusSubscription, TestRabbitMQEventBusDLQ, TestRabbitMQEventBusStats, TestRabbitMQEventBusEdgeCases, TestRabbitMQEventBusPerformance, TestRabbitMQEventBusConformance, rabbitmq_container, rabbitmq_connection_url, sample_customer_id, TestRabbitMQPublishConsumeRoundTrip, TestRabbitMQMultipleConsumerGroups, TestAdvancedExchangeTypes, TestAdvancedMultipleConsumers, TestAdvancedBatchPublishing
- `test_rabbitmq_reliability.py`: TestRabbitMQReliabilityDLQ, TestRabbitMQReliabilityRetry, TestRabbitMQReliabilityShutdown, TestRabbitMQReliabilityStats, TestRabbitMQReliabilityQueueInfo

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/bus/test_rabbitmq.py (3114 lines):
  Submodule 'test_rabbitmq_event.py' (~2138 lines):
    - [function] rabbitmq_event_bus_factory (lines 120-154)
    - [function] rabbitmq_event_bus (lines 158-181)
    - [class] TestRabbitMQEventBusConnection (lines 195-295)
    - [class] TestRabbitMQEventBusPublishing (lines 303-404)
    - [class] TestRabbitMQEventBusSubscription (lines 412-576)
    - [class] TestRabbitMQEventBusDLQ (lines 739-836)
    - [class] TestRabbitMQEventBusStats (lines 1008-1145)
    - [class] TestRabbitMQEventBusEdgeCases (lines 1153-1285)
    - [class] TestRabbitMQEventBusPerformance (lines 1293-1371)
    - [class] TestRabbitMQEventBusConformance (lines 3012-3114)
    - [function] rabbitmq_container (lines 83-108)
    - [function] rabbitmq_connection_url (lines 112-116)
    - [function] sample_customer_id (lines 185-187)
    - [class] TestRabbitMQPublishConsumeRoundTrip (lines 584-731)
    - [class] TestRabbitMQMultipleConsumerGroups (lines 844-1000)
    - [class] TestAdvancedExchangeTypes (lines 2175-2511)
    - [class] TestAdvancedMultipleConsumers (lines 2519-2740)
    - [class] TestAdvancedBatchPublishing (lines 2748-3009)
  Submodule 'test_rabbitmq_reliability.py' (~761 lines):
    - [class] TestRabbitMQReliabilityDLQ (lines 1379-1635)
    - [class] TestRabbitMQReliabilityRetry (lines 1643-1762)
    - [class] TestRabbitMQReliabilityShutdown (lines 1770-1859)
    - [class] TestRabbitMQReliabilityStats (lines 1867-2052)
    - [class] TestRabbitMQReliabilityQueueInfo (lines 2060-2167)
  Suggested barrel exports:
    from .test_rabbitmq_event import rabbitmq_event_bus_factory, rabbitmq_event_bus, TestRabbitMQEventBusConnection, TestRabbitMQEventBusPublishing, TestRabbitMQEventBusSubscription, TestRabbitMQEventBusDLQ, TestRabbitMQEventBusStats, TestRabbitMQEventBusEdgeCases, TestRabbitMQEventBusPerformance, TestRabbitMQEventBusConformance, rabbitmq_container, rabbitmq_connection_url, sample_customer_id, TestRabbitMQPublishConsumeRoundTrip, TestRabbitMQMultipleConsumerGroups, TestAdvancedExchangeTypes, TestAdvancedMultipleConsumers, TestAdvancedBatchPublishing
    from .test_rabbitmq_reliability import TestRabbitMQReliabilityDLQ, TestRabbitMQReliabilityRetry, TestRabbitMQReliabilityShutdown, TestRabbitMQReliabilityStats, TestRabbitMQReliabilityQueueInfo

    __all__ = ["rabbitmq_event_bus_factory", "rabbitmq_event_bus", "TestRabbitMQEventBusConnection", "TestRabbitMQEventBusPublishing", "TestRabbitMQEventBusSubscription", "TestRabbitMQEventBusDLQ", "TestRabbitMQEventBusStats", "TestRabbitMQEventBusEdgeCases", "TestRabbitMQEventBusPerformance", "TestRabbitMQEventBusConformance", "rabbitmq_container", "rabbitmq_connection_url", "sample_customer_id", "TestRabbitMQPublishConsumeRoundTrip", "TestRabbitMQMultipleConsumerGroups", "TestAdvancedExchangeTypes", "TestAdvancedMultipleConsumers", "TestAdvancedBatchPublishing", "TestRabbitMQReliabilityDLQ", "TestRabbitMQReliabilityRetry", "TestRabbitMQReliabilityShutdown", "TestRabbitMQReliabilityStats", "TestRabbitMQReliabilityQueueInfo"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
