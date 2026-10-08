---
id: REFACTOR-tests-unit-test_kafka_event_bus
title: Refactor and Decompose Legacy File test_kafka_event_bus.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-test_kafka_event_bus: Refactor Legacy File test_kafka_event_bus.py

## Summary
The grandfathered debt file `tests/unit/test_kafka_event_bus.py` contains 1858 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_kafka_event_bus_sample.py, test_kafka_event_bus_config.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/test_kafka_event_bus/` with submodules:
- `test_kafka_event_bus_sample.py`: SampleOrderCreated, SampleOrderShipped, SamplePaymentReceived, SampleKafkaEvent, SampleSubscriber, sample_event, OrderHandler, SyncHandler, event_registry, mock_producer, test_handlers_resolve_when_event_type_field_differs_from_class_name, test_retry_delay_comes_from_the_shared_policy, test_publish_sends_all_events_before_awaiting_any, TestKafkaEventBusStats, TestKafkaEventBus, TestKafkaNotAvailableError, _get_metric_value, _get_metric_attributes, _has_histogram_data, _get_histogram_sum, _get_gauge_value, TestKafkaEventBusMetricsInfrastructure, TestKafkaEventBusCounterMetrics, TestKafkaEventBusHistogramMetrics, TestKafkaEventBusGaugeMetrics, TestKafkaEventBusMetricsEdgeCases
- `test_kafka_event_bus_config.py`: TestKafkaEventBusConfig, TestKafkaEventBusConfigSecurity, TestKafkaEventBusConfigSSLContext, TestKafkaEventBusConfigSanitization, TestKafkaEventBusConfigProducerConsumer, TestKafkaEventBusMetricsConfig

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/test_kafka_event_bus.py (1858 lines):
  Submodule 'test_kafka_event_bus_sample.py' (~1135 lines):
    - [class] SampleOrderCreated (lines 35-40)
    - [class] SampleOrderShipped (lines 43-47)
    - [class] SamplePaymentReceived (lines 50-54)
    - [class] SampleKafkaEvent (lines 57-60)
    - [class] SampleSubscriber (lines 86-96)
    - [function] sample_event (lines 113-120)
    - [class] OrderHandler (lines 66-73)
    - [class] SyncHandler (lines 76-83)
    - [function] event_registry (lines 103-109)
    - [function] mock_producer (lines 124-133)
    - [function] test_handlers_resolve_when_event_type_field_differs_from_class_name (lines 136-151)
    - [function] test_retry_delay_comes_from_the_shared_policy (lines 154-167)
    - [function] test_publish_sends_all_events_before_awaiting_any (lines 170-194)
    - [class] TestKafkaEventBusStats (lines 715-755)
    - [class] TestKafkaEventBus (lines 764-932)
    - [class] TestKafkaNotAvailableError (lines 935-942)
    - [function] _get_metric_value (lines 972-995)
    - [function] _get_metric_attributes (lines 998-1021)
    - [function] _has_histogram_data (lines 1024-1046)
    - [function] _get_histogram_sum (lines 1049-1072)
    - [function] _get_gauge_value (lines 1075-1098)
    - [class] TestKafkaEventBusMetricsInfrastructure (lines 1107-1179)
    - [class] TestKafkaEventBusCounterMetrics (lines 1235-1385)
    - [class] TestKafkaEventBusHistogramMetrics (lines 1395-1461)
    - [class] TestKafkaEventBusGaugeMetrics (lines 1471-1636)
    - [class] TestKafkaEventBusMetricsEdgeCases (lines 1645-1858)
  Submodule 'test_kafka_event_bus_config.py' (~521 lines):
    - [class] TestKafkaEventBusConfig (lines 202-266)
    - [class] TestKafkaEventBusConfigSecurity (lines 274-400)
    - [class] TestKafkaEventBusConfigSSLContext (lines 408-488)
    - [class] TestKafkaEventBusConfigSanitization (lines 496-565)
    - [class] TestKafkaEventBusConfigProducerConsumer (lines 573-707)
    - [class] TestKafkaEventBusMetricsConfig (lines 1183-1225)
  Suggested barrel exports:
    from .test_kafka_event_bus_sample import SampleOrderCreated, SampleOrderShipped, SamplePaymentReceived, SampleKafkaEvent, SampleSubscriber, sample_event, OrderHandler, SyncHandler, event_registry, mock_producer, test_handlers_resolve_when_event_type_field_differs_from_class_name, test_retry_delay_comes_from_the_shared_policy, test_publish_sends_all_events_before_awaiting_any, TestKafkaEventBusStats, TestKafkaEventBus, TestKafkaNotAvailableError, _get_metric_value, _get_metric_attributes, _has_histogram_data, _get_histogram_sum, _get_gauge_value, TestKafkaEventBusMetricsInfrastructure, TestKafkaEventBusCounterMetrics, TestKafkaEventBusHistogramMetrics, TestKafkaEventBusGaugeMetrics, TestKafkaEventBusMetricsEdgeCases
    from .test_kafka_event_bus_config import TestKafkaEventBusConfig, TestKafkaEventBusConfigSecurity, TestKafkaEventBusConfigSSLContext, TestKafkaEventBusConfigSanitization, TestKafkaEventBusConfigProducerConsumer, TestKafkaEventBusMetricsConfig

    __all__ = ["SampleOrderCreated", "SampleOrderShipped", "SamplePaymentReceived", "SampleKafkaEvent", "SampleSubscriber", "sample_event", "OrderHandler", "SyncHandler", "event_registry", "mock_producer", "test_handlers_resolve_when_event_type_field_differs_from_class_name", "test_retry_delay_comes_from_the_shared_policy", "test_publish_sends_all_events_before_awaiting_any", "TestKafkaEventBusStats", "TestKafkaEventBus", "TestKafkaNotAvailableError", "_get_metric_value", "_get_metric_attributes", "_has_histogram_data", "_get_histogram_sum", "_get_gauge_value", "TestKafkaEventBusMetricsInfrastructure", "TestKafkaEventBusCounterMetrics", "TestKafkaEventBusHistogramMetrics", "TestKafkaEventBusGaugeMetrics", "TestKafkaEventBusMetricsEdgeCases", "TestKafkaEventBusConfig", "TestKafkaEventBusConfigSecurity", "TestKafkaEventBusConfigSSLContext", "TestKafkaEventBusConfigSanitization", "TestKafkaEventBusConfigProducerConsumer", "TestKafkaEventBusMetricsConfig"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
