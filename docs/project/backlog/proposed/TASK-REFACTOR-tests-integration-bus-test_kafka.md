---
id: REFACTOR-tests-integration-bus-test_kafka
title: Refactor and Decompose Legacy File test_kafka.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-bus-test_kafka: Refactor Legacy File test_kafka.py

## Summary
The grandfathered debt file `tests/integration/bus/test_kafka.py` contains 2435 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_kafka_event.py, test_kafka_metrics.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/bus/test_kafka/` with submodules:
- `test_kafka_event.py`: kafka_event_bus_factory, kafka_event_bus, TestKafkaEventBusConnection, TestKafkaEventBusPublishing, TestKafkaEventBusSubscription, TestKafkaEventBusConsumerGroups, TestKafkaEventBusDLQ, TestKafkaEventBusStats, TestKafkaEventBusTopicInfo, TestKafkaEventBusShutdown, TestKafkaEventBusEdgeCases, event_registry, TestKafkaEventBusConformance, kafka_container, kafka_bootstrap_servers, sample_customer_id, TestKafkaPublishConsumeRoundTrip, _get_metric_value, _get_metric_attributes, _has_histogram_data, _get_histogram_sum, _get_gauge_value, TestKafkaHistogramIntegration, TestKafkaGaugeIntegration
- `test_kafka_metrics.py`: metrics_provider, metrics_setup, TestKafkaMetricsIntegration, TestKafkaMetricsPerformance, TestKafkaMetricsCardinality

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/bus/test_kafka.py (2435 lines):
  Submodule 'test_kafka_event.py' (~1704 lines):
    - [function] kafka_event_bus_factory (lines 131-168)
    - [function] kafka_event_bus (lines 172-190)
    - [class] TestKafkaEventBusConnection (lines 204-258)
    - [class] TestKafkaEventBusPublishing (lines 266-341)
    - [class] TestKafkaEventBusSubscription (lines 349-513)
    - [class] TestKafkaEventBusConsumerGroups (lines 676-852)
    - [class] TestKafkaEventBusDLQ (lines 860-1068)
    - [class] TestKafkaEventBusStats (lines 1076-1192)
    - [class] TestKafkaEventBusTopicInfo (lines 1200-1225)
    - [class] TestKafkaEventBusShutdown (lines 1233-1310)
    - [class] TestKafkaEventBusEdgeCases (lines 1318-1433)
    - [function] event_registry (lines 1663-1669)
    - [class] TestKafkaEventBusConformance (lines 2333-2435)
    - [function] kafka_container (lines 101-121)
    - [function] kafka_bootstrap_servers (lines 125-127)
    - [function] sample_customer_id (lines 194-196)
    - [class] TestKafkaPublishConsumeRoundTrip (lines 521-668)
    - [function] _get_metric_value (lines 1464-1487)
    - [function] _get_metric_attributes (lines 1490-1513)
    - [function] _has_histogram_data (lines 1516-1538)
    - [function] _get_histogram_sum (lines 1541-1564)
    - [function] _get_gauge_value (lines 1567-1590)
    - [class] TestKafkaHistogramIntegration (lines 1899-2011)
    - [class] TestKafkaGaugeIntegration (lines 2020-2130)
  Submodule 'test_kafka_metrics.py' (~439 lines):
    - [function] metrics_provider (lines 1605-1631)
    - [function] metrics_setup (lines 1637-1659)
    - [class] TestKafkaMetricsIntegration (lines 1678-1890)
    - [class] TestKafkaMetricsPerformance (lines 2142-2262)
    - [class] TestKafkaMetricsCardinality (lines 2271-2325)
  Suggested barrel exports:
    from .test_kafka_event import kafka_event_bus_factory, kafka_event_bus, TestKafkaEventBusConnection, TestKafkaEventBusPublishing, TestKafkaEventBusSubscription, TestKafkaEventBusConsumerGroups, TestKafkaEventBusDLQ, TestKafkaEventBusStats, TestKafkaEventBusTopicInfo, TestKafkaEventBusShutdown, TestKafkaEventBusEdgeCases, event_registry, TestKafkaEventBusConformance, kafka_container, kafka_bootstrap_servers, sample_customer_id, TestKafkaPublishConsumeRoundTrip, _get_metric_value, _get_metric_attributes, _has_histogram_data, _get_histogram_sum, _get_gauge_value, TestKafkaHistogramIntegration, TestKafkaGaugeIntegration
    from .test_kafka_metrics import metrics_provider, metrics_setup, TestKafkaMetricsIntegration, TestKafkaMetricsPerformance, TestKafkaMetricsCardinality

    __all__ = ["kafka_event_bus_factory", "kafka_event_bus", "TestKafkaEventBusConnection", "TestKafkaEventBusPublishing", "TestKafkaEventBusSubscription", "TestKafkaEventBusConsumerGroups", "TestKafkaEventBusDLQ", "TestKafkaEventBusStats", "TestKafkaEventBusTopicInfo", "TestKafkaEventBusShutdown", "TestKafkaEventBusEdgeCases", "event_registry", "TestKafkaEventBusConformance", "kafka_container", "kafka_bootstrap_servers", "sample_customer_id", "TestKafkaPublishConsumeRoundTrip", "_get_metric_value", "_get_metric_attributes", "_has_histogram_data", "_get_histogram_sum", "_get_gauge_value", "TestKafkaHistogramIntegration", "TestKafkaGaugeIntegration", "metrics_provider", "metrics_setup", "TestKafkaMetricsIntegration", "TestKafkaMetricsPerformance", "TestKafkaMetricsCardinality"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
