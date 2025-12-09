# Kafka Event Bus Metrics Guide

This guide covers the OpenTelemetry metrics available for the Kafka Event Bus, including configuration, metric reference, PromQL queries, and alerting recommendations.

## Overview

The Kafka Event Bus emits OpenTelemetry metrics for monitoring throughput, latency, and health of your event processing pipeline. Metrics complement the existing tracing support for comprehensive observability.

When metrics are enabled, you gain visibility into:

- **Throughput**: Messages published and consumed per second
- **Latency**: Publish, consume, and handler execution times
- **Errors**: Handler failures, publish errors, and DLQ routing
- **Health**: Connection status, consumer lag, and rebalance frequency

## Prerequisites

### Install Dependencies

Install the eventsource package with Kafka and telemetry extras:

```bash
pip install "eventsource-py[kafka,telemetry]"
```

This installs:
- `aiokafka` - Async Kafka client
- `opentelemetry-api` - OpenTelemetry API
- `opentelemetry-sdk` - OpenTelemetry SDK

### Configure OpenTelemetry Exporter

Set up an OTLP exporter or Prometheus exporter to collect metrics:

**OTLP Export (Jaeger, Tempo, etc.):**

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Set up OTLP export to collector
reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://otel-collector:4317"),
    export_interval_millis=60000,  # Export every 60 seconds
)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
```

**Prometheus Export:**

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

# Start Prometheus metrics server
start_http_server(port=8000, addr="0.0.0.0")

# Set up Prometheus reader
reader = PrometheusMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
```

## Configuration

Enable metrics via `KafkaEventBusConfig`:

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="events",
    consumer_group="projections",
    enable_metrics=True,   # Enable OpenTelemetry metrics (default: True)
    enable_tracing=True,   # Enable OpenTelemetry tracing (default: True)
)

bus = KafkaEventBus(config=config)
```

### Disabling Metrics

To disable metrics while keeping tracing:

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="events",
    consumer_group="projections",
    enable_metrics=False,  # Disable metrics
    enable_tracing=True,   # Keep tracing
)
```

Metrics are also automatically disabled if the OpenTelemetry SDK is not installed.

---

## Metrics Reference

All metrics use the `kafka.eventbus.` prefix and follow OpenTelemetry semantic conventions for messaging systems.

### Counter Metrics

| Metric Name | Unit | Description | Attributes |
|-------------|------|-------------|------------|
| `kafka.eventbus.messages.published` | messages | Total messages successfully published to Kafka | `messaging.system`, `messaging.destination`, `event.type` |
| `kafka.eventbus.messages.consumed` | messages | Total messages consumed from Kafka | `messaging.system`, `messaging.destination`, `messaging.kafka.partition`, `event.type` |
| `kafka.eventbus.handler.invocations` | invocations | Total handler invocations (successful) | `handler.name`, `event.type` |
| `kafka.eventbus.handler.errors` | errors | Total handler errors/exceptions | `handler.name`, `event.type`, `error.type` |
| `kafka.eventbus.messages.dlq` | messages | Messages sent to dead letter queue | `dlq.reason`, `error.type` |
| `kafka.eventbus.connection.errors` | errors | Connection errors (connect/consume failures) | `error.type` |
| `kafka.eventbus.reconnections` | reconnections | Reconnection attempts | - |
| `kafka.eventbus.rebalances` | rebalances | Consumer group rebalances | `messaging.kafka.consumer_group` |
| `kafka.eventbus.publish.errors` | errors | Publish failures | `messaging.system`, `messaging.destination`, `event.type`, `error.type` |

### Histogram Metrics

| Metric Name | Unit | Description | Attributes |
|-------------|------|-------------|------------|
| `kafka.eventbus.publish.duration` | ms | Time to publish a batch of messages | `messaging.destination` |
| `kafka.eventbus.consume.duration` | ms | Time to process a consumed message | `messaging.destination` |
| `kafka.eventbus.handler.duration` | ms | Individual handler execution time | `handler.name`, `event.type` |
| `kafka.eventbus.batch.size` | messages | Number of messages per publish batch | - |

### Observable Gauge Metrics

| Metric Name | Unit | Description | Attributes |
|-------------|------|-------------|------------|
| `kafka.eventbus.connections.active` | connections | Connection status (1=connected, 0=disconnected) | `messaging.kafka.consumer_group` |
| `kafka.eventbus.consumer.lag` | messages | Messages behind (lag) per partition | `messaging.kafka.partition`, `messaging.kafka.consumer_group`, `messaging.destination` |

### Attribute Reference

| Attribute | Description | Example Values |
|-----------|-------------|----------------|
| `messaging.system` | Messaging system identifier | `"kafka"` |
| `messaging.destination` | Topic name | `"events.stream"` |
| `messaging.kafka.partition` | Partition number | `0`, `1`, `2` |
| `messaging.kafka.consumer_group` | Consumer group ID | `"projections"` |
| `event.type` | Event class name | `"OrderCreated"`, `"PaymentReceived"` |
| `handler.name` | Handler class or function name | `"OrderProjection"`, `"send_notification"` |
| `error.type` | Exception class name | `"ValidationError"`, `"TimeoutError"` |
| `dlq.reason` | Reason for DLQ routing | `"max_retries_exceeded"` |

---

## Example PromQL Queries

### Throughput Metrics

```promql
# Publish rate (messages per second)
rate(kafka_eventbus_messages_published_total[5m])

# Consume rate by event type
sum(rate(kafka_eventbus_messages_consumed_total[5m])) by (event_type)

# Handler throughput by handler name
sum(rate(kafka_eventbus_handler_invocations_total[5m])) by (handler_name)

# Total events processed per minute
sum(increase(kafka_eventbus_messages_consumed_total[1m]))
```

### Latency Metrics

```promql
# P50 publish latency
histogram_quantile(0.50, rate(kafka_eventbus_publish_duration_bucket[5m]))

# P95 publish latency
histogram_quantile(0.95, rate(kafka_eventbus_publish_duration_bucket[5m]))

# P99 handler latency by handler
histogram_quantile(0.99,
  sum(rate(kafka_eventbus_handler_duration_bucket[5m])) by (le, handler_name)
)

# Average consume latency
rate(kafka_eventbus_consume_duration_sum[5m]) / rate(kafka_eventbus_consume_duration_count[5m])

# Average handler duration by event type
sum(rate(kafka_eventbus_handler_duration_sum[5m])) by (event_type)
/
sum(rate(kafka_eventbus_handler_duration_count[5m])) by (event_type)
```

### Error Rate Metrics

```promql
# Overall error rate (percentage)
sum(rate(kafka_eventbus_handler_errors_total[5m]))
/
sum(rate(kafka_eventbus_handler_invocations_total[5m]))
* 100

# Handler error rate by handler
sum(rate(kafka_eventbus_handler_errors_total[5m])) by (handler_name)
/
sum(rate(kafka_eventbus_handler_invocations_total[5m])) by (handler_name)
* 100

# DLQ rate (messages per minute)
rate(kafka_eventbus_messages_dlq_total[5m]) * 60

# Publish error rate
rate(kafka_eventbus_publish_errors_total[5m])

# Errors by type
sum(rate(kafka_eventbus_handler_errors_total[5m])) by (error_type)
```

### Consumer Lag Metrics

```promql
# Total lag across all partitions
sum(kafka_eventbus_consumer_lag) by (messaging_kafka_consumer_group)

# Maximum lag per partition
max(kafka_eventbus_consumer_lag) by (messaging_kafka_partition)

# Lag trend (positive = falling behind, negative = catching up)
deriv(kafka_eventbus_consumer_lag[10m])

# Partitions with high lag (> 1000 messages)
kafka_eventbus_consumer_lag > 1000
```

### Connection Health Metrics

```promql
# Connection uptime percentage
avg_over_time(kafka_eventbus_connections_active[1h]) * 100

# Disconnection events
changes(kafka_eventbus_connections_active[1h])

# Reconnection rate (per hour)
increase(kafka_eventbus_reconnections_total[1h])

# Rebalance frequency (per hour)
increase(kafka_eventbus_rebalances_total[1h])
```

---

## Alerting Recommendations

### Prometheus Alerting Rules

```yaml
groups:
  - name: kafka-eventbus-alerts
    rules:
      # High consumer lag
      - alert: KafkaEventBusConsumerLagHigh
        expr: kafka_eventbus_consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag detected"
          description: >
            Consumer group {{ $labels.messaging_kafka_consumer_group }}
            partition {{ $labels.messaging_kafka_partition }}
            has {{ $value }} messages lag.

      # Critical consumer lag
      - alert: KafkaEventBusConsumerLagCritical
        expr: kafka_eventbus_consumer_lag > 100000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Critical consumer lag - consumers falling behind"
          description: >
            Consumer group {{ $labels.messaging_kafka_consumer_group }}
            has critical lag of {{ $value }} messages.

      # Connection lost
      - alert: KafkaEventBusDisconnected
        expr: kafka_eventbus_connections_active == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka event bus disconnected"
          description: >
            Consumer group {{ $labels.messaging_kafka_consumer_group }}
            has lost connection to Kafka.

      # High handler error rate
      - alert: KafkaEventBusHandlerErrorRateHigh
        expr: |
          (
            sum(rate(kafka_eventbus_handler_errors_total[5m])) by (handler_name)
            /
            sum(rate(kafka_eventbus_handler_invocations_total[5m])) by (handler_name)
          ) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Handler error rate exceeds 5%"
          description: >
            Handler {{ $labels.handler_name }} has error rate of
            {{ $value | humanizePercentage }}.

      # High publish latency
      - alert: KafkaEventBusPublishLatencyHigh
        expr: |
          histogram_quantile(0.95, rate(kafka_eventbus_publish_duration_bucket[5m])) > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P95 publish latency exceeds 1 second"
          description: >
            Publish latency P95 is {{ $value | humanizeDuration }}.

      # DLQ accumulating
      - alert: KafkaEventBusDLQAccumulating
        expr: increase(kafka_eventbus_messages_dlq_total[1h]) > 100
        labels:
          severity: warning
        annotations:
          summary: "Dead letter queue accumulating messages"
          description: >
            {{ $value }} messages sent to DLQ in the last hour.

      # Frequent rebalances
      - alert: KafkaEventBusFrequentRebalances
        expr: increase(kafka_eventbus_rebalances_total[1h]) > 10
        labels:
          severity: warning
        annotations:
          summary: "Frequent consumer group rebalances"
          description: >
            Consumer group {{ $labels.messaging_kafka_consumer_group }}
            has rebalanced {{ $value }} times in the last hour.
```

### Alert Severity Guidelines

| Severity | Condition | Action |
|----------|-----------|--------|
| **Critical** | Connection lost, lag > 100k | Immediate investigation required |
| **Warning** | Error rate > 5%, lag > 10k, latency > 1s | Investigate within hours |
| **Info** | DLQ messages, rebalances | Review during business hours |

---

## Grafana Dashboard Examples

### Overview Panel (Stat)

```json
{
  "title": "Messages Published/sec",
  "type": "stat",
  "targets": [{
    "expr": "sum(rate(kafka_eventbus_messages_published_total[5m]))",
    "legendFormat": "Published/sec"
  }]
}
```

### Throughput Graph (Time Series)

```json
{
  "title": "Message Throughput",
  "type": "timeseries",
  "targets": [
    {
      "expr": "sum(rate(kafka_eventbus_messages_published_total[5m]))",
      "legendFormat": "Published"
    },
    {
      "expr": "sum(rate(kafka_eventbus_messages_consumed_total[5m]))",
      "legendFormat": "Consumed"
    }
  ]
}
```

### Latency Heatmap

```json
{
  "title": "Handler Latency Distribution",
  "type": "heatmap",
  "targets": [{
    "expr": "sum(rate(kafka_eventbus_handler_duration_bucket[5m])) by (le)",
    "format": "heatmap"
  }]
}
```

### Consumer Lag Gauge

```json
{
  "title": "Consumer Lag by Partition",
  "type": "gauge",
  "targets": [{
    "expr": "kafka_eventbus_consumer_lag",
    "legendFormat": "Partition {{ messaging_kafka_partition }}"
  }],
  "fieldConfig": {
    "defaults": {
      "thresholds": {
        "steps": [
          {"color": "green", "value": 0},
          {"color": "yellow", "value": 1000},
          {"color": "red", "value": 10000}
        ]
      }
    }
  }
}
```

### Error Rate Table

```json
{
  "title": "Handler Error Rates",
  "type": "table",
  "targets": [{
    "expr": "sum(rate(kafka_eventbus_handler_errors_total[5m])) by (handler_name) / sum(rate(kafka_eventbus_handler_invocations_total[5m])) by (handler_name) * 100",
    "format": "table",
    "instant": true
  }],
  "transformations": [{
    "id": "organize",
    "options": {
      "renameByName": {
        "Value": "Error Rate %",
        "handler_name": "Handler"
      }
    }
  }]
}
```

---

## Performance Considerations

### Overhead

- Metrics add approximately **< 5%** overhead to event processing
- Counter and histogram operations are O(1)
- Observable gauges are collected at SDK collection interval (default: 60 seconds)
- Metrics export happens asynchronously, not blocking event processing

### Cardinality Guidelines

Metric cardinality is bounded by:

| Attribute | Cardinality Source | Typical Range |
|-----------|-------------------|---------------|
| `event.type` | Registered event types | 10-100 |
| `handler.name` | Subscribed handlers | 5-50 |
| `messaging.kafka.partition` | Topic partition count | 1-100 |
| `error.type` | Exception class names | 5-20 |

**Best Practices:**

- Avoid adding high-cardinality attributes like event IDs or aggregate IDs
- Keep the number of unique event types manageable
- Monitor cardinality in your metrics backend
- Use recording rules in Prometheus to pre-aggregate high-cardinality metrics

### Memory Usage

The KafkaEventBusMetrics class creates instruments once at initialization. Each instrument consumes minimal memory:

- Counters: ~100 bytes per unique label set
- Histograms: ~500 bytes per unique label set (depending on bucket count)
- Gauges: Callback-based, no stored state

---

## Troubleshooting

### Metrics Not Appearing

1. **Check OpenTelemetry is installed:**
   ```python
   from eventsource.bus.kafka import OTEL_AVAILABLE
   print(f"OpenTelemetry available: {OTEL_AVAILABLE}")
   ```

2. **Verify metrics are enabled:**
   ```python
   print(f"Metrics enabled: {bus.config.enable_metrics}")
   ```

3. **Check meter provider is configured:**
   ```python
   from opentelemetry import metrics
   provider = metrics.get_meter_provider()
   print(f"Meter provider: {type(provider)}")
   ```

### Consumer Lag Always Zero

Consumer lag requires:
- Consumer must be actively consuming (`start_consuming()` called)
- Consumer must have partition assignments
- Gauge callback runs at SDK collection interval

### Handler Duration Not Recording

Handler duration is only recorded when:
- Handler invocation completes (success or error)
- Metrics are enabled before `start_consuming()` is called

---

## Integration with Existing Observability

### Correlation with Tracing

When both tracing and metrics are enabled, you can correlate:

- Use trace ID from spans to investigate specific slow operations
- Aggregate latency metrics show overall health
- Handler duration histogram shows P50/P95/P99 across all requests

### Combining with Application Metrics

```python
from opentelemetry import metrics

# Get the same meter used by KafkaEventBus
meter = metrics.get_meter("eventsource.bus.kafka")

# Add custom application metrics
orders_counter = meter.create_counter(
    name="app.orders.created",
    description="Orders created via event processing",
)

async def handle_order_created(event: OrderCreated):
    # Process order...
    orders_counter.add(1, {"region": event.region})
```

---

## See Also

- [Kafka Event Bus Guide](kafka-event-bus.md) - Full Kafka Event Bus documentation
- [Observability API Reference](../api/observability.md) - Tracing utilities
- [Production Guide](production.md) - Production deployment recommendations
- [Installation Guide](../installation.md) - Installing optional dependencies
