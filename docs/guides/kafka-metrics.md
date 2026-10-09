# Kafka Event Bus Metrics & Observability

`KafkaEventBus` provides native OpenTelemetry metrics for throughput, latency, consumer group lag, rebalances, and dead-letter queues. These metrics follow OpenTelemetry semantic conventions for messaging systems and allow monitoring production event streaming with Prometheus and Grafana.

## Enabling Metrics

Metrics are enabled by default on `KafkaEventBusConfig`. Ensure that OpenTelemetry is installed (`pip install "eventsource-py[all]"`) and your OpenTelemetry SDK `MeterProvider` is configured:

```python
from eventsource.adapters.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="events",
    consumer_group="order-projections",
    enable_metrics=True,  # Default: True
)

bus = KafkaEventBus(config=config, event_registry=registry)
await bus.connect()
await bus.start_consuming()
```

When `enable_metrics=True` and OpenTelemetry is present, `KafkaEventBus` registers metric instruments with the global `MeterProvider` under the meter name `"eventsource.bus.kafka"`.

---

## Metric Instruments Catalog

### Counter Metrics

| Metric Name | Unit | Attributes / Labels | Description |
|---|---|---|---|
| `kafka.eventbus.messages.published` | `messages` | `messaging.system`, `messaging.destination`, `event.type` | Total count of messages successfully published to Kafka. |
| `kafka.eventbus.messages.consumed` | `messages` | `messaging.system`, `messaging.destination`, `messaging.kafka.partition`, `event.type` | Total count of messages consumed and dispatched to handlers. |
| `kafka.eventbus.handler.invocations` | `invocations` | `handler.name`, `event.type` | Number of times an event handler was invoked. |
| `kafka.eventbus.handler.errors` | `errors` | `handler.name`, `event.type`, `error.type` | Number of exceptions raised by event handlers during dispatch. |
| `kafka.eventbus.messages.dlq` | `messages` | `dlq.reason`, `error.type` | Messages routed to the dead-letter queue after retries exhausted. |
| `kafka.eventbus.connection.errors` | `errors` | `error.type` | Connection dropped or broker connectivity errors. |
| `kafka.eventbus.reconnections` | `attempts` | _(none)_ | Total reconnection attempts initiated by the bus. |
| `kafka.eventbus.rebalances` | `rebalances` | `messaging.kafka.consumer_group` | Number of consumer group partition rebalance events. |
| `kafka.eventbus.publish.errors` | `errors` | `messaging.system`, `messaging.destination`, `event.type`, `error.type` | Number of failed publish attempts. |

### Histogram Metrics

| Metric Name | Unit | Attributes / Labels | Description |
|---|---|---|---|
| `kafka.eventbus.publish.duration` | `ms` | `messaging.destination` | End-to-end latency to acknowledge a publish request. |
| `kafka.eventbus.consume.duration` | `ms` | `messaging.destination` | Duration from message delivery to completion of all handlers. |
| `kafka.eventbus.handler.duration` | `ms` | `handler.name`, `event.type` | Execution duration of an individual handler callable. |
| `kafka.eventbus.batch.size` | `messages` | _(none)_ | Distribution of message counts in batch `publish()` calls. |

### Observable Gauge Metrics

| Metric Name | Unit | Attributes / Labels | Description |
|---|---|---|---|
| `kafka.eventbus.connections.active` | `1` | `messaging.system` | Connection status (1 = connected, 0 = disconnected). |
| `kafka.eventbus.consumer.lag` | `messages` | `messaging.kafka.consumer_group`, `messaging.destination`, `messaging.kafka.partition` | Number of uncommitted messages behind the partition high-water mark. |

---

## PromQL Queries for Monitoring & Alerting

### 1. Throughput (Published vs Consumed Rate)

```promql
# Published events per second by event type
sum by (event_type) (rate(kafka_eventbus_messages_published_total[5m]))

# Consumed events per second by topic
sum by (messaging_destination) (rate(kafka_eventbus_messages_consumed_total[5m]))
```

### 2. Error Rate & Handler Failures

```promql
# Handler error rate percentage
sum(rate(kafka_eventbus_handler_errors_total[5m]))
  /
sum(rate(kafka_eventbus_handler_invocations_total[5m])) * 100
```

### 3. P99 Latency

```promql
# Handler execution 99th percentile latency (seconds)
histogram_quantile(0.99, sum by (le, handler_name) (rate(kafka_eventbus_handler_duration_milliseconds_bucket[5m]))) / 1000

# Publish 99th percentile latency
histogram_quantile(0.99, sum by (le) (rate(kafka_eventbus_publish_duration_milliseconds_bucket[5m])))
```

### 4. Consumer Group Lag per Partition

```promql
# Total lag across consumer group
sum by (messaging_kafka_consumer_group) (kafka_eventbus_consumer_lag)

# Partitions with highest lag
topk(5, kafka_eventbus_consumer_lag)
```

---

## Recommended Prometheus Alerting Rules

```yaml
groups:
  - name: kafka-eventbus-alerts
    rules:
      - alert: KafkaConsumerLagSpike
        expr: sum by (messaging_kafka_consumer_group) (kafka_eventbus_consumer_lag) > 5000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafka consumer group {{ $labels.messaging_kafka_consumer_group }} lagging behind"
          description: "Consumer group has accumulated more than 5,000 unconsumed events for over 5 minutes."

      - alert: KafkaHighDLQRate
        expr: sum(rate(kafka_eventbus_messages_dlq_total[5m])) > 0.1
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Messages are being sent to DLQ"
          description: "Dead-letter queue rate is > 0.1 msg/sec. Inspect failing events immediately."

      - alert: KafkaRebalanceStorm
        expr: sum by (messaging_kafka_consumer_group) (rate(kafka_eventbus_rebalances_total[10m])) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Frequent Kafka rebalances detected"
          description: "Consumer group {{ $labels.messaging_kafka_consumer_group }} is experiencing frequent rebalances."

      - alert: KafkaBrokerDisconnected
        expr: kafka_eventbus_connections_active == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka Event Bus disconnected"
          description: "Kafka connection is down for more than 1 minute."
```
