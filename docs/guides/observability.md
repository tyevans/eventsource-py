# Observability Guide

This guide covers observability features in eventsource-py: tracing, metrics, and logging.

**Contents:**
- [Overview](#overview)
- [Quick Start](#quick-start)
- [Traced Components](#traced-components) - All traceable components and span names
- [Standard Attributes](#standard-attributes) - Attribute constants for spans
- [TracingMixin Usage](#tracingmixin-usage-guide) - Add tracing to custom components
- [Distributed Tracing](#distributed-tracing) - Cross-service trace propagation
- [Metrics](#metrics) - Kafka and subscription metrics
- [Logging](#logging) - Structured logging configuration
- [Troubleshooting](#troubleshooting)

---

## Overview

eventsource-py provides three observability pillars:

| Pillar | Technology | Purpose |
|--------|------------|---------|
| **Tracing** | OpenTelemetry | Distributed request tracking, latency analysis |
| **Metrics** | OpenTelemetry | Throughput, error rates, lag monitoring |
| **Logging** | Python `logging` | Structured operational logs |

All observability features are optional and degrade gracefully when dependencies are not installed.

## Quick Start

### Installation

```bash
# Install with telemetry support
pip install eventsource-py[telemetry]

# Includes: opentelemetry-api, opentelemetry-sdk
```

### Minimal Setup

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

# Configure tracing (do this once at startup)
trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(ConsoleSpanExporter())  # Replace with your exporter
)

# All eventsource components have tracing enabled by default
from eventsource.stores import SQLiteEventStore
from eventsource.aggregates import AggregateRepository

store = SQLiteEventStore(":memory:")  # Tracing ON by default
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
)
```

### Disable Tracing Per-Component

```python
# Disable for high-frequency operations
store = SQLiteEventStore(":memory:", enable_tracing=False)
projection = MyProjection(connection, enable_tracing=False)
```

## Traced Components

### Event Stores

All event store implementations support tracing:

| Component | Span Names |
|-----------|------------|
| SQLiteEventStore | `sqlite_event_store.append_events`, `sqlite_event_store.get_events` |
| PostgreSQLEventStore | `postgresql_event_store.append_events`, `postgresql_event_store.get_events` |
| InMemoryEventStore | `in_memory_event_store.append_events`, `in_memory_event_store.get_events` |

### Event Buses

| Component | Span Names |
|-----------|------------|
| InMemoryEventBus | `eventsource.event_bus.dispatch`, `eventsource.event_bus.handle` |
| RedisEventBus | `eventsource.event_bus.publish`, `eventsource.event_bus.process`, `eventsource.event_bus.dispatch`, `eventsource.event_bus.handle` |
| RabbitMQEventBus | `eventsource.event_bus.publish`, `eventsource.event_bus.consume`, `eventsource.event_bus.dispatch` |
| KafkaEventBus | `eventsource.event_bus.publish {event_type}`, `eventsource.event_bus.consume {event_type}`, `eventsource.event_bus.dispatch {handler}` |

### Repositories

| Component | Span Names |
|-----------|------------|
| AggregateRepository | `eventsource.repository.load`, `eventsource.repository.save`, `eventsource.repository.exists`, `eventsource.repository.create_snapshot` |
| CheckpointRepository | `eventsource.checkpoint.get_checkpoint`, `eventsource.checkpoint.update_checkpoint`, `eventsource.checkpoint.reset_checkpoint`, `eventsource.checkpoint.get_all_checkpoints`, `eventsource.checkpoint.get_lag_metrics` |
| DLQRepository | `eventsource.dlq.add`, `eventsource.dlq.get`, `eventsource.dlq.get_by_id`, `eventsource.dlq.resolve`, `eventsource.dlq.retry`, `eventsource.dlq.get_stats`, `eventsource.dlq.get_projection_counts`, `eventsource.dlq.delete_resolved` |
| OutboxRepository | `eventsource.outbox.add`, `eventsource.outbox.get_pending`, `eventsource.outbox.mark_published`, `eventsource.outbox.increment_retry`, `eventsource.outbox.mark_failed`, `eventsource.outbox.cleanup`, `eventsource.outbox.get_stats` |

### Snapshot Stores

| Component | Span Names |
|-----------|------------|
| InMemorySnapshotStore | `eventsource.snapshot.save`, `eventsource.snapshot.get`, `eventsource.snapshot.delete`, `eventsource.snapshot.exists`, `eventsource.snapshot.delete_by_type`, `eventsource.snapshot.clear` |
| PostgreSQLSnapshotStore | `eventsource.snapshot.save`, `eventsource.snapshot.get`, `eventsource.snapshot.delete`, `eventsource.snapshot.exists`, `eventsource.snapshot.delete_by_type` |
| SQLiteSnapshotStore | `eventsource.snapshot.save`, `eventsource.snapshot.get`, `eventsource.snapshot.delete`, `eventsource.snapshot.exists`, `eventsource.snapshot.delete_by_type` |

### Projections

| Component | Span Names |
|-----------|------------|
| Projection | `eventsource.projection.handle` |
| DeclarativeProjection | `eventsource.projection.handle`, `eventsource.projection.handler` |
| ProjectionCoordinator | `eventsource.projection.coordinate`, `eventsource.projection.registry.dispatch` |

**Note:** Projection tracing is OFF by default. Enable with `enable_tracing=True`.

### Subscription Manager

| Component | Span Names |
|-----------|------------|
| SubscriptionManager | `eventsource.subscription_manager.subscribe`, `eventsource.subscription_manager.start_subscription`, `eventsource.subscription_manager.stop` |
| TransitionCoordinator | `eventsource.transition_coordinator.execute` |
| CatchUpRunner | `eventsource.catchup_runner.run_until_position`, `eventsource.catchup_runner.deliver_event` |
| LiveRunner | `eventsource.live_runner.start`, `eventsource.live_runner.process_event` |
| PauseResumeController | `eventsource.pause_resume.pause`, `eventsource.pause_resume.resume` |
| SubscriptionLifecycleManager | `eventsource.lifecycle.start`, `eventsource.lifecycle.stop` |

## Standard Attributes

All spans include standardized attributes from `eventsource.observability.attributes`. These follow OpenTelemetry semantic conventions where applicable.

### Aggregate Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_AGGREGATE_ID` | `eventsource.aggregate.id` | Unique identifier for the aggregate instance (UUID string) | `"550e8400-e29b-41d4-a716-446655440000"` |
| `ATTR_AGGREGATE_TYPE` | `eventsource.aggregate.type` | Type name of the aggregate | `"Order"` |

### Event Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_EVENT_ID` | `eventsource.event.id` | Unique identifier for the event (UUID string) | `"550e8400-e29b-41d4-a716-446655440001"` |
| `ATTR_EVENT_TYPE` | `eventsource.event.type` | Type name of the event | `"OrderCreated"` |
| `ATTR_EVENT_COUNT` | `eventsource.event.count` | Number of events in an operation | `3` |

### Version Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_VERSION` | `eventsource.version` | Current version of an aggregate or stream | `5` |
| `ATTR_EXPECTED_VERSION` | `eventsource.expected_version` | Expected version for optimistic concurrency | `4` |
| `ATTR_FROM_VERSION` | `eventsource.from_version` | Starting version for event retrieval | `0` |

### Tenant and Actor Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_TENANT_ID` | `eventsource.tenant.id` | Tenant identifier for multi-tenant systems | `"tenant-123"` |
| `ATTR_ACTOR_ID` | `eventsource.actor.id` | Actor/user identifier who initiated the action | `"user@example.com"` |

### Component-Specific Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_PROJECTION_NAME` | `eventsource.projection.name` | Name of the projection processing events | `"OrderSummaryProjection"` |
| `ATTR_HANDLER_NAME` | `eventsource.handler.name` | Name of the event handler being invoked | `"OrderCreatedHandler"` |
| `ATTR_HANDLER_COUNT` | `eventsource.handler.count` | Number of handlers registered for an event type | `2` |
| `ATTR_HANDLER_SUCCESS` | `eventsource.handler.success` | Whether the handler executed successfully | `true` |
| `ATTR_STREAM_ID` | `eventsource.stream.id` | Identifier for an event stream | `"order-123:Order"` |
| `ATTR_POSITION` | `eventsource.position` | Position in the event stream | `42` |

### Database Attributes (OpenTelemetry Semantic Conventions)

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_DB_SYSTEM` | `db.system` | Database system identifier | `"postgresql"`, `"sqlite"` |
| `ATTR_DB_NAME` | `db.name` | Database name being accessed | `"events_db"` |
| `ATTR_DB_OPERATION` | `db.operation` | Database operation type | `"INSERT"`, `"SELECT"` |

### Messaging Attributes (OpenTelemetry Semantic Conventions)

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_MESSAGING_SYSTEM` | `messaging.system` | Messaging system identifier | `"rabbitmq"`, `"redis"`, `"kafka"` |
| `ATTR_MESSAGING_DESTINATION` | `messaging.destination` | Destination queue or topic name | `"events"` |
| `ATTR_MESSAGING_OPERATION` | `messaging.operation` | Messaging operation type | `"publish"`, `"receive"` |

### Subscription Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_SUBSCRIPTION_NAME` | `eventsource.subscription.name` | Subscription name | `"OrderProjection"` |
| `ATTR_SUBSCRIPTION_STATE` | `eventsource.subscription.state` | Current state | `"live"`, `"catching_up"` |
| `ATTR_SUBSCRIPTION_PHASE` | `eventsource.subscription.phase` | Transition phase | `"initial_catchup"` |
| `ATTR_FROM_POSITION` | `eventsource.from_position` | Starting position | `0` |
| `ATTR_TO_POSITION` | `eventsource.to_position` | Target position | `1000` |
| `ATTR_BATCH_SIZE` | `eventsource.batch.size` | Events per batch | `100` |
| `ATTR_EVENTS_PROCESSED` | `eventsource.events.processed` | Events processed count | `5000` |

### Error and Retry Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_RETRY_COUNT` | `eventsource.retry.count` | Retry attempts | `3` |
| `ATTR_ERROR_TYPE` | `eventsource.error.type` | Exception class name | `"OptimisticLockError"` |

## TracingMixin Usage Guide

The `TracingMixin` class provides a consistent way to add tracing to your own components:

```python
from eventsource.observability import TracingMixin
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_TYPE,
)

class MyCustomStore(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        # Initialize tracing via mixin
        self._init_tracing(__name__, enable_tracing)

    async def save(self, aggregate_id: str, data: dict) -> None:
        # Create span with dynamic attributes
        with self._create_span_context(
            "my_store.save",
            {
                ATTR_AGGREGATE_ID: aggregate_id,
                "data.size": len(data),
            },
        ) as span:
            # Your implementation
            result = await self._do_save(aggregate_id, data)

            # Add additional attributes after operation
            if span:
                span.set_attribute("result.success", True)

            return result

    @property
    def tracing_enabled(self) -> bool:
        """Check if tracing is currently enabled."""
        return self._enable_tracing and self._tracer is not None
```

### TracingMixin Methods

| Method | Description |
|--------|-------------|
| `_init_tracing(tracer_name, enable_tracing)` | Initialize tracing attributes. Call in `__init__`. |
| `_create_span_context(name, attributes)` | Create a span context manager that handles None tracer gracefully. |
| `tracing_enabled` (property) | Check if tracing is currently enabled and available. |

### Using the @traced Decorator

For methods with only static attributes, use the `@traced` decorator:

```python
from eventsource.observability import traced, TracingMixin

class MyService(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    @traced("my_service.operation", attributes={"db.system": "sqlite"})
    async def operation(self, item_id: str) -> None:
        # Method body - automatically traced
        pass
```

## Distributed Tracing

### RabbitMQ Context Propagation

RabbitMQEventBus supports distributed trace context propagation using W3C Trace Context format:

```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

# Publisher service
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://localhost:5672/",
    enable_tracing=True,
)
bus = RabbitMQEventBus(config=config)

async with bus:
    # Trace context is automatically injected into message headers
    await bus.publish([order_created])
```

```python
# Consumer service (different process/server)
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://localhost:5672/",
    consumer_group="order-projections",
    enable_tracing=True,
)
bus = RabbitMQEventBus(config=config)

async with bus:
    bus.subscribe(OrderCreated, order_handler)
    await bus.start_consuming()
    # Consumer spans are children of publisher spans
    # Full distributed trace is maintained
```

### Kafka Context Propagation

KafkaEventBus also supports distributed tracing:

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    enable_tracing=True,
)
bus = KafkaEventBus(config=config)
```

## Example Traces

### Aggregate Load with Snapshot

```
[eventsource.repository.load] 45ms
    eventsource.aggregate.id: "550e8400-..."
    eventsource.aggregate.type: "Order"
    snapshot.used: true
    events.replayed: 15
    eventsource.version: 115
  [eventsource.snapshot.get] 5ms
    db.system: postgresql
  [postgresql_event_store.get_events] 35ms
    db.system: postgresql
    eventsource.from_version: 100
```

### Event Publishing via RabbitMQ

```
[eventsource.repository.save] 80ms
    eventsource.aggregate.id: "550e8400-..."
    eventsource.event.count: 2
    eventsource.version: 117
  [sqlite_event_store.append_events] 40ms
    db.system: sqlite
    eventsource.event.count: 2
  [eventsource.event_bus.publish] 35ms
    messaging.system: rabbitmq
    messaging.destination: events
    eventsource.event.type: OrderCreated
```

### Distributed Trace Across Services

```
Service A (Publisher):
[eventsource.event_bus.publish] 25ms
    messaging.system: rabbitmq
    eventsource.event.type: OrderCreated
    eventsource.aggregate.id: "order-123"

Service B (Consumer):
  [eventsource.event_bus.consume] 150ms  (child of publish span)
      eventsource.event.type: OrderCreated
    [eventsource.event_bus.dispatch] 145ms
      eventsource.handler.count: 2
      [eventsource.event_bus.handle] 70ms
          eventsource.handler.name: OrderProjection
          eventsource.handler.success: true
      [eventsource.event_bus.handle] 72ms
          eventsource.handler.name: NotificationService
          eventsource.handler.success: true
```

## Example Configurations

### Jaeger

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

provider = TracerProvider()
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)
provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
trace.set_tracer_provider(provider)
```

### Zipkin

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.zipkin.json import ZipkinExporter

provider = TracerProvider()
zipkin_exporter = ZipkinExporter(endpoint="http://localhost:9411/api/v2/spans")
provider.add_span_processor(BatchSpanProcessor(zipkin_exporter))
trace.set_tracer_provider(provider)
```

### OTLP (Grafana Tempo, Honeycomb, etc.)

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

provider = TracerProvider()

# For Grafana Tempo
otlp_exporter = OTLPSpanExporter(endpoint="http://tempo:4317", insecure=True)

# For Honeycomb
# otlp_exporter = OTLPSpanExporter(
#     endpoint="https://api.honeycomb.io:443",
#     headers={"x-honeycomb-team": "YOUR_API_KEY"},
# )

provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(provider)
```

### Console Exporter (Development)

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter

provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
```

## Best Practices

### 1. Enable Tracing Selectively

```python
import os

# Enable full tracing in development
ENABLE_TRACING = os.getenv("ENVIRONMENT") in ("development", "staging")

# Or use sampling in production
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

provider = TracerProvider(
    sampler=TraceIdRatioBased(0.1)  # Sample 10% of traces
)
```

### 2. Use Standard Attributes

Import and use attribute constants for consistency:

```python
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_TYPE,
    ATTR_DB_SYSTEM,
)

# Use constants instead of string literals
span.set_attribute(ATTR_AGGREGATE_ID, str(aggregate_id))
span.set_attribute(ATTR_EVENT_TYPE, event.__class__.__name__)
```

### 3. Graceful Degradation

eventsource gracefully handles missing OpenTelemetry:

```python
# Works even without OpenTelemetry installed
store = SQLiteEventStore(":memory:", enable_tracing=True)
# Tracing becomes a no-op when OTEL is not available
```

Check availability programmatically:

```python
from eventsource.observability import OTEL_AVAILABLE

if OTEL_AVAILABLE:
    print("OpenTelemetry is available")
else:
    print("OpenTelemetry not installed, tracing disabled")
```

### 4. Disable High-Frequency Tracing in Production

Projection handlers can be invoked very frequently. Consider disabling tracing for projections in production unless debugging:

```python
# High-frequency projection - disable tracing in production
class OrderSummaryProjection(Projection):
    def __init__(self, connection):
        super().__init__(
            connection=connection,
            enable_tracing=os.getenv("ENVIRONMENT") != "production",
        )
```

### 5. Use BatchSpanProcessor

Always use `BatchSpanProcessor` instead of `SimpleSpanProcessor` in production to avoid blocking:

```python
# Good - batches spans and exports asynchronously
from opentelemetry.sdk.trace.export import BatchSpanProcessor
provider.add_span_processor(BatchSpanProcessor(exporter))

# Bad - blocks on every span (only use for development)
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
provider.add_span_processor(SimpleSpanProcessor(exporter))
```

## Troubleshooting

### No Spans Appearing

1. **Check OpenTelemetry is installed:**
   ```bash
   pip show opentelemetry-api opentelemetry-sdk
   ```

2. **Verify `enable_tracing=True` is set on components:**
   ```python
   store = SQLiteEventStore(":memory:", enable_tracing=True)  # Explicit
   ```

3. **Confirm exporter is configured correctly:**
   ```python
   from opentelemetry import trace
   provider = trace.get_tracer_provider()
   print(f"Provider: {provider}")  # Should not be NoOpTracerProvider
   ```

4. **Check `OTEL_AVAILABLE` from observability module:**
   ```python
   from eventsource.observability import OTEL_AVAILABLE
   print(f"OTEL Available: {OTEL_AVAILABLE}")
   ```

### High Overhead

1. **Disable projection tracing** (highest frequency):
   ```python
   projection = MyProjection(connection, enable_tracing=False)
   ```

2. **Enable sampling** in your OpenTelemetry configuration:
   ```python
   from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
   provider = TracerProvider(sampler=TraceIdRatioBased(0.1))
   ```

3. **Use batch span processor**, not simple processor:
   ```python
   from opentelemetry.sdk.trace.export import BatchSpanProcessor
   provider.add_span_processor(BatchSpanProcessor(exporter))
   ```

### Missing Distributed Context

1. **Ensure both publisher and consumer have tracing enabled:**
   ```python
   # Publisher
   bus = RabbitMQEventBus(config=RabbitMQEventBusConfig(enable_tracing=True))

   # Consumer
   bus = RabbitMQEventBus(config=RabbitMQEventBusConfig(enable_tracing=True))
   ```

2. **Check propagation packages are installed:**
   ```bash
   pip show opentelemetry-propagator-w3c-trace-context
   ```

3. **Verify both services are configured with the same trace provider backend**

---

## Metrics

eventsource-py emits OpenTelemetry metrics for monitoring throughput, latency, and system health.

### Kafka Event Bus Metrics

The Kafka event bus provides comprehensive metrics. See [Kafka Metrics Guide](kafka-metrics.md) for details.

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    enable_metrics=True,   # Enabled by default
    enable_tracing=True,   # Enabled by default
)
bus = KafkaEventBus(config=config)
```

**Key Kafka Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `kafka.eventbus.messages.published` | Counter | Messages published |
| `kafka.eventbus.messages.consumed` | Counter | Messages consumed |
| `kafka.eventbus.handler.duration` | Histogram | Handler execution time (ms) |
| `kafka.eventbus.consumer.lag` | Gauge | Consumer lag per partition |

### Subscription Manager Metrics

The subscription manager emits metrics for event processing:

```python
from eventsource.subscriptions import SubscriptionManager, get_metrics

# Metrics are emitted automatically
manager = SubscriptionManager(
    event_store=store,
    event_bus=bus,
    checkpoint_repo=checkpoint_repo,
)

# Access metrics programmatically
metrics = get_metrics("OrderProjection")
snapshot = metrics.get_snapshot()
print(f"Processed: {snapshot.events_processed}")
print(f"Lag: {snapshot.current_lag}")
```

**Subscription Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `subscription.events.processed` | Counter | Events processed successfully |
| `subscription.events.failed` | Counter | Events that failed processing |
| `subscription.processing.duration` | Histogram | Processing time (ms) |
| `subscription.lag` | Gauge | Current event lag |
| `subscription.state` | Gauge | Current state (numeric) |

**State Values:**

| Value | State | Description |
|-------|-------|-------------|
| 1 | `starting` | Subscription initializing |
| 2 | `catching_up` | Processing historical events |
| 3 | `live` | Processing real-time events |
| 4 | `paused` | Temporarily paused |
| 5 | `stopped` | Stopped |
| 6 | `error` | Error state |

### Configure Metrics Export

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# OTLP export to collector
reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://otel-collector:4317"),
    export_interval_millis=60000,
)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
```

```python
# Prometheus export
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

start_http_server(port=8000)
reader = PrometheusMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
```

---

## Logging

eventsource-py uses Python's standard `logging` module. All components log to named loggers under the `eventsource` namespace.

### Configure Logging

```python
import logging

# Basic configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Adjust eventsource log level
logging.getLogger("eventsource").setLevel(logging.DEBUG)

# Fine-tune specific components
logging.getLogger("eventsource.stores.sqlite").setLevel(logging.WARNING)
logging.getLogger("eventsource.bus.kafka").setLevel(logging.DEBUG)
```

### Logger Hierarchy

| Logger Name | Component |
|-------------|-----------|
| `eventsource.stores.sqlite` | SQLite event store |
| `eventsource.stores.postgresql` | PostgreSQL event store |
| `eventsource.bus.kafka` | Kafka event bus |
| `eventsource.bus.rabbitmq` | RabbitMQ event bus |
| `eventsource.bus.redis` | Redis event bus |
| `eventsource.aggregates.repository` | Aggregate repository |
| `eventsource.subscriptions.manager` | Subscription manager |
| `eventsource.projections` | Projection system |

### Structured Logging with JSON

For production environments, use structured JSON logging:

```python
import logging
import json
from datetime import datetime, UTC

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logging.getLogger("eventsource").addHandler(handler)
```

### Correlating Logs with Traces

Inject trace context into log records:

```python
from opentelemetry import trace

class TraceContextFilter(logging.Filter):
    def filter(self, record):
        span = trace.get_current_span()
        ctx = span.get_span_context()
        record.trace_id = format(ctx.trace_id, "032x") if ctx.is_valid else ""
        record.span_id = format(ctx.span_id, "016x") if ctx.is_valid else ""
        return True

handler = logging.StreamHandler()
handler.addFilter(TraceContextFilter())
handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] trace_id=%(trace_id)s span_id=%(span_id)s - %(message)s"
))
logging.getLogger("eventsource").addHandler(handler)
```

---

## Related Resources

**eventsource-py guides:**
- [Kafka Metrics Guide](kafka-metrics.md) - Detailed Kafka metrics, PromQL queries, alerting
- [Subscriptions Guide](subscriptions.md) - Subscription manager with metrics section
- [Production Guide](production.md) - Production deployment recommendations

**OpenTelemetry:**
- [OpenTelemetry Python](https://opentelemetry.io/docs/instrumentation/python/)
- [Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)

**Observability backends:**
- [Jaeger](https://www.jaegertracing.io/docs/)
- [Zipkin](https://zipkin.io/)
- [Grafana Tempo](https://grafana.com/docs/tempo/latest/)
- [Honeycomb](https://docs.honeycomb.io/)
