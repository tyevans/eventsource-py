# Observability Guide

This guide covers the observability features in eventsource-py, including OpenTelemetry tracing integration, span naming conventions, standard attributes, and configuration options.

## Overview

eventsource-py provides built-in support for OpenTelemetry tracing across all components. Tracing is designed to:

- Provide visibility into event sourcing operations
- Enable distributed tracing across services (e.g., RabbitMQ, Kafka)
- Support debugging and performance analysis
- Integrate with popular observability backends (Jaeger, Zipkin, Grafana Tempo, etc.)

All tracing is optional and degrades gracefully when OpenTelemetry is not installed.

## Quick Start

### Install OpenTelemetry

```bash
# Install eventsource with telemetry support
pip install eventsource-py[telemetry]

# Or install OpenTelemetry packages directly
pip install opentelemetry-api opentelemetry-sdk
```

### Enable Tracing on Components

All eventsource components support tracing via the `enable_tracing` parameter:

```python
from eventsource.stores import SQLiteEventStore, InMemoryEventStore
from eventsource.bus import InMemoryEventBus
from eventsource.aggregates import AggregateRepository
from eventsource.snapshots import InMemorySnapshotStore

# Enable tracing on event store (enabled by default)
store = SQLiteEventStore(":memory:", enable_tracing=True)

# Enable tracing on event bus
bus = InMemoryEventBus(enable_tracing=True)

# Enable tracing on repository
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=True,
)

# Enable tracing on snapshot store
snapshot_store = InMemorySnapshotStore(enable_tracing=True)
```

### Configure an Exporter

Configure where traces are sent:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Configure provider
provider = TracerProvider()
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
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

### Error and Retry Attributes

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_RETRY_COUNT` | `eventsource.retry.count` | Number of retry attempts for an operation | `3` |
| `ATTR_ERROR_TYPE` | `eventsource.error.type` | Type of error encountered (exception class name) | `"OptimisticLockError"` |

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

## Related Resources

- [OpenTelemetry Python Documentation](https://opentelemetry.io/docs/instrumentation/python/)
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [Kafka Metrics Guide](kafka-metrics.md) - Metrics support for Kafka Event Bus
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [Zipkin Documentation](https://zipkin.io/)
- [Grafana Tempo Documentation](https://grafana.com/docs/tempo/latest/)
