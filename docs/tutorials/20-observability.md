# Tutorial 20: Observability with OpenTelemetry

**Difficulty:** Advanced
**Progress:** Tutorial 20 of 21 | Phase 4: Advanced Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 18: Using Kafka Event Bus](18-kafka.md)
- Completed [Tutorial 15: Production Considerations](15-production.md)
- Python 3.11+ installed
- Basic understanding of distributed tracing concepts
- Optional: Jaeger running locally (Docker recommended)

This tutorial builds on the production concepts from Tutorial 15 and teaches you how to implement comprehensive observability in your event-sourced applications.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Configure OpenTelemetry for event-sourced applications
2. Understand which components are traced automatically
3. Add tracing to custom components using TracingMixin
4. Use standard attributes for consistent span metadata
5. Configure exporters for development and production environments
6. Implement distributed tracing across services

---

## Why Observability Matters for Event Sourcing

Event-sourced systems have unique observability challenges:

- **Event Flow Complexity**: Events flow through multiple stages - stores, buses, projections, handlers
- **Asynchronous Processing**: Events may be processed out-of-order or with delays
- **Distributed Nature**: Events often cross service boundaries
- **Debugging Difficulty**: Without proper tracing, finding the root cause of issues is difficult
- **Performance Analysis**: Understanding where time is spent requires detailed instrumentation

Observability provides the visibility needed to:

- Track events from creation to final processing
- Identify bottlenecks in event processing pipelines
- Debug projection failures and event handling errors
- Monitor system health and performance trends
- Correlate issues across distributed services

### The Three Pillars of Observability

eventsource-py provides support for all three observability pillars:

| Pillar | Technology | Purpose |
|--------|------------|---------|
| **Tracing** | OpenTelemetry | Distributed request tracking, latency analysis |
| **Metrics** | OpenTelemetry | Throughput, error rates, lag monitoring |
| **Logging** | Python `logging` | Structured operational logs |

This tutorial focuses primarily on tracing, with sections on metrics and logging integration.

---

## Installation

Install eventsource-py with telemetry support:

```bash
pip install eventsource-py[telemetry]
```

This installs:

- `opentelemetry-api` - OpenTelemetry API
- `opentelemetry-sdk` - OpenTelemetry SDK

For production, you'll also want exporters:

```bash
# For Jaeger
pip install opentelemetry-exporter-jaeger

# For OTLP (Grafana Tempo, Honeycomb, etc.)
pip install opentelemetry-exporter-otlp

# For Zipkin
pip install opentelemetry-exporter-zipkin
```

### Verifying Installation

Check that OpenTelemetry support is available:

```python
from eventsource.observability import OTEL_AVAILABLE

if OTEL_AVAILABLE:
    print("OpenTelemetry is available!")
else:
    print("OpenTelemetry not available. Install with: pip install eventsource-py[telemetry]")
```

---

## OpenTelemetry Setup

### Basic Configuration

Configure OpenTelemetry once at application startup:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

# Create a tracer provider
provider = TracerProvider()

# Add a span processor with an exporter
provider.add_span_processor(
    BatchSpanProcessor(ConsoleSpanExporter())  # Prints spans to console
)

# Set as the global tracer provider
trace.set_tracer_provider(provider)

# Now all eventsource components will emit traces
```

### Console Exporter (Development)

For development, the console exporter prints spans directly to stdout:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter

# Simple processor for immediate output (development only)
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
```

**Note**: Use `SimpleSpanProcessor` only for development. It blocks on every span, which impacts performance.

### Jaeger Exporter (Production)

Jaeger provides a full-featured distributed tracing UI:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

# Create resource with service name
resource = Resource(attributes={
    SERVICE_NAME: "order-service"
})

# Create provider with resource
provider = TracerProvider(resource=resource)

# Add Jaeger exporter
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)
provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

# Set as global provider
trace.set_tracer_provider(provider)

print("Traces will be sent to Jaeger at http://localhost:16686")
```

### OTLP Exporter (Grafana Tempo, Honeycomb)

For cloud-native backends:

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

---

## Traced Components

All eventsource components have built-in OpenTelemetry tracing. When tracing is enabled and OpenTelemetry is configured, operations automatically emit spans.

### Event Stores

All event store implementations support tracing:

| Component | Span Names |
|-----------|------------|
| SQLiteEventStore | `sqlite_event_store.append_events`, `sqlite_event_store.get_events` |
| PostgreSQLEventStore | `postgresql_event_store.append_events`, `postgresql_event_store.get_events` |
| InMemoryEventStore | `in_memory_event_store.append_events`, `in_memory_event_store.get_events` |

```python
from eventsource import InMemoryEventStore

# Tracing is ON by default
store = InMemoryEventStore()

# Disable for high-frequency operations
store_no_trace = InMemoryEventStore(enable_tracing=False)
```

### Repositories

| Component | Span Names |
|-----------|------------|
| AggregateRepository | `eventsource.repository.load`, `eventsource.repository.save`, `eventsource.repository.exists`, `eventsource.repository.create_snapshot` |
| CheckpointRepository | `eventsource.checkpoint.get_checkpoint`, `eventsource.checkpoint.update_checkpoint`, `eventsource.checkpoint.reset_checkpoint` |
| DLQRepository | `eventsource.dlq.add`, `eventsource.dlq.get`, `eventsource.dlq.resolve`, `eventsource.dlq.retry` |
| OutboxRepository | `eventsource.outbox.add`, `eventsource.outbox.get_pending`, `eventsource.outbox.mark_published` |

```python
from eventsource import AggregateRepository

# Tracing is ON by default
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=True,  # Default
)
```

### Event Buses

| Component | Span Names |
|-----------|------------|
| InMemoryEventBus | `eventsource.event_bus.dispatch`, `eventsource.event_bus.handle` |
| RedisEventBus | `eventsource.event_bus.publish`, `eventsource.event_bus.process`, `eventsource.event_bus.dispatch`, `eventsource.event_bus.handle` |
| RabbitMQEventBus | `eventsource.event_bus.publish`, `eventsource.event_bus.consume`, `eventsource.event_bus.dispatch` |
| KafkaEventBus | `eventsource.event_bus.publish`, `eventsource.event_bus.consume`, `eventsource.event_bus.dispatch` |

### Snapshot Stores

| Component | Span Names |
|-----------|------------|
| InMemorySnapshotStore | `eventsource.snapshot.save`, `eventsource.snapshot.get`, `eventsource.snapshot.delete`, `eventsource.snapshot.exists` |
| PostgreSQLSnapshotStore | `eventsource.snapshot.save`, `eventsource.snapshot.get`, `eventsource.snapshot.delete`, `eventsource.snapshot.exists` |
| SQLiteSnapshotStore | `eventsource.snapshot.save`, `eventsource.snapshot.get`, `eventsource.snapshot.delete`, `eventsource.snapshot.exists` |

### Projections

| Component | Span Names |
|-----------|------------|
| Projection | `eventsource.projection.handle` |
| DeclarativeProjection | `eventsource.projection.handle`, `eventsource.projection.handler` |

**Note**: Projection tracing is OFF by default due to high-frequency invocation. Enable explicitly:

```python
class OrderProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__(enable_tracing=True)  # Explicit opt-in
```

---

## Standard Attributes

eventsource-py provides standard attribute constants for consistent span metadata. These follow OpenTelemetry semantic conventions where applicable.

### Attribute Constants

Import attributes from the observability module:

```python
from eventsource.observability import (
    # Aggregate attributes
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    # Event attributes
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EVENT_COUNT,
    # Version attributes
    ATTR_VERSION,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
    # Component attributes
    ATTR_PROJECTION_NAME,
    ATTR_HANDLER_NAME,
    # Database attributes (OTEL semantic)
    ATTR_DB_SYSTEM,
    ATTR_DB_NAME,
    ATTR_DB_OPERATION,
    # Messaging attributes (OTEL semantic)
    ATTR_MESSAGING_SYSTEM,
    ATTR_MESSAGING_DESTINATION,
    # Error attributes
    ATTR_ERROR_TYPE,
    ATTR_RETRY_COUNT,
)
```

### Attribute Reference

| Constant | Attribute Name | Description | Example Value |
|----------|----------------|-------------|---------------|
| `ATTR_AGGREGATE_ID` | `eventsource.aggregate.id` | Aggregate instance UUID | `"550e8400-..."` |
| `ATTR_AGGREGATE_TYPE` | `eventsource.aggregate.type` | Type name | `"Order"` |
| `ATTR_EVENT_TYPE` | `eventsource.event.type` | Event type name | `"OrderCreated"` |
| `ATTR_EVENT_COUNT` | `eventsource.event.count` | Number of events | `3` |
| `ATTR_VERSION` | `eventsource.version` | Current version | `5` |
| `ATTR_DB_SYSTEM` | `db.system` | Database system | `"postgresql"` |
| `ATTR_MESSAGING_SYSTEM` | `messaging.system` | Messaging system | `"kafka"` |

### Using Attributes

Use constants instead of string literals for consistency:

```python
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_TYPE,
    get_tracer,
)

tracer = get_tracer(__name__)

with tracer.start_as_current_span("process_event") as span:
    span.set_attribute(ATTR_AGGREGATE_ID, str(aggregate_id))
    span.set_attribute(ATTR_EVENT_TYPE, event.__class__.__name__)
```

---

## Custom Component Tracing

Add tracing to your own components using `TracingMixin`.

### Using TracingMixin

The `TracingMixin` class provides a consistent way to add tracing:

```python
from eventsource.observability import TracingMixin
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_COUNT,
)

class OrderService(TracingMixin):
    """
    Service layer with custom tracing.
    """

    def __init__(self, repository, enable_tracing: bool = True):
        # Initialize tracing via mixin
        self._init_tracing(__name__, enable_tracing)
        self._repository = repository

    async def place_order(self, customer_id: str, items: list) -> Order:
        """Place a new order with tracing."""
        # Create a custom span with dynamic attributes
        with self._create_span_context(
            "order_service.place_order",
            {"customer.id": customer_id, "items.count": len(items)},
        ) as span:
            order_id = uuid4()

            # Add attributes after computation
            if span:
                span.set_attribute(ATTR_AGGREGATE_ID, str(order_id))

            # Create and save the order
            order = self._repository.create_new(order_id)
            order.place(customer_id, items)
            await self._repository.save(order)

            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(order.pending_events))

            return order

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

### The @traced Decorator

For methods with only static attributes, use the `@traced` decorator:

```python
from eventsource.observability import traced, TracingMixin

class NotificationService(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    @traced("notification_service.send_email", attributes={"channel": "email"})
    async def send_email(self, recipient: str, message: str) -> None:
        """Send an email notification. Automatically traced."""
        await self._email_client.send(recipient, message)

    @traced("notification_service.send_sms", attributes={"channel": "sms"})
    async def send_sms(self, phone: str, message: str) -> None:
        """Send an SMS notification. Automatically traced."""
        await self._sms_client.send(phone, message)
```

**Note**: The `@traced` decorator requires the class to have `_tracer` and `_enable_tracing` attributes, which `TracingMixin` provides via `_init_tracing()`.

---

## Distributed Tracing

Distributed tracing allows you to track events across service boundaries.

### Context Propagation

When events flow between services, trace context must be propagated. eventsource's distributed event buses handle this automatically.

### RabbitMQ Context Propagation

RabbitMQEventBus propagates trace context using W3C Trace Context format:

```python
# Publisher service
from eventsource import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://localhost:5672/",
    enable_tracing=True,  # Enable context propagation
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
    enable_tracing=True,  # Enable context extraction
)
bus = RabbitMQEventBus(config=config)

async with bus:
    bus.subscribe(OrderCreated, order_handler)
    await bus.start_consuming()
    # Consumer spans are children of publisher spans
    # Full distributed trace is maintained
```

### Kafka Context Propagation

KafkaEventBus also supports distributed tracing with automatic context propagation:

```python
from eventsource import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="projections",
    enable_tracing=True,  # Enabled by default
)
bus = KafkaEventBus(config=config)
```

### Example Distributed Trace

When a publisher sends an event that is consumed by another service:

```
Service A (Publisher):
[eventsource.event_bus.publish] 25ms
    messaging.system: kafka
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

---

## Metrics

eventsource-py emits OpenTelemetry metrics for monitoring throughput, latency, and system health.

### Kafka Event Bus Metrics

The Kafka event bus provides comprehensive metrics:

```python
from eventsource import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="events",
    consumer_group="projections",
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

See the [Kafka Metrics Guide](../guides/kafka-metrics.md) for complete reference.

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

---

## Logging Integration

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
| `eventsource.aggregates.repository` | Aggregate repository |
| `eventsource.projections` | Projection system |

### Correlating Logs with Traces

Inject trace context into log records for correlation:

```python
from opentelemetry import trace
import logging

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

## Jaeger Setup with Docker

The easiest way to run Jaeger locally is with Docker.

### Docker Compose File

Create a file named `docker-compose.jaeger.yml`:

```yaml
version: '3.8'

services:
  jaeger:
    image: jaegertracing/all-in-one:1.51
    ports:
      - "6831:6831/udp"   # Thrift compact
      - "6832:6832/udp"   # Thrift binary
      - "16686:16686"     # UI
      - "14268:14268"     # HTTP collector
      - "4317:4317"       # OTLP gRPC
      - "4318:4318"       # OTLP HTTP
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

### Starting Jaeger

```bash
docker compose -f docker-compose.jaeger.yml up -d
```

Access the Jaeger UI at http://localhost:16686

---

## Complete Example

Here is a comprehensive example demonstrating observability setup:

```python
"""
Tutorial 20: Observability with OpenTelemetry

This example demonstrates observability setup for event-sourced applications.
Run with: python tutorial_20_observability.py

Prerequisites:
- pip install eventsource-py[telemetry]
- Optional: Jaeger running on localhost:6831
"""
import asyncio
from uuid import uuid4

from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)

# Check OpenTelemetry availability
from eventsource.observability import (
    OTEL_AVAILABLE,
    TracingMixin,
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_TYPE,
    ATTR_EVENT_COUNT,
)

if not OTEL_AVAILABLE:
    print("OpenTelemetry not available. Install with: pip install eventsource-py[telemetry]")
    print("Continuing without tracing...\n")


# =============================================================================
# OpenTelemetry Setup
# =============================================================================

def setup_tracing_console():
    """Set up tracing with console output (development)."""
    if not OTEL_AVAILABLE:
        return

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
    )

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)

    print("Tracing configured with ConsoleSpanExporter")


def setup_tracing_jaeger():
    """Set up tracing with Jaeger (production)."""
    if not OTEL_AVAILABLE:
        return

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME

    try:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    except ImportError:
        print("Jaeger exporter not installed. Using console.")
        setup_tracing_console()
        return

    resource = Resource(attributes={SERVICE_NAME: "tutorial-20-app"})
    provider = TracerProvider(resource=resource)
    jaeger_exporter = JaegerExporter(
        agent_host_name="localhost",
        agent_port=6831,
    )
    provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
    trace.set_tracer_provider(provider)

    print("Tracing configured with JaegerExporter")
    print("View traces at: http://localhost:16686")


# =============================================================================
# Domain Events
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


# =============================================================================
# Aggregate
# =============================================================================

class TaskState(BaseModel):
    task_id: str
    title: str = ""
    completed: bool = False


class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=str(self.aggregate_id),
                title=event.title,
                completed=False,
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"completed": True})

    def create(self, title: str) -> None:
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Custom Component with TracingMixin
# =============================================================================

class TaskService(TracingMixin):
    """
    Service layer with custom tracing.

    Demonstrates how to add tracing to your own components.
    """

    def __init__(self, repository: AggregateRepository, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)
        self._repository = repository

    async def create_task(self, title: str) -> TaskAggregate:
        """Create a new task with tracing."""
        with self._create_span_context(
            "task_service.create_task",
            {"task.title": title},
        ) as span:
            task_id = uuid4()

            if span:
                span.set_attribute(ATTR_AGGREGATE_ID, str(task_id))
                span.set_attribute(ATTR_AGGREGATE_TYPE, "Task")

            task = self._repository.create_new(task_id)
            task.create(title)
            await self._repository.save(task)

            if span:
                span.set_attribute(ATTR_EVENT_COUNT, 1)

            return task

    async def complete_task(self, task_id) -> TaskAggregate:
        """Complete a task with tracing."""
        with self._create_span_context(
            "task_service.complete_task",
            {ATTR_AGGREGATE_ID: str(task_id)},
        ) as span:
            task = await self._repository.load(task_id)
            task.complete()
            await self._repository.save(task)

            return task

    async def get_task(self, task_id) -> TaskAggregate:
        """Get a task by ID with tracing."""
        with self._create_span_context(
            "task_service.get_task",
            {ATTR_AGGREGATE_ID: str(task_id)},
        ) as span:
            return await self._repository.load(task_id)


# =============================================================================
# Examples
# =============================================================================

async def basic_tracing_example():
    """Demonstrate basic tracing with built-in components."""
    print("\n=== Basic Tracing Example ===\n")

    # Event store has tracing enabled by default
    event_store = InMemoryEventStore()  # enable_tracing=True

    # Repository also traced
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Operations automatically create spans
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Learn OpenTelemetry")
    await repo.save(task)  # Creates span: eventsource.repository.save

    loaded = await repo.load(task_id)  # Creates span: eventsource.repository.load
    print(f"Task loaded: {loaded.state.title}")

    print("Check console output for span information")


async def custom_tracing_example():
    """Demonstrate custom component tracing."""
    print("\n=== Custom Component Tracing ===\n")

    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Service with custom tracing
    service = TaskService(repo, enable_tracing=True)

    # Service operations create custom spans
    task = await service.create_task("Build observability")
    print(f"Created: {task.state.title}")

    task = await service.complete_task(task.aggregate_id)
    print(f"Completed: {task.state.completed}")

    print("Custom spans created: task_service.create_task, task_service.complete_task")


async def standard_attributes_example():
    """Show standard attribute usage."""
    print("\n=== Standard Attributes ===\n")

    print("eventsource provides standard attribute constants:")
    print()
    print("Aggregate attributes:")
    print(f"  ATTR_AGGREGATE_ID   = '{ATTR_AGGREGATE_ID}'")
    print(f"  ATTR_AGGREGATE_TYPE = '{ATTR_AGGREGATE_TYPE}'")
    print()
    print("Event attributes:")
    print(f"  ATTR_EVENT_TYPE     = '{ATTR_EVENT_TYPE}'")
    print(f"  ATTR_EVENT_COUNT    = '{ATTR_EVENT_COUNT}'")
    print()
    print("Use these for consistent span attributes across your application.")


async def disable_tracing_example():
    """Show how to disable tracing."""
    print("\n=== Disabling Tracing ===\n")

    # Disable for high-frequency operations
    store_no_trace = InMemoryEventStore(enable_tracing=False)
    print("Created event store with tracing disabled")

    # Useful for:
    print("- High-frequency read operations")
    print("- Batch processing that would generate too many spans")
    print("- Testing scenarios where tracing adds noise")


async def main():
    print("Tutorial 20: Observability with OpenTelemetry")
    print("=" * 50)

    # Set up tracing (choose one)
    setup_tracing_console()
    # setup_tracing_jaeger()  # Uncomment if Jaeger is running

    await basic_tracing_example()
    await custom_tracing_example()
    await standard_attributes_example()
    await disable_tracing_example()

    print("\n" + "=" * 50)
    print("Tutorial complete!")

    # Flush traces before exit
    if OTEL_AVAILABLE:
        print("\nFlushing traces...")
        from opentelemetry import trace
        provider = trace.get_tracer_provider()
        if hasattr(provider, 'force_flush'):
            provider.force_flush()


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_20_observability.py` and run it:

```bash
python tutorial_20_observability.py
```

---

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
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_TYPE,
)

# Use constants instead of string literals
span.set_attribute(ATTR_AGGREGATE_ID, str(aggregate_id))
span.set_attribute(ATTR_EVENT_TYPE, event.__class__.__name__)
```

### 3. Graceful Degradation

eventsource gracefully handles missing OpenTelemetry:

```python
# Works even without OpenTelemetry installed
store = InMemoryEventStore(enable_tracing=True)
# Tracing becomes a no-op when OTEL is not available
```

### 4. Disable High-Frequency Tracing in Production

```python
# High-frequency projection - disable tracing in production
class OrderSummaryProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__(
            enable_tracing=os.getenv("ENVIRONMENT") != "production",
        )
```

### 5. Use BatchSpanProcessor in Production

```python
# Good - batches spans and exports asynchronously
from opentelemetry.sdk.trace.export import BatchSpanProcessor
provider.add_span_processor(BatchSpanProcessor(exporter))

# Bad - blocks on every span (development only)
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
provider.add_span_processor(SimpleSpanProcessor(exporter))
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Full Observability Setup

**Objective:** Add complete observability to an event-sourced application.

**Time:** 30-35 minutes

**Requirements:**

1. Set up OpenTelemetry with Jaeger (or console exporter)
2. Create a shopping cart aggregate with events
3. Create a `CartService` with custom TracingMixin tracing
4. Add standard attributes to your custom spans
5. Trace cart operations end-to-end
6. View traces in Jaeger UI (or console)

**Starter Code:**

```python
"""
Tutorial 20 - Exercise 1: Full Observability Setup

Your task: Add complete observability to a shopping cart application.
"""
import asyncio
from uuid import uuid4

from eventsource.observability import (
    OTEL_AVAILABLE,
    TracingMixin,
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_COUNT,
)

if not OTEL_AVAILABLE:
    print("Install telemetry: pip install eventsource-py[telemetry]")
    exit(1)


# Step 1: Set up OpenTelemetry tracing
# TODO: Configure TracerProvider with ConsoleSpanExporter or JaegerExporter


# Step 2: Define cart events (ItemAdded, ItemRemoved)
# TODO: Create events with @register_event


# Step 3: Create CartAggregate
# TODO: Implement aggregate with state and event handlers


# Step 4: Create CartService with TracingMixin
# TODO: Add custom spans for cart operations


async def main():
    print("=== Exercise 20-1: Full Observability Setup ===\n")

    # Step 5: Create traced components
    # TODO: Create event store, repository, and service

    # Step 6: Perform cart operations
    # TODO: Add items, view cart, verify traces

    # Step 7: Flush traces
    # TODO: Ensure traces are exported


if __name__ == "__main__":
    asyncio.run(main())
```

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 20 - Exercise 1 Solution: Full Observability Setup
"""
import asyncio
from uuid import uuid4

from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)
from eventsource.observability import (
    OTEL_AVAILABLE,
    TracingMixin,
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
)

if not OTEL_AVAILABLE:
    print("Install telemetry: pip install eventsource-py[telemetry]")
    exit(1)


# Step 1: Set up OpenTelemetry tracing
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME

resource = Resource(attributes={SERVICE_NAME: "exercise-20-cart"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)


# Step 2: Define cart events
@register_event
class ItemAdded(DomainEvent):
    event_type: str = "ItemAdded"
    aggregate_type: str = "Cart"
    product: str
    quantity: int = 1


@register_event
class ItemRemoved(DomainEvent):
    event_type: str = "ItemRemoved"
    aggregate_type: str = "Cart"
    product: str


# Step 3: Create CartAggregate
class CartState(BaseModel):
    cart_id: str
    items: dict[str, int] = {}  # product -> quantity


class CartAggregate(AggregateRoot[CartState]):
    aggregate_type = "Cart"

    def _get_initial_state(self) -> CartState:
        return CartState(cart_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if self._state is None:
            self._state = self._get_initial_state()

        if isinstance(event, ItemAdded):
            current = self._state.items.get(event.product, 0)
            new_items = {**self._state.items, event.product: current + event.quantity}
            self._state = self._state.model_copy(update={"items": new_items})
        elif isinstance(event, ItemRemoved):
            new_items = {k: v for k, v in self._state.items.items() if k != event.product}
            self._state = self._state.model_copy(update={"items": new_items})

    def add_item(self, product: str, quantity: int = 1) -> None:
        self.apply_event(ItemAdded(
            aggregate_id=self.aggregate_id,
            product=product,
            quantity=quantity,
            aggregate_version=self.get_next_version(),
        ))

    def remove_item(self, product: str) -> None:
        self.apply_event(ItemRemoved(
            aggregate_id=self.aggregate_id,
            product=product,
            aggregate_version=self.get_next_version(),
        ))


# Step 4: Create CartService with TracingMixin
class CartService(TracingMixin):
    def __init__(self, repo: AggregateRepository, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)
        self._repo = repo

    async def add_to_cart(self, cart_id, product: str, quantity: int = 1) -> CartAggregate:
        with self._create_span_context(
            "cart_service.add_to_cart",
            {
                ATTR_AGGREGATE_ID: str(cart_id),
                "product.name": product,
                "product.quantity": quantity,
            },
        ) as span:
            try:
                cart = await self._repo.load(cart_id)
            except Exception:
                cart = self._repo.create_new(cart_id)

            cart.add_item(product, quantity)
            await self._repo.save(cart)

            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(cart.state.items))

            return cart

    async def remove_from_cart(self, cart_id, product: str) -> CartAggregate:
        with self._create_span_context(
            "cart_service.remove_from_cart",
            {
                ATTR_AGGREGATE_ID: str(cart_id),
                "product.name": product,
            },
        ) as span:
            cart = await self._repo.load(cart_id)
            cart.remove_item(product)
            await self._repo.save(cart)

            return cart

    async def get_cart(self, cart_id) -> CartAggregate:
        with self._create_span_context(
            "cart_service.get_cart",
            {ATTR_AGGREGATE_ID: str(cart_id)},
        ):
            return await self._repo.load(cart_id)


async def main():
    print("=== Exercise 20-1: Full Observability Setup ===\n")

    # Step 5: Create traced components
    print("Step 1: Creating traced components...")
    store = InMemoryEventStore(enable_tracing=True)
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CartAggregate,
        aggregate_type="Cart",
        enable_tracing=True,
    )
    service = CartService(repo, enable_tracing=True)
    print("   Components created with tracing enabled!")

    # Step 6: Perform cart operations
    print("\nStep 2: Performing cart operations...")
    cart_id = uuid4()

    cart = await service.add_to_cart(cart_id, "Laptop", 1)
    print(f"   Added Laptop to cart")

    cart = await service.add_to_cart(cart_id, "Mouse", 2)
    print(f"   Added Mouse (x2) to cart")

    cart = await service.add_to_cart(cart_id, "Keyboard", 1)
    print(f"   Added Keyboard to cart")

    cart = await service.get_cart(cart_id)
    print(f"\nCart contents: {cart.state.items}")
    print(f"Total items: {sum(cart.state.items.values())}")

    # Step 7: Flush traces
    print("\nStep 3: Flushing traces...")
    trace.get_tracer_provider().force_flush()

    print("\n=== Exercise Complete! ===")
    print("\nCheck console output above for span traces.")
    print("For Jaeger: View traces at http://localhost:16686")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is available at: `docs/tutorials/exercises/solutions/20-1.py`

---

## Summary

In this tutorial, you learned:

- **OpenTelemetry provides unified observability** for event-sourced applications
- **All eventsource components have built-in tracing** that is enabled by default
- **TracingMixin enables custom component tracing** with minimal boilerplate
- **Standard attributes ensure consistency** across all spans in your application
- **Exporters send data to backends** like Jaeger, Zipkin, or Grafana Tempo
- **Distributed tracing** automatically propagates context across services
- **Graceful degradation** ensures applications work without OpenTelemetry installed

---

## Key Takeaways

!!! note "Remember"
    - Configure OpenTelemetry once at application startup
    - Use `BatchSpanProcessor` in production (not `SimpleSpanProcessor`)
    - Disable tracing for high-frequency operations to reduce overhead
    - Use standard attributes (ATTR_*) for consistent span metadata

!!! tip "Observability Best Practices"
    - Enable tracing in development for debugging
    - Use standard attributes for consistency
    - Sample high-volume operations in production
    - Correlate traces with logs using trace_id

!!! warning "Common Mistakes"
    - Using SimpleSpanProcessor in production (blocks on every span)
    - Not flushing traces before application exit
    - Enabling tracing for high-frequency projections without sampling
    - Forgetting to call `_init_tracing()` in TracingMixin subclasses

---

## Next Steps

You now understand how to implement comprehensive observability in event-sourced applications. In the final tutorial, you will explore advanced aggregate patterns.

Continue to [Tutorial 21: Advanced Aggregate Patterns](21-advanced-aggregates.md) to complete the series.

---

## Related Documentation

- [Observability Guide](../guides/observability.md) - Comprehensive observability reference
- [Kafka Metrics Guide](../guides/kafka-metrics.md) - Detailed Kafka metrics and alerting
- [Production Guide](../guides/production.md) - Production deployment recommendations
- [Tutorial 15: Production Considerations](15-production.md) - Production concepts
- [Tutorial 18: Kafka Event Bus](18-kafka.md) - Kafka with metrics and tracing
