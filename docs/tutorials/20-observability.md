# Tutorial 20: Observability with OpenTelemetry

**Difficulty:** Advanced

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 7: Event Bus - Distributing Events Across Your System](07-event-bus.md)
- [Tutorial 13: Subscription Management](13-subscriptions.md)
- [Tutorial 15: Production Deployment](15-production.md)
- Python 3.10 or higher
- Understanding of async/await
- Basic observability concepts helpful but not required

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain the three pillars of observability and their role in event-sourced systems
2. Install and configure OpenTelemetry for distributed tracing
3. Use built-in tracing in eventsource-py components
4. Add custom tracing to your domain services with the Tracer protocol
5. Understand span attributes and semantic conventions
6. Integrate logs with trace context for correlation
7. Monitor event-sourced systems with metrics
8. Deploy observability to production environments
9. Debug distributed event flows across services
10. Choose appropriate observability backends (Jaeger, Tempo, Honeycomb)

---

## What is Observability?

**Observability** is the ability to understand what's happening inside your system by examining its outputs. For event-sourced systems, observability is critical because:

- Events flow asynchronously through multiple components
- Business logic spans aggregates, projections, and event buses
- Failures may be subtle (events processed out of order, projections lagging)
- Distributed systems make debugging even harder

### The Three Pillars

Modern observability relies on three complementary data sources:

| Pillar | Purpose | Example |
|--------|---------|---------|
| **Traces** | Track request flow through system | "This order took 450ms: 200ms in aggregate, 250ms in projection" |
| **Metrics** | Monitor system health over time | "Event bus throughput: 1,500 events/sec, 99th percentile latency: 12ms" |
| **Logs** | Record discrete events and errors | "ERROR: Projection failed for OrderPlaced event 123abc" |

**Why all three?** Each pillar answers different questions:
- Traces: "Why is this request slow?"
- Metrics: "Is the system healthy?"
- Logs: "What error occurred at 2:30 PM?"

---

## OpenTelemetry: Industry Standard Observability

**OpenTelemetry** (OTEL) is a vendor-neutral standard for collecting telemetry data. eventsource-py uses OpenTelemetry for:

- **Vendor flexibility**: Send data to Jaeger, Tempo, Honeycomb, Datadog, etc.
- **Consistent instrumentation**: Same APIs across languages and frameworks
- **Ecosystem integration**: Works with existing tools (SQLAlchemy, FastAPI, etc.)
- **Future-proof**: Industry-standard backed by CNCF

### Architecture

```
┌─────────────────┐
│ Your Application│
│  (eventsource)  │
└────────┬────────┘
         │ OpenTelemetry API
         ▼
┌─────────────────┐
│ OpenTelemetry   │
│    SDK          │
└────────┬────────┘
         │ OTLP Protocol
         ▼
┌─────────────────┐      ┌─────────────────┐
│  Jaeger         │  or  │  Grafana Tempo  │  or  Honeycomb, Datadog...
│  (Backend)      │      │  (Backend)      │
└─────────────────┘      └─────────────────┘
```

---

## Installation

Install OpenTelemetry support for eventsource-py:

```bash
# Base telemetry support
pip install eventsource-py[telemetry]

# Specific exporters (choose based on your backend)
pip install opentelemetry-exporter-jaeger
pip install opentelemetry-exporter-otlp
```

The `[telemetry]` extra installs:
- `opentelemetry-api`: Core API for creating spans
- `opentelemetry-sdk`: Default implementation

### Verify Installation

```python
from eventsource.observability import OTEL_AVAILABLE

if OTEL_AVAILABLE:
    print("OpenTelemetry is available!")
else:
    print("OpenTelemetry not installed")
    print("Install with: pip install eventsource-py[telemetry]")
```

**Important:** eventsource-py gracefully degrades when OpenTelemetry is not installed. All components work normally; tracing is simply disabled.

---

## Setting Up OpenTelemetry

Before using tracing, configure an OpenTelemetry `TracerProvider`. This tells OTEL where to send spans.

### Console Exporter (Development)

For local development, export spans to console for debugging:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

# Create provider
provider = TracerProvider()

# Add console exporter
console_exporter = ConsoleSpanExporter()
provider.add_span_processor(BatchSpanProcessor(console_exporter))

# Set as global provider
trace.set_tracer_provider(provider)

print("Console tracing enabled - spans will print to stdout")
```

Spans will print as JSON to stdout when operations complete.

### Jaeger Exporter (Production)

For production monitoring with Jaeger:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

# Create resource with service name
resource = Resource(attributes={
    SERVICE_NAME: "order-service",
})

# Create provider
provider = TracerProvider(resource=resource)

# Configure Jaeger exporter
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)
provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

# Set as global provider
trace.set_tracer_provider(provider)

print("Jaeger tracing enabled - view spans at http://localhost:16686")
```

### OTLP Exporter (Grafana Tempo, Honeycomb)

For OTLP-compatible backends like Grafana Tempo:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Create resource
resource = Resource(attributes={
    SERVICE_NAME: "order-service",
    "service.version": "1.0.0",
    "deployment.environment": "production",
})

# Create provider
provider = TracerProvider(resource=resource)

# Configure OTLP exporter
otlp_exporter = OTLPSpanExporter(
    endpoint="http://tempo:4317",
    insecure=True,  # Use TLS in production
)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

# Set as global provider
trace.set_tracer_provider(provider)
```

For Honeycomb, add your API key:

```python
otlp_exporter = OTLPSpanExporter(
    endpoint="https://api.honeycomb.io:443",
    headers={
        "x-honeycomb-team": "YOUR_API_KEY",
    },
)
```

---

## Built-In Tracing

eventsource-py components have built-in OpenTelemetry tracing. When `enable_tracing=True` (the default), components automatically create spans for operations.

### Event Stores

Event stores trace database operations:

```python
from eventsource import InMemoryEventStore

# Tracing enabled by default
event_store = InMemoryEventStore(enable_tracing=True)

# Operations automatically traced:
# - eventsource.event_store.append_events
# - eventsource.event_store.get_events
```

**Traced operations:**
- `append_events`: Appending events to the store
- `get_events`: Retrieving events by aggregate

**Span attributes:**
- `eventsource.aggregate.id`: Aggregate UUID
- `eventsource.aggregate.type`: Aggregate type name
- `eventsource.event.count`: Number of events
- `db.system`: Database system (e.g., "sqlite", "postgresql")

### Repositories

Repositories trace aggregate lifecycle operations:

```python
from eventsource import AggregateRepository

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=True,  # Default
)

# Operations automatically traced:
# - eventsource.repository.load
# - eventsource.repository.save
# - eventsource.repository.exists
```

**Traced operations:**
- `load`: Loading aggregate from events
- `save`: Saving aggregate events
- `exists`: Checking aggregate existence

**Span attributes:**
- `eventsource.aggregate.id`: Aggregate UUID
- `eventsource.aggregate.type`: Aggregate type name
- `eventsource.version`: Current aggregate version
- `eventsource.event.count`: Events in this save

### Event Buses

Event buses trace message publishing and consumption:

```python
from eventsource.bus import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://localhost:5672/",
    exchange_name="events",
    consumer_group="projections",
    enable_tracing=True,  # Default
)
bus = RabbitMQEventBus(config=config, event_registry=registry)

# Operations automatically traced:
# - eventsource.event_bus.publish (PRODUCER span)
# - eventsource.event_bus.consume (CONSUMER span)
# - eventsource.event_bus.handle (INTERNAL span)
```

**Traced operations:**
- `publish`: Publishing events to the bus
- `consume`: Consuming events from the bus
- `handle`: Invoking event handlers

**Span attributes:**
- `eventsource.event.type`: Event type name
- `eventsource.event.count`: Number of events
- `messaging.system`: Messaging system (e.g., "rabbitmq", "kafka")
- `messaging.destination`: Queue/topic name

### Subscription Manager

The SubscriptionManager traces subscription operations:

```python
from eventsource.subscriptions import SubscriptionManager

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    enable_tracing=True,  # Default
)

# Operations automatically traced:
# - eventsource.subscription.poll_batch
# - eventsource.subscription.process_batch
# - eventsource.subscription.save_checkpoint
```

**Span attributes:**
- `eventsource.subscription.name`: Subscription name
- `eventsource.subscription.state`: Current state (catching_up, live)
- `eventsource.batch.size`: Batch size
- `eventsource.from_position`: Starting position
- `eventsource.to_position`: Ending position

### Disabling Tracing

Disable tracing for high-throughput components or performance testing:

```python
# Disable for specific components
event_store = InMemoryEventStore(enable_tracing=False)
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=False,
)

# Tracing is lightweight but has small overhead
# Disable for:
# - Performance benchmarks
# - Very high throughput paths (>10k ops/sec)
# - Components that create too many spans
```

---

## Standard Span Attributes

eventsource-py defines standard attribute constants for consistent span metadata. Use these instead of hardcoding strings.

### Attribute Constants

```python
from eventsource.observability import (
    ATTR_AGGREGATE_ID,      # "eventsource.aggregate.id"
    ATTR_AGGREGATE_TYPE,    # "eventsource.aggregate.type"
    ATTR_EVENT_TYPE,        # "eventsource.event.type"
    ATTR_EVENT_COUNT,       # "eventsource.event.count"
    ATTR_VERSION,           # "eventsource.version"
    ATTR_DB_SYSTEM,         # "db.system" (OTEL semantic)
    ATTR_MESSAGING_SYSTEM,  # "messaging.system" (OTEL semantic)
)
```

### Using Attributes

```python
from eventsource.observability import get_tracer, ATTR_AGGREGATE_ID, ATTR_EVENT_TYPE

tracer = get_tracer(__name__)

if tracer:
    with tracer.start_as_current_span("process_order") as span:
        span.set_attribute(ATTR_AGGREGATE_ID, str(order_id))
        span.set_attribute(ATTR_EVENT_TYPE, "OrderPlaced")
        span.set_attribute("order.total", 99.99)

        # Process order...
```

**Why use constants?**
- Consistent naming across your codebase
- Typo prevention (compile-time checks)
- OpenTelemetry semantic conventions compliance
- Easy to grep/search for attribute usage

### Semantic Conventions

Some attributes follow **OpenTelemetry Semantic Conventions** - standardized names for common telemetry:

**Database:**
- `db.system`: Database type (e.g., "postgresql", "sqlite")
- `db.name`: Database name
- `db.operation`: Operation type (e.g., "SELECT", "INSERT")

**Messaging:**
- `messaging.system`: Messaging system (e.g., "rabbitmq", "kafka")
- `messaging.destination`: Queue/topic name
- `messaging.operation`: Operation (e.g., "publish", "receive")

These conventions enable vendor tools to understand your spans automatically.

---

## Adding Custom Tracing

Add tracing to your custom domain services and components using the `Tracer` protocol.

### Using the Tracer Protocol

The `Tracer` protocol provides composition-based tracing:

```python
from eventsource.observability import create_tracer, Tracer, ATTR_AGGREGATE_ID

class OrderService:
    """Domain service for order operations."""

    def __init__(self, repo: AggregateRepository, enable_tracing: bool = True):
        self._repo = repo
        self._tracer: Tracer = create_tracer(__name__, enable_tracing)

    async def place_order(self, customer_id: str, items: list) -> OrderAggregate:
        """Place a new order with distributed tracing."""
        with self._tracer.span("order_service.place_order") as span:
            order_id = uuid4()

            # Add span attributes
            if span:
                span.set_attribute(ATTR_AGGREGATE_ID, str(order_id))
                span.set_attribute("customer.id", customer_id)
                span.set_attribute("items.count", len(items))

            # Business logic
            order = self._repo.create_new(order_id)
            order.place(customer_id, items)
            await self._repo.save(order)

            if span:
                span.set_attribute("order.total", order.state.total)

            return order
```

### Using the @traced Decorator

For methods with static attributes, use the `@traced` decorator:

```python
from eventsource.observability import traced, create_tracer

class NotificationService:
    """Service for sending notifications."""

    def __init__(self, enable_tracing: bool = True):
        self._tracer = create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

    @traced("notification_service.send_email", attributes={"channel": "email"})
    async def send_email(self, recipient: str, subject: str, body: str) -> None:
        """Send email notification - automatically traced."""
        await self._email_client.send(recipient, subject, body)

    @traced("notification_service.send_sms", attributes={"channel": "sms"})
    async def send_sms(self, phone: str, message: str) -> None:
        """Send SMS notification - automatically traced."""
        await self._sms_client.send(phone, message)
```

**Requirements for @traced:**
- Class must have `_tracer` attribute (from `create_tracer()`)
- Class must have `_enable_tracing` attribute (boolean)
- Decorator is a no-op if tracing disabled (zero overhead)

### Dynamic Span Attributes

For attributes that depend on method arguments, use manual span creation:

```python
from eventsource.observability import create_tracer, ATTR_AGGREGATE_ID, ATTR_EVENT_TYPE

class PaymentProcessor:
    def __init__(self, enable_tracing: bool = True):
        self._tracer = create_tracer(__name__, enable_tracing)

    async def process_payment(
        self,
        order_id: UUID,
        amount: float,
        payment_method: str
    ) -> PaymentResult:
        """Process payment with dynamic tracing attributes."""
        with self._tracer.span(
            "payment_processor.process_payment",
            attributes={
                ATTR_AGGREGATE_ID: str(order_id),
                "payment.amount": amount,
                "payment.method": payment_method,
            }
        ) as span:
            result = await self._gateway.charge(amount, payment_method)

            if span:
                span.set_attribute("payment.success", result.success)
                span.set_attribute("payment.transaction_id", result.transaction_id)

            return result
```

### MockTracer for Testing

Use `MockTracer` in tests to verify tracing behavior without OpenTelemetry:

```python
from eventsource.observability import MockTracer

def test_order_service_tracing():
    """Verify OrderService creates correct spans."""
    tracer = MockTracer()
    service = OrderService(repo)
    service._tracer = tracer

    # Execute operation
    await service.place_order("cust-123", [{"sku": "WIDGET", "qty": 2}])

    # Verify spans created
    assert "order_service.place_order" in tracer.span_names

    # Verify attributes
    span_name, attrs = tracer.spans[0]
    assert attrs["customer.id"] == "cust-123"
    assert attrs["items.count"] == 1
```

---

## Distributed Tracing

Distributed tracing tracks requests across multiple services. Event buses in eventsource-py automatically propagate trace context using the **W3C Trace Context** standard.

### How It Works

```
┌──────────────┐                      ┌──────────────┐
│  Service A   │                      │  Service B   │
│              │                      │              │
│  Publisher   │─────────────────────>│  Consumer    │
│  (PRODUCER   │   Event + Context    │  (CONSUMER   │
│   span)      │                      │   span)      │
└──────────────┘                      └──────────────┘
       │                                     │
       │                                     │
       └─────────────────┬───────────────────┘
                         │
                    Same Trace
                (parent-child relationship)
```

### Automatic Context Propagation

RabbitMQ and Kafka event buses automatically propagate trace context:

```python
# Service A: Publisher
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://localhost:5672/",
    exchange_name="events",
    consumer_group="publisher",
    enable_tracing=True,
)
publisher_bus = RabbitMQEventBus(config=config, event_registry=registry)

await publisher_bus.connect()

# Publish event - trace context injected into message headers
event = OrderPlaced(
    aggregate_id=uuid4(),
    customer_id="cust-123",
    total_amount=99.99,
    aggregate_version=1,
)
await publisher_bus.publish([event])
# Creates PRODUCER span, adds traceparent header to message

# Service B: Consumer
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://localhost:5672/",
    exchange_name="events",
    consumer_group="projections",
    enable_tracing=True,
)
consumer_bus = RabbitMQEventBus(config=config, event_registry=registry)

async def update_projection(event: OrderPlaced):
    # This handler runs in a CONSUMER span
    # Span is automatically linked to publisher's PRODUCER span
    pass

consumer_bus.subscribe(OrderPlaced, update_projection)
await consumer_bus.start_consuming()
# Consumer span becomes child of producer span
```

### Viewing Distributed Traces

In Jaeger, you'll see:

```
Trace: place-order-workflow (450ms)
├─ order_service.place_order (200ms)
│  └─ eventsource.repository.save (50ms)
│     └─ eventsource.event_store.append_events (20ms)
│
├─ eventsource.event_bus.publish (10ms) [PRODUCER]
│
└─ eventsource.event_bus.consume (240ms) [CONSUMER]
   └─ projection.handle (230ms)
      └─ database.update (200ms)
```

This shows the entire flow: order placed → event stored → event published → projection updated.

---

## Logging Integration

Integrate logs with traces for correlated debugging. Add trace IDs to log records so you can find all logs for a specific trace.

### Trace Context Filter

Add trace context to log records:

```python
import logging
from opentelemetry import trace

class TraceContextFilter(logging.Filter):
    """Add trace_id and span_id to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add trace context to every log record."""
        span = trace.get_current_span()
        ctx = span.get_span_context()

        if ctx.is_valid:
            record.trace_id = format(ctx.trace_id, "032x")
            record.span_id = format(ctx.span_id, "016x")
        else:
            record.trace_id = ""
            record.span_id = ""

        return True

# Configure logging
handler = logging.StreamHandler()
handler.addFilter(TraceContextFilter())
handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] trace_id=%(trace_id)s span_id=%(span_id)s - %(message)s"
))

logger = logging.getLogger("eventsource")
logger.addHandler(handler)
logger.setLevel(logging.INFO)
```

### Structured Logging with Trace Context

For production, use JSON logging with trace context:

```python
import json
import logging
from datetime import datetime, UTC
from opentelemetry import trace

class JSONFormatter(logging.Formatter):
    """Format logs as JSON with trace context."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        span = trace.get_current_span()
        ctx = span.get_span_context()

        log_data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add trace context
        if ctx.is_valid:
            log_data["trace_id"] = format(ctx.trace_id, "032x")
            log_data["span_id"] = format(ctx.span_id, "016x")

        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add custom fields from extra={}
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName",
                "relativeCreated", "thread", "threadName", "exc_info",
                "exc_text", "stack_info",
            }:
                log_data[key] = value

        return json.dumps(log_data)

# Configure
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())

logger = logging.getLogger("eventsource")
logger.addHandler(handler)
logger.setLevel(logging.INFO)
```

**Example output:**

```json
{
  "timestamp": "2024-01-15T12:00:00.123456Z",
  "level": "INFO",
  "logger": "eventsource.stores.postgresql",
  "message": "Appended 3 events to aggregate",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "aggregate_id": "550e8400-e29b-41d4-a716-446655440000",
  "aggregate_type": "Order",
  "event_count": 3
}
```

### Correlating Logs and Traces

With trace IDs in logs, you can:

1. **Find trace from log**: Copy trace_id from error log → paste into Jaeger → see full trace
2. **Find logs from trace**: Copy trace_id from Jaeger span → search logs for trace_id → see all related logs

This correlation is invaluable for debugging production issues.

---

## Metrics

While tracing tracks individual requests, metrics track system health over time. eventsource-py uses OpenTelemetry metrics where applicable.

### Kafka Event Bus Metrics

The KafkaEventBus emits comprehensive metrics:

```python
from eventsource.bus import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="projections",
    enable_metrics=True,  # Default
    enable_tracing=True,
)
bus = KafkaEventBus(config=config, event_registry=registry)
```

**Available metrics:**

**Counters:**
- `kafka.eventbus.messages.published`: Total messages published
- `kafka.eventbus.messages.consumed`: Total messages consumed
- `kafka.eventbus.handler.invocations`: Handler invocation count
- `kafka.eventbus.handler.errors`: Handler error count
- `kafka.eventbus.messages.dlq`: Messages sent to DLQ

**Histograms:**
- `kafka.eventbus.publish.duration`: Publish latency (ms)
- `kafka.eventbus.consume.duration`: Consume latency (ms)
- `kafka.eventbus.handler.duration`: Handler execution time (ms)

**Gauges:**
- `kafka.eventbus.consumer.lag`: Messages behind per partition

### Setting Up Metrics Export

Configure OpenTelemetry metrics export to Prometheus:

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

# Start Prometheus HTTP server
start_http_server(port=8000)

# Configure OpenTelemetry
metrics.set_meter_provider(
    MeterProvider(metric_readers=[PrometheusMetricReader()])
)

print("Metrics available at http://localhost:8000/metrics")
```

Or export to OTLP collector:

```python
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://otel-collector:4317"),
    export_interval_millis=60000,  # Export every 60 seconds
)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
```

### Example PromQL Queries

Monitor your event-sourced system with these queries:

```promql
# Event publishing rate (events/sec)
rate(kafka_eventbus_messages_published_total[1m])

# Consumer lag (messages behind)
kafka_eventbus_consumer_lag

# Handler error rate
rate(kafka_eventbus_handler_errors_total[5m])

# 99th percentile handler latency
histogram_quantile(0.99, rate(kafka_eventbus_handler_duration_bucket[5m]))

# Messages sent to DLQ
increase(kafka_eventbus_messages_dlq_total[1h])
```

For more on metrics, see the [Kafka Event Bus tutorial](18-kafka.md#opentelemetry-metrics).

---

## Running Jaeger Locally

Jaeger is a popular open-source distributed tracing backend. Run it locally with Docker:

### Docker Compose Setup

Create `docker-compose.jaeger.yml`:

```yaml
version: '3.8'

services:
  jaeger:
    image: jaegertracing/all-in-one:1.51
    container_name: jaeger
    ports:
      - "6831:6831/udp"   # Jaeger Thrift compact
      - "16686:16686"     # Jaeger UI
      - "4317:4317"       # OTLP gRPC
      - "4318:4318"       # OTLP HTTP
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
      SPAN_STORAGE_TYPE: "badger"
      BADGER_EPHEMERAL: "false"
      BADGER_DIRECTORY_VALUE: "/badger/data"
      BADGER_DIRECTORY_KEY: "/badger/key"
    volumes:
      - jaeger_data:/badger

volumes:
  jaeger_data:
```

Start Jaeger:

```bash
docker compose -f docker-compose.jaeger.yml up -d

# Check status
docker ps | grep jaeger

# View logs
docker logs jaeger
```

Access Jaeger UI at http://localhost:16686

### Using Jaeger

1. **Run your application** with Jaeger exporter configured
2. **Generate some load** (create orders, trigger projections)
3. **Open Jaeger UI** at http://localhost:16686
4. **Select service** from dropdown (e.g., "order-service")
5. **Click "Find Traces"**
6. **Click on a trace** to see the full span tree

**What to look for:**
- Total trace duration (end-to-end latency)
- Span durations (which operations are slow?)
- Span relationships (parent-child, follows-from)
- Span attributes (aggregate IDs, event types)
- Errors (spans with error status)

---

## Complete Example

Let's put it all together with a complete example showing tracing, logging, and metrics:

```python
"""
Tutorial 20: Observability with OpenTelemetry

Demonstrates:
- OpenTelemetry tracing setup
- Built-in tracing in components
- Custom tracing in domain services
- Log integration with trace context
- Distributed tracing across event bus

Run with: python tutorial_20_observability.py
"""

import asyncio
import logging
import json
from datetime import datetime, UTC
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    EventRegistry,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)
from eventsource.observability import (
    OTEL_AVAILABLE,
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_TYPE,
    ATTR_EVENT_COUNT,
    create_tracer,
    Tracer,
)

if not OTEL_AVAILABLE:
    print("ERROR: OpenTelemetry not installed")
    print("Install with: pip install eventsource-py[telemetry]")
    exit(1)

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME


# =============================================================================
# OpenTelemetry Configuration
# =============================================================================

def setup_tracing_console() -> None:
    """Configure OpenTelemetry to export spans to console."""
    resource = Resource(attributes={SERVICE_NAME: "tutorial-20-observability"})
    provider = TracerProvider(resource=resource)

    console_exporter = ConsoleSpanExporter()
    provider.add_span_processor(BatchSpanProcessor(console_exporter))

    trace.set_tracer_provider(provider)
    print("Console tracing enabled")


def setup_tracing_jaeger() -> None:
    """Configure OpenTelemetry to export spans to Jaeger."""
    try:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    except ImportError:
        print("Jaeger exporter not installed, falling back to console")
        setup_tracing_console()
        return

    resource = Resource(attributes={SERVICE_NAME: "tutorial-20-observability"})
    provider = TracerProvider(resource=resource)

    jaeger_exporter = JaegerExporter(
        agent_host_name="localhost",
        agent_port=6831,
    )
    provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

    trace.set_tracer_provider(provider)
    print("Jaeger tracing enabled - view at http://localhost:16686")


# =============================================================================
# Logging Configuration
# =============================================================================

class TraceContextFilter(logging.Filter):
    """Add trace_id and span_id to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        span = trace.get_current_span()
        ctx = span.get_span_context()

        if ctx.is_valid:
            record.trace_id = format(ctx.trace_id, "032x")
            record.span_id = format(ctx.span_id, "016x")
        else:
            record.trace_id = ""
            record.span_id = ""

        return True


def setup_logging() -> None:
    """Configure logging with trace context."""
    handler = logging.StreamHandler()
    handler.addFilter(TraceContextFilter())
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] trace=%(trace_id)s - %(message)s"
    ))

    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    print("Logging with trace context enabled")


# =============================================================================
# Domain Model
# =============================================================================

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str


class OrderState(BaseModel):
    order_id: UUID
    customer_id: str | None = None
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                total_amount=event.total_amount,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def place(self, customer_id: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        self.apply_event(OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Domain Service with Custom Tracing
# =============================================================================

class OrderService:
    """
    Domain service with custom OpenTelemetry tracing.

    Demonstrates composition-based tracing using the Tracer protocol.
    """

    def __init__(self, repo: AggregateRepository, enable_tracing: bool = True):
        self._repo = repo
        self._tracer: Tracer = create_tracer(__name__, enable_tracing)
        self._logger = logging.getLogger(__name__)

    async def place_order(self, customer_id: str, total_amount: float) -> OrderAggregate:
        """Place a new order with distributed tracing."""
        with self._tracer.span(
            "order_service.place_order",
            attributes={
                "customer.id": customer_id,
                "order.amount": total_amount,
            }
        ) as span:
            self._logger.info(
                f"Placing order for customer {customer_id}, amount ${total_amount}"
            )

            order_id = uuid4()

            # Add span attributes
            if span:
                span.set_attribute(ATTR_AGGREGATE_ID, str(order_id))
                span.set_attribute(ATTR_AGGREGATE_TYPE, "Order")

            # Business logic
            order = self._repo.create_new(order_id)
            order.place(customer_id, total_amount)
            await self._repo.save(order)

            if span:
                span.set_attribute(ATTR_EVENT_COUNT, 1)

            self._logger.info(f"Order placed successfully: {order_id}")

            return order

    async def ship_order(self, order_id: UUID, tracking_number: str) -> OrderAggregate:
        """Ship an existing order with tracing."""
        with self._tracer.span(
            "order_service.ship_order",
            attributes={
                ATTR_AGGREGATE_ID: str(order_id),
                "shipping.tracking_number": tracking_number,
            }
        ) as span:
            self._logger.info(f"Shipping order {order_id}, tracking: {tracking_number}")

            # Load and ship
            order = await self._repo.load(order_id)
            order.ship(tracking_number)
            await self._repo.save(order)

            if span:
                span.set_attribute("order.status", order.state.status)

            self._logger.info(f"Order shipped: {order_id}")

            return order


# =============================================================================
# Projection with Tracing
# =============================================================================

class OrderStatisticsProjection:
    """Projection that tracks order statistics."""

    def __init__(self):
        self.total_orders = 0
        self.total_revenue = 0.0
        self.shipped_count = 0
        self._logger = logging.getLogger(__name__)

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        """Handle events - runs in traced context from event bus."""
        if isinstance(event, OrderPlaced):
            self.total_orders += 1
            self.total_revenue += event.total_amount
            self._logger.info(
                f"Order placed: ${event.total_amount:.2f}, "
                f"total revenue: ${self.total_revenue:.2f}"
            )

        elif isinstance(event, OrderShipped):
            self.shipped_count += 1
            self._logger.info(
                f"Order shipped: {event.tracking_number}, "
                f"total shipped: {self.shipped_count}"
            )


# =============================================================================
# Demo Functions
# =============================================================================

async def demo_basic_tracing():
    """Demo 1: Basic built-in tracing."""
    print("\n" + "=" * 60)
    print("Demo 1: Built-In Component Tracing")
    print("=" * 60 + "\n")

    # Create infrastructure with tracing enabled
    event_store = InMemoryEventStore(enable_tracing=True)
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        enable_tracing=True,
    )

    # Create and save order
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.place(customer_id="cust-123", total_amount=99.99)
    await repo.save(order)

    # Load order
    loaded = await repo.load(order_id)

    print(f"Order {order_id} placed and loaded successfully")
    print(f"Status: {loaded.state.status}, Total: ${loaded.state.total_amount:.2f}")
    print("\nCheck console output for traced spans from:")
    print("  - eventsource.repository.save")
    print("  - eventsource.event_store.append_events")
    print("  - eventsource.repository.load")
    print("  - eventsource.event_store.get_events")


async def demo_custom_tracing():
    """Demo 2: Custom service tracing."""
    print("\n" + "=" * 60)
    print("Demo 2: Custom Service Tracing")
    print("=" * 60 + "\n")

    # Create infrastructure
    event_store = InMemoryEventStore(enable_tracing=True)
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        enable_tracing=True,
    )

    # Create service with custom tracing
    service = OrderService(repo, enable_tracing=True)

    # Use service - creates custom spans
    order = await service.place_order("cust-premium-456", 499.99)
    await service.ship_order(order.aggregate_id, "TRACK-XYZ-123")

    print(f"Order {order.aggregate_id} placed and shipped")
    print("\nCheck console output for custom spans:")
    print("  - order_service.place_order")
    print("  - order_service.ship_order")
    print("\nNote: Custom spans include repository and event store spans as children")


async def demo_distributed_tracing():
    """Demo 3: Distributed tracing across event bus."""
    print("\n" + "=" * 60)
    print("Demo 3: Distributed Tracing Across Event Bus")
    print("=" * 60 + "\n")

    # Create infrastructure
    event_store = InMemoryEventStore(enable_tracing=True)
    event_bus = InMemoryEventBus(enable_tracing=True)
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
        enable_tracing=True,
    )

    # Create projection
    stats = OrderStatisticsProjection()
    event_bus.subscribe(OrderPlaced, stats.handle)
    event_bus.subscribe(OrderShipped, stats.handle)

    # Create service
    service = OrderService(repo, enable_tracing=True)

    # Execute workflow
    order = await service.place_order("cust-789", 299.99)
    await service.ship_order(order.aggregate_id, "TRACK-ABC-456")

    print(f"Order {order.aggregate_id} processed through event bus")
    print(f"\nProjection statistics:")
    print(f"  Total orders: {stats.total_orders}")
    print(f"  Total revenue: ${stats.total_revenue:.2f}")
    print(f"  Shipped count: {stats.shipped_count}")
    print("\nCheck console output for distributed trace:")
    print("  - order_service.place_order")
    print("    └─ eventsource.repository.save")
    print("       └─ eventsource.event_bus.publish (PRODUCER)")
    print("          └─ eventsource.event_bus.handle (CONSUMER)")
    print("             └─ projection.handle")


async def demo_trace_context_logging():
    """Demo 4: Log correlation with trace context."""
    print("\n" + "=" * 60)
    print("Demo 4: Log Correlation with Trace Context")
    print("=" * 60 + "\n")

    # Create infrastructure
    event_store = InMemoryEventStore(enable_tracing=True)
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        enable_tracing=True,
    )

    service = OrderService(repo, enable_tracing=True)

    # Execute operations - logs will include trace_id
    order1 = await service.place_order("cust-001", 100.00)
    order2 = await service.place_order("cust-002", 200.00)
    await service.ship_order(order1.aggregate_id, "TRACK-001")

    print("\nCheck log output above - each log line includes trace_id")
    print("Logs from the same operation share the same trace_id")
    print("\nIn production:")
    print("  1. Find trace_id in error log")
    print("  2. Search all logs for that trace_id")
    print("  3. See complete picture of what happened")
    print("  4. Search Jaeger for same trace_id to see spans")


async def main():
    """Run all demos."""
    print("=" * 60)
    print("Tutorial 20: Observability with OpenTelemetry")
    print("=" * 60)

    # Setup
    setup_logging()
    setup_tracing_console()
    # setup_tracing_jaeger()  # Uncomment if Jaeger is running

    # Run demos
    await demo_basic_tracing()
    await demo_custom_tracing()
    await demo_distributed_tracing()
    await demo_trace_context_logging()

    # Flush traces
    print("\n" + "=" * 60)
    print("Tutorial Complete!")
    print("=" * 60)
    print("\nFlushing traces...")

    provider = trace.get_tracer_provider()
    if hasattr(provider, 'force_flush'):
        provider.force_flush()

    await asyncio.sleep(1)  # Give time for export
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_20_observability.py` and run:

```bash
python tutorial_20_observability.py
```

**Expected output:**

```
Logging with trace context enabled
Console tracing enabled
============================================================
Tutorial 20: Observability with OpenTelemetry
============================================================

============================================================
Demo 1: Built-In Component Tracing
============================================================

2024-01-15 12:00:00 [INFO] trace=4bf92f3577b34da6a3ce929d0e0e4736 - Placing order for customer cust-123, amount $99.99
Order 550e8400-e29b-41d4-a716-446655440000 placed and loaded successfully
Status: placed, Total: $99.99

Check console output for traced spans from:
  - eventsource.repository.save
  - eventsource.event_store.append_events
  - eventsource.repository.load
  - eventsource.event_store.get_events

{
  "name": "eventsource.repository.save",
  "context": {...},
  "attributes": {
    "eventsource.aggregate.id": "550e8400-e29b-41d4-a716-446655440000",
    "eventsource.aggregate.type": "Order",
    "eventsource.event.count": 1
  },
  ...
}

============================================================
Demo 2: Custom Service Tracing
============================================================

2024-01-15 12:00:01 [INFO] trace=a8b7c6d5e4f3a2b1c9d8e7f6a5b4c3d2 - Placing order for customer cust-premium-456, amount $499.99
2024-01-15 12:00:01 [INFO] trace=a8b7c6d5e4f3a2b1c9d8e7f6a5b4c3d2 - Order placed successfully: 661f9500-f3ac-52e5-b827-557766551111
...

Tutorial Complete!
```

---

## Production Deployment

Deploy observability to production with these best practices:

### Environment-Based Configuration

```python
import os

def setup_production_tracing():
    """Configure tracing based on environment."""
    environment = os.getenv("ENVIRONMENT", "development")
    service_name = os.getenv("SERVICE_NAME", "eventsource-app")
    otlp_endpoint = os.getenv("OTLP_ENDPOINT", "http://otel-collector:4317")

    if environment == "production":
        # Production: Use OTLP exporter
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        resource = Resource(attributes={
            SERVICE_NAME: service_name,
            "service.version": os.getenv("SERVICE_VERSION", "1.0.0"),
            "deployment.environment": environment,
        })
        provider = TracerProvider(resource=resource)

        exporter = OTLPSpanExporter(
            endpoint=otlp_endpoint,
            insecure=os.getenv("OTLP_INSECURE", "false").lower() == "true",
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))

    else:
        # Development: Use console exporter
        provider = TracerProvider()
        provider.add_span_processor(
            BatchSpanProcessor(ConsoleSpanExporter())
        )

    trace.set_tracer_provider(provider)
```

### Selective Tracing

Disable tracing for high-throughput paths:

```python
# High-throughput event store - disable tracing
event_store = PostgreSQLEventStore(
    session_factory,
    enable_tracing=False,  # Saves ~5-10% overhead
)

# Critical business logic - keep tracing enabled
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=True,  # Important for debugging
)

# Event bus - enable for distributed tracing
event_bus = KafkaEventBus(
    config=config,
    event_registry=registry,
    enable_tracing=True,  # Critical for cross-service traces
)
```

### Sampling

For very high traffic, use trace sampling:

```python
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

# Sample 10% of traces
sampler = TraceIdRatioBased(0.1)

provider = TracerProvider(
    resource=resource,
    sampler=sampler,
)
```

**When to sample:**
- Production systems with >1000 req/sec
- Cost concerns with commercial backends
- Storage constraints

**What to avoid:**
- Don't sample error traces (use ParentBased sampler)
- Don't sample critical paths (authentication, payments)
- Don't over-sample (you'll miss rare bugs)

---

## Key Takeaways

1. **OpenTelemetry is the industry standard** for distributed tracing and observability
2. **eventsource-py has built-in tracing** - just enable and configure an exporter
3. **Tracer protocol enables composition-based tracing** - inject tracers into your services
4. **Standard attributes ensure consistency** - use constants like ATTR_AGGREGATE_ID
5. **Distributed tracing works automatically** - event buses propagate trace context
6. **Log integration is critical** - correlate logs and traces with trace_id
7. **Metrics complement traces** - use both for complete observability
8. **Jaeger is easy to run locally** - great for development and testing
9. **Production requires careful configuration** - use OTLP, sampling, and selective tracing
10. **Observability is not optional** - essential for debugging distributed event-sourced systems

---

## Next Steps

Continue to [Tutorial 21: Advanced Aggregate Patterns](21-advanced-aggregates.md) to learn about:
- Aggregate versioning strategies
- Complex aggregate hierarchies
- Process managers and sagas
- Event upcasting and schema evolution

For more on observability:
- [Tutorial 15: Production Deployment](15-production.md) - Health checks and monitoring
- [Tutorial 18: Kafka Event Bus](18-kafka.md) - Metrics and monitoring
- Examples: `examples/projection_example.py`
- Tests: `tests/integration/observability/`

---

## Additional Resources

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [OpenTelemetry Python](https://opentelemetry-python.readthedocs.io/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [Grafana Tempo](https://grafana.com/oss/tempo/)
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)
- `src/eventsource/observability/` - eventsource-py observability implementation
