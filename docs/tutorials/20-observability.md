# Tutorial 20: Observability with OpenTelemetry

**Difficulty:** Advanced
**Progress:** Tutorial 20 of 21 | Phase 4: Advanced Patterns

Learn how to implement comprehensive observability using OpenTelemetry in event-sourced applications. This tutorial covers tracing configuration, built-in instrumentation, and distributed tracing across services.

## Prerequisites

- Completed [Tutorial 18: Using Kafka Event Bus](18-kafka.md)
- Completed [Tutorial 15: Production Considerations](15-production.md)
- Python 3.11+ installed
- Optional: Jaeger running locally (Docker recommended)

## Three Pillars of Observability

eventsource-py supports all three observability pillars:

| Pillar | Technology | Purpose |
|--------|------------|---------|
| **Tracing** | OpenTelemetry | Distributed request tracking, latency analysis |
| **Metrics** | OpenTelemetry | Throughput, error rates, lag monitoring |
| **Logging** | Python `logging` | Structured operational logs |

## Installation

```bash
# Base telemetry support
pip install eventsource-py[telemetry]

# Exporters
pip install opentelemetry-exporter-jaeger
pip install opentelemetry-exporter-otlp
pip install opentelemetry-exporter-zipkin
```

Verify installation:

```python
from eventsource.observability import OTEL_AVAILABLE

if OTEL_AVAILABLE:
    print("OpenTelemetry is available!")
else:
    print("OpenTelemetry not available. Install with: pip install eventsource-py[telemetry]")
```

## OpenTelemetry Setup

### Console Exporter (Development)

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
```

### Jaeger Exporter (Production)

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

resource = Resource(attributes={SERVICE_NAME: "order-service"})
provider = TracerProvider(resource=resource)
jaeger_exporter = JaegerExporter(agent_host_name="localhost", agent_port=6831)
provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
trace.set_tracer_provider(provider)
```

### OTLP Exporter

```python
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Grafana Tempo
otlp_exporter = OTLPSpanExporter(endpoint="http://tempo:4317", insecure=True)

# Honeycomb
# otlp_exporter = OTLPSpanExporter(
#     endpoint="https://api.honeycomb.io:443",
#     headers={"x-honeycomb-team": "YOUR_API_KEY"},
# )
```

## Traced Components

**Event Stores:** `append_events`, `get_events`
**Repositories:** `load`, `save`, `exists`, `create_snapshot`
**Event Buses:** `publish`, `consume`, `dispatch`, `handle`
**Snapshot Stores:** `save`, `get`, `delete`, `exists`
**Projections:** `handle`, `handler` (OFF by default)

```python
# Tracing enabled by default
store = InMemoryEventStore()
repo = AggregateRepository(event_store=store, aggregate_factory=Order, aggregate_type="Order")

# Disable for high-frequency operations
store_no_trace = InMemoryEventStore(enable_tracing=False)

# Enable projection tracing (off by default)
class OrderProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__(enable_tracing=True)
```

## Standard Attributes

Use attribute constants for consistent span metadata:

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

with tracer.start_as_current_span("process_event") as span:
    span.set_attribute(ATTR_AGGREGATE_ID, str(aggregate_id))
    span.set_attribute(ATTR_EVENT_TYPE, event.__class__.__name__)
```

## Custom Component Tracing

Add tracing to custom components using `TracingMixin`:

```python
from eventsource.observability import TracingMixin, ATTR_AGGREGATE_ID, ATTR_EVENT_COUNT

class OrderService(TracingMixin):
    def __init__(self, repository, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)
        self._repository = repository

    async def place_order(self, customer_id: str, items: list) -> Order:
        with self._create_span_context(
            "order_service.place_order",
            {"customer.id": customer_id, "items.count": len(items)},
        ) as span:
            order_id = uuid4()
            if span:
                span.set_attribute(ATTR_AGGREGATE_ID, str(order_id))

            order = self._repository.create_new(order_id)
            order.place(customer_id, items)
            await self._repository.save(order)

            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(order.pending_events))
            return order
```

Use `@traced` decorator for static attributes:

```python
from eventsource.observability import traced, TracingMixin

class NotificationService(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    @traced("notification_service.send_email", attributes={"channel": "email"})
    async def send_email(self, recipient: str, message: str) -> None:
        await self._email_client.send(recipient, message)

    @traced("notification_service.send_sms", attributes={"channel": "sms"})
    async def send_sms(self, phone: str, message: str) -> None:
        await self._sms_client.send(phone, message)
```

## Distributed Tracing

Distributed event buses (RabbitMQ, Kafka) automatically propagate trace context using W3C Trace Context format:

```python
# Publisher
config = RabbitMQEventBusConfig(rabbitmq_url="amqp://localhost:5672/", enable_tracing=True)
bus = RabbitMQEventBus(config=config)
async with bus:
    await bus.publish([order_created])  # Context injected into headers

# Consumer (different service)
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://localhost:5672/",
    consumer_group="projections",
    enable_tracing=True,
)
bus = RabbitMQEventBus(config=config)
async with bus:
    bus.subscribe(OrderCreated, handler)
    await bus.start_consuming()  # Consumer spans are children of publisher spans
```

## Metrics

Kafka event bus emits metrics (enabled by default):

```python
config = KafkaEventBusConfig(enable_metrics=True, enable_tracing=True)
# Metrics: messages.published, messages.consumed, handler.duration, consumer.lag
```

Configure metrics export:

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://otel-collector:4317"),
    export_interval_millis=60000,
)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
```

See [Kafka Metrics Guide](../guides/kafka-metrics.md) for complete reference.

## Logging Integration

Configure logging for eventsource components:

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("eventsource").setLevel(logging.DEBUG)
logging.getLogger("eventsource.stores.sqlite").setLevel(logging.WARNING)
```

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
    "%(asctime)s [%(levelname)s] trace_id=%(trace_id)s - %(message)s"
))
logging.getLogger("eventsource").addHandler(handler)
```

## Jaeger Setup with Docker

```yaml
# docker-compose.jaeger.yml
version: '3.8'
services:
  jaeger:
    image: jaegertracing/all-in-one:1.51
    ports:
      - "6831:6831/udp"   # Thrift compact
      - "16686:16686"     # UI
      - "4317:4317"       # OTLP gRPC
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

```bash
docker compose -f docker-compose.jaeger.yml up -d
# Access UI at http://localhost:16686
```

## Complete Example

```python
"""Tutorial 20: Observability with OpenTelemetry"""
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
    ATTR_EVENT_TYPE,
    ATTR_EVENT_COUNT,
)

if not OTEL_AVAILABLE:
    print("Install: pip install eventsource-py[telemetry]")


def setup_tracing_console():
    if not OTEL_AVAILABLE:
        return
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)


def setup_tracing_jaeger():
    if not OTEL_AVAILABLE:
        return
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME

    try:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    except ImportError:
        setup_tracing_console()
        return

    resource = Resource(attributes={SERVICE_NAME: "tutorial-20-app"})
    provider = TracerProvider(resource=resource)
    jaeger_exporter = JaegerExporter(agent_host_name="localhost", agent_port=6831)
    provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
    trace.set_tracer_provider(provider)

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


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
                task_id=str(self.aggregate_id), title=event.title, completed=False
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


class TaskService(TracingMixin):
    def __init__(self, repository: AggregateRepository, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)
        self._repository = repository

    async def create_task(self, title: str) -> TaskAggregate:
        with self._create_span_context("task_service.create_task", {"task.title": title}) as span:
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
        with self._create_span_context("task_service.complete_task", {ATTR_AGGREGATE_ID: str(task_id)}):
            task = await self._repository.load(task_id)
            task.complete()
            await self._repository.save(task)
            return task

    async def get_task(self, task_id) -> TaskAggregate:
        with self._create_span_context("task_service.get_task", {ATTR_AGGREGATE_ID: str(task_id)}):
            return await self._repository.load(task_id)


async def basic_tracing_example():
    print("\n=== Basic Tracing ===\n")
    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Learn OpenTelemetry")
    await repo.save(task)
    loaded = await repo.load(task_id)
    print(f"Task loaded: {loaded.state.title}")


async def custom_tracing_example():
    print("\n=== Custom Component Tracing ===\n")
    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )
    service = TaskService(repo, enable_tracing=True)
    task = await service.create_task("Build observability")
    print(f"Created: {task.state.title}")
    task = await service.complete_task(task.aggregate_id)
    print(f"Completed: {task.state.completed}")


async def standard_attributes_example():
    print("\n=== Standard Attributes ===\n")
    print(f"ATTR_AGGREGATE_ID   = '{ATTR_AGGREGATE_ID}'")
    print(f"ATTR_AGGREGATE_TYPE = '{ATTR_AGGREGATE_TYPE}'")
    print(f"ATTR_EVENT_TYPE     = '{ATTR_EVENT_TYPE}'")
    print(f"ATTR_EVENT_COUNT    = '{ATTR_EVENT_COUNT}'")


async def main():
    print("Tutorial 20: Observability with OpenTelemetry")
    setup_tracing_console()
    # setup_tracing_jaeger()  # Uncomment if Jaeger is running

    await basic_tracing_example()
    await custom_tracing_example()
    await standard_attributes_example()

    if OTEL_AVAILABLE:
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

## Exercises

### Exercise 1: Full Observability Setup

Add complete observability to a shopping cart application (30-35 minutes).

```python
"""Exercise 1: Add complete observability to a shopping cart application."""
import asyncio
from uuid import uuid4
from eventsource.observability import OTEL_AVAILABLE, TracingMixin, ATTR_AGGREGATE_ID

if not OTEL_AVAILABLE:
    print("Install: pip install eventsource-py[telemetry]")
    exit(1)

# TODO: Configure TracerProvider with ConsoleSpanExporter or JaegerExporter
# TODO: Define cart events (ItemAdded, ItemRemoved) with @register_event
# TODO: Create CartAggregate with state and event handlers
# TODO: Create CartService with TracingMixin and custom spans
# TODO: Create traced components and perform cart operations
# TODO: Flush traces before exit

asyncio.run(main())
```

<details>
<summary>Solution</summary>

```python
"""Exercise 1 Solution: Full Observability Setup"""
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
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME

resource = Resource(attributes={SERVICE_NAME: "exercise-20-cart"})
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
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
    store = InMemoryEventStore(enable_tracing=True)
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CartAggregate,
        aggregate_type="Cart",
        enable_tracing=True,
    )
    service = CartService(repo, enable_tracing=True)

    cart_id = uuid4()
    await service.add_to_cart(cart_id, "Laptop", 1)
    await service.add_to_cart(cart_id, "Mouse", 2)
    await service.add_to_cart(cart_id, "Keyboard", 1)
    cart = await service.get_cart(cart_id)

    print(f"Cart: {cart.state.items}")
    print(f"Total items: {sum(cart.state.items.values())}")
    trace.get_tracer_provider().force_flush()


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

Solution: `docs/tutorials/exercises/solutions/20-1.py`

## Next Steps

Continue to [Tutorial 21: Advanced Aggregate Patterns](21-advanced-aggregates.md)
