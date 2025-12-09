# Observability API Reference

This document covers the observability utilities for OpenTelemetry integration across eventsource components.

## Overview

The `eventsource.observability` module provides reusable tracing utilities that reduce boilerplate and ensure consistent observability across all eventsource components.

```python
from eventsource.observability import (
    # Constants
    OTEL_AVAILABLE,

    # Helper functions
    get_tracer,
    should_trace,

    # Decorator
    traced,

    # Mixin class
    TracingMixin,
)
```

---

## Module Constants

### `OTEL_AVAILABLE`

Boolean constant indicating whether OpenTelemetry is installed and available.

```python
from eventsource.observability import OTEL_AVAILABLE

if OTEL_AVAILABLE:
    print("OpenTelemetry tracing is available")
else:
    print("Install opentelemetry-api for tracing support")
```

**Note:** This is the single source of truth for OpenTelemetry availability. All components should use this constant rather than performing their own import checks.

---

## Helper Functions

### `get_tracer()`

Get an OpenTelemetry tracer if available.

```python
def get_tracer(name: str) -> Tracer | None
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | The name for the tracer (typically `__name__` of the module) |

**Returns:** OpenTelemetry `Tracer` if available, `None` otherwise.

**Example:**

```python
from eventsource.observability import get_tracer

tracer = get_tracer(__name__)
if tracer:
    with tracer.start_as_current_span("my_operation"):
        # traced operation
        pass
```

### `should_trace()`

Determine if tracing should be active based on component configuration and global availability.

```python
def should_trace(enable_tracing: bool) -> bool
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `enable_tracing` | `bool` | Component-level tracing configuration |

**Returns:** `True` if both tracing is enabled and OpenTelemetry is available.

**Example:**

```python
from eventsource.observability import should_trace

if should_trace(self._enable_tracing):
    # perform traced operation
    pass
```

---

## `@traced` Decorator

Decorator to add OpenTelemetry tracing to methods with minimal boilerplate.

```python
@traced(
    name: str,
    attributes: dict[str, Any] | None = None,
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | Required | Span name (e.g., "event_store.append_events") |
| `attributes` | `dict[str, Any]` | `None` | Static attributes to include in span |

**Requirements:**

The decorated method's class must have:
- `_tracer: Tracer | None` attribute
- `_enable_tracing: bool` attribute

These are typically provided by `TracingMixin` or manual initialization.

**Behavior:**
- If tracing is disabled or tracer is None, the decorator is a no-op
- Supports both async and sync methods
- Automatically detects coroutine functions

**Example:**

```python
from eventsource.observability import traced, TracingMixin

class MyStore(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    @traced("my_store.save")
    async def save(self, item_id: str) -> None:
        # This method is automatically traced
        await self._do_save(item_id)

    @traced("my_store.query", attributes={"db.system": "sqlite"})
    async def query(self, sql: str) -> list:
        # This method includes static attributes in the span
        return await self._execute_query(sql)
```

**Note:** For dynamic attributes that depend on method arguments, use `TracingMixin._create_span_context()` instead.

---

## `TracingMixin` Class

Mixin class providing standardized OpenTelemetry tracing support with minimal boilerplate.

```python
class TracingMixin:
    _tracer: Tracer | None
    _enable_tracing: bool
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_tracer` | `Tracer \| None` | OpenTelemetry tracer instance (or None if unavailable) |
| `_enable_tracing` | `bool` | Whether tracing is enabled for this instance |

### Methods

#### `_init_tracing()`

Initialize tracing attributes. Call this in your `__init__` to set up tracing support.

```python
def _init_tracing(
    self,
    tracer_name: str,
    enable_tracing: bool = True,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tracer_name` | `str` | Required | Name for the tracer (typically `__name__`) |
| `enable_tracing` | `bool` | `True` | Whether to enable tracing |

**Example:**

```python
class MyComponent(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)
```

#### `_create_span_context()`

Create a span context that handles None tracer gracefully. Use this for operations requiring dynamic attributes computed at runtime.

```python
def _create_span_context(
    self,
    name: str,
    attributes: dict[str, Any] | None = None,
) -> AbstractContextManager[Span | None]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | Required | Span name (e.g., "event_bus.dispatch") |
| `attributes` | `dict[str, Any]` | `None` | Dynamic attributes to include in span |

**Returns:** Context manager that yields `Span` or `None`.

**Behavior:**
- Creates a span if tracing is enabled and tracer is available
- Returns `nullcontext()` if tracing is disabled
- The yielded span can be used to add additional attributes

**Example:**

```python
async def process(self, event: DomainEvent) -> None:
    with self._create_span_context(
        "processor.handle",
        {
            "event.type": type(event).__name__,
            "event.id": str(event.event_id),
        },
    ) as span:
        result = await self._do_process(event)
        if span:
            span.set_attribute("result.success", True)
        return result
```

#### `tracing_enabled` Property

Check if tracing is currently enabled.

```python
@property
def tracing_enabled(self) -> bool
```

**Returns:** `True` if tracing is enabled and tracer is available.

**Example:**

```python
if self.tracing_enabled:
    print("Tracing is active for this component")
```

---

## Usage Patterns

### Pattern 1: Using `@traced` Decorator (Simplest)

Best for methods with static attributes or no attributes:

```python
from eventsource.observability import traced, TracingMixin

class MyEventStore(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    @traced("event_store.get_version")
    async def get_version(self, aggregate_id: UUID) -> int:
        # Implementation
        pass
```

### Pattern 2: Using `_create_span_context()` (Dynamic Attributes)

Best for methods needing runtime attributes:

```python
from eventsource.observability import TracingMixin

class MyEventStore(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    async def append_events(
        self,
        aggregate_id: UUID,
        events: list[DomainEvent],
    ) -> AppendResult:
        with self._create_span_context(
            "event_store.append_events",
            {
                "aggregate.id": str(aggregate_id),
                "event.count": len(events),
            },
        ):
            return await self._do_append(aggregate_id, events)
```

### Pattern 3: Mixed (Decorator + Dynamic Attributes)

For complex scenarios requiring both:

```python
class MyEventBus(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    async def dispatch(self, event: DomainEvent) -> None:
        # Outer span with dynamic event attributes
        with self._create_span_context(
            f"event.dispatch.{type(event).__name__}",
            {"event.id": str(event.event_id)},
        ):
            await self._invoke_handlers(event)

    @traced("handler.invoke")
    async def _invoke_single_handler(self, handler: Any) -> None:
        # Inner span with static name
        await handler.handle(event)
```

---

## Span Naming Conventions

Follow these conventions for consistent span names across the codebase:

| Component | Operation | Span Name |
|-----------|-----------|-----------|
| Event Store | Append events | `event_store.append_events` |
| Event Store | Get events | `event_store.get_events` |
| Event Bus | Dispatch event | `event.dispatch.{EventType}` |
| Event Bus | Handler execution | `event_handler.{HandlerName}` |
| Snapshot Store | Save snapshot | `snapshot_store.save` |
| Snapshot Store | Get snapshot | `snapshot_store.get` |
| Subscription Manager | Subscribe | `eventsource.subscription_manager.subscribe` |
| Subscription Manager | Start subscription | `eventsource.subscription_manager.start_subscription` |
| Subscription Manager | Stop | `eventsource.subscription_manager.stop` |
| Subscription Manager | Pause/Resume | `eventsource.subscription_manager.pause_subscription` |
| Transition Coordinator | Execute transition | `eventsource.transition_coordinator.execute` |
| Catch-up Runner | Run until position | `eventsource.catchup_runner.run_until_position` |
| Catch-up Runner | Deliver event | `eventsource.catchup_runner.deliver_event` |
| Live Runner | Start | `eventsource.live_runner.start` |
| Live Runner | Process event | `eventsource.live_runner.process_event` |
| Live Runner | Process buffer | `eventsource.live_runner.process_buffer` |

**Attribute Guidelines:**

| Attribute | When to Use | Example Value |
|-----------|-------------|---------------|
| `aggregate.id` | Any aggregate operation | `"550e8400-e29b-41d4-a716-446655440000"` |
| `aggregate.type` | Any aggregate operation | `"Order"` |
| `event.type` | Event operations | `"OrderCreated"` |
| `event.id` | Event operations | `"550e8400-e29b-41d4-a716-446655440001"` |
| `event.count` | Batch operations | `5` |
| `handler.name` | Handler execution | `"OrderProjection"` |
| `handler.success` | After handler completes | `True` or `False` |
| `db.system` | Database operations | `"postgresql"` or `"sqlite"` |
| `db.name` | Database operations | `"/path/to/events.db"` |

**Subscription Attributes:**

| Attribute | When to Use | Example Value |
|-----------|-------------|---------------|
| `eventsource.subscription.name` | Subscription operations | `"OrderProjection"` |
| `eventsource.subscription.phase` | Transition phase tracking | `"live"`, `"catching_up"` |
| `eventsource.from_position` | Catch-up start | `0` |
| `eventsource.to_position` | Catch-up target | `10000` |
| `eventsource.batch.size` | Batch processing | `100` |
| `eventsource.buffer.size` | Buffer processing | `50` |
| `eventsource.events.processed` | After processing | `1000` |
| `eventsource.events.skipped` | Duplicate/filtered events | `5` |
| `eventsource.watermark` | Transition watermark | `9500` |

---

## Components Using Observability

The following components use the observability module:

| Component | Tracing Method | Configuration |
|-----------|----------------|---------------|
| `InMemoryEventBus` | `TracingMixin` | `enable_tracing` parameter |
| `SQLiteEventStore` | Direct tracing | `enable_tracing` parameter |
| `PostgreSQLEventStore` | Direct tracing | `enable_tracing` parameter |
| `PostgreSQLSnapshotStore` | Direct tracing | `enable_tracing` parameter |
| `RedisEventBus` | Config-based | `RedisEventBusConfig.enable_tracing` |
| `RabbitMQEventBus` | Config-based | `RabbitMQEventBusConfig.enable_tracing` |
| `SubscriptionManager` | `TracingMixin` | `enable_tracing` parameter |
| `TransitionCoordinator` | `TracingMixin` | `enable_tracing` parameter |
| `CatchUpRunner` | `TracingMixin` | `enable_tracing` parameter |
| `LiveRunner` | `TracingMixin` | `enable_tracing` parameter |
| `PostgreSQLCheckpointRepository` | `TracingMixin` | `enable_tracing` parameter |
| `PostgreSQLDLQRepository` | `TracingMixin` | `enable_tracing` parameter |

---

## Testing with Tracing

### Disabling Tracing in Tests

```python
# Disable tracing to avoid OpenTelemetry overhead in tests
store = SQLiteEventStore(":memory:", enable_tracing=False)
bus = InMemoryEventBus(enable_tracing=False)
```

### Mocking Tracers

```python
from unittest.mock import MagicMock, patch

# Mock the tracer for testing span creation
@patch('eventsource.observability.tracing.trace')
def test_tracing(mock_trace):
    mock_tracer = MagicMock()
    mock_trace.get_tracer.return_value = mock_tracer

    store = SQLiteEventStore(":memory:", enable_tracing=True)
    # ... test operations

    # Verify spans were created
    mock_tracer.start_as_current_span.assert_called()
```

---

## See Also

- [Architecture Overview](../architecture.md#observability) - How tracing fits into the system
- [Installation Guide](../installation.md) - Installing the telemetry extra
- [Production Guide](../guides/production.md) - Production observability setup
