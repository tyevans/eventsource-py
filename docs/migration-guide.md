# Migration Guide

This guide helps Honeybadger users migrate to the eventsource library.

## Overview

The eventsource library was extracted from Honeybadger to provide a standalone, reusable event sourcing implementation. Most concepts and patterns remain the same, but there are some changes to import paths and naming conventions.

## Import Path Changes

### Events

**Before (Honeybadger):**
```python
from backend.events.base import DomainEvent
from backend.events.registry import register_event, get_event_class
```

**After (eventsource):**
```python
from eventsource import DomainEvent, register_event, get_event_class
```

### Event Store

**Before:**
```python
from backend.stores.postgresql import PostgreSQLEventStore
from backend.stores.in_memory import InMemoryEventStore
```

**After:**
```python
from eventsource import PostgreSQLEventStore, InMemoryEventStore
```

### Aggregates

**Before:**
```python
from backend.aggregates.base import AggregateRoot
from backend.aggregates.repository import AggregateRepository
```

**After:**
```python
from eventsource import AggregateRoot, AggregateRepository
```

### Projections

**Before:**
```python
from backend.projections.base import Projection, CheckpointTrackingProjection
from backend.projections.decorators import handles
```

**After:**
```python
from eventsource.projections import (
    Projection,
    CheckpointTrackingProjection,
    DeclarativeProjection,
    handles,
)
```

### Event Bus

**Before:**
```python
from backend.bus.memory import InMemoryEventBus
from backend.bus.redis import RedisEventBus
```

**After:**
```python
from eventsource import InMemoryEventBus, RedisEventBus
```

### Repositories

**Before:**
```python
from backend.repositories.checkpoint import CheckpointRepository
from backend.repositories.dlq import DLQRepository
from backend.repositories.outbox import OutboxRepository
```

**After:**
```python
from eventsource import (
    CheckpointRepository,
    PostgreSQLCheckpointRepository,
    InMemoryCheckpointRepository,
    DLQRepository,
    PostgreSQLDLQRepository,
    InMemoryDLQRepository,
    OutboxRepository,
    PostgreSQLOutboxRepository,
    InMemoryOutboxRepository,
)
```

### Exceptions

**Before:**
```python
from backend.exceptions import (
    EventSourceError,
    OptimisticLockError,
    AggregateNotFoundError,
)
```

**After:**
```python
from eventsource import (
    EventSourceError,
    OptimisticLockError,
    AggregateNotFoundError,
    EventNotFoundError,
    ProjectionError,
)
```

## Configuration Changes

### PostgreSQL Event Store

**Before:**
```python
store = PostgreSQLEventStore(engine)
```

**After:**
```python
from sqlalchemy.ext.asyncio import async_sessionmaker

session_factory = async_sessionmaker(engine, expire_on_commit=False)
store = PostgreSQLEventStore(
    session_factory,
    event_registry=default_registry,  # Optional, uses default
    outbox_enabled=False,             # Optional outbox pattern
    enable_tracing=True,              # Optional OpenTelemetry
)
```

### Redis Event Bus

**Before:**
```python
bus = RedisEventBus(redis_url="redis://localhost:6379")
```

**After:**
```python
from eventsource import RedisEventBusConfig, RedisEventBus

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_name="events",
    consumer_group="my-app",
    consumer_name="worker-1",
)
bus = RedisEventBus(config)
```

## API Changes

### Event Registry

The `@register_event` decorator now supports more options:

```python
# Basic (unchanged)
@register_event
class MyEvent(DomainEvent):
    event_type: str = "MyEvent"

# With explicit type name
@register_event(event_type="my.custom.event")
class MyEvent(DomainEvent):
    event_type: str = "my.custom.event"

# With custom registry (new)
@register_event(registry=custom_registry)
class MyEvent(DomainEvent):
    event_type: str = "MyEvent"
```

### AggregateRepository

The repository now accepts `aggregate_factory` instead of `aggregate_class`:

**Before:**
```python
repo = AggregateRepository(
    event_store=store,
    aggregate_class=OrderAggregate,
    aggregate_type="Order",
)
```

**After:**
```python
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,  # Renamed parameter
    aggregate_type="Order",
    event_publisher=event_bus,  # Optional, new parameter
)
```

### CheckpointTrackingProjection

The projection base class now requires explicit checkpoint and DLQ repositories:

**Before:**
```python
class MyProjection(CheckpointTrackingProjection):
    pass
```

**After:**
```python
class MyProjection(CheckpointTrackingProjection):
    def __init__(self, checkpoint_repo=None, dlq_repo=None):
        super().__init__(
            checkpoint_repo=checkpoint_repo,  # Or InMemoryCheckpointRepository()
            dlq_repo=dlq_repo,                # Or InMemoryDLQRepository()
        )
```

### DeclarativeProjection

New base class for projections using `@handles` decorator:

**Before:**
```python
class MyProjection(CheckpointTrackingProjection):
    def subscribed_to(self):
        return [EventA, EventB]

    async def _process_event(self, event):
        if isinstance(event, EventA):
            await self._handle_a(event)
        elif isinstance(event, EventB):
            await self._handle_b(event)
```

**After:**
```python
class MyProjection(DeclarativeProjection):
    @handles(EventA)
    async def _handle_a(self, event: EventA) -> None:
        pass

    @handles(EventB)
    async def _handle_b(self, event: EventB) -> None:
        pass

    # subscribed_to() is auto-generated from @handles decorators
```

## Database Schema Changes

### Events Table

No schema changes required. The library works with existing `events` tables.

### Checkpoint Table

Add `event_type` column if not present:

```sql
ALTER TABLE projection_checkpoints
ADD COLUMN IF NOT EXISTS event_type VARCHAR(255);
```

### DLQ Table

Schema compatible. No changes required.

### Outbox Table

Add `status` column if not present:

```sql
ALTER TABLE event_outbox
ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'pending';
```

## Step-by-Step Migration

### 1. Install eventsource

```bash
pip install eventsource[postgresql,redis]
```

### 2. Update Imports

Use find-and-replace to update import statements:

```bash
# Example using sed
sed -i 's/from backend\.events\.base/from eventsource/g' **/*.py
sed -i 's/from backend\.events\.registry/from eventsource/g' **/*.py
# ... continue for other imports
```

### 3. Update Event Definitions

Ensure all events have proper `event_type` and `aggregate_type`:

```python
@register_event
class MyEvent(DomainEvent):
    event_type: str = "MyEvent"        # Required
    aggregate_type: str = "MyAggregate"  # Required
    # ... other fields
```

### 4. Update Aggregate Repository Usage

```python
# Old
repo = AggregateRepository(store, OrderAggregate, "Order")

# New
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
)
```

### 5. Update Projections

If using `CheckpointTrackingProjection`, update to pass repositories:

```python
projection = MyProjection(
    checkpoint_repo=PostgreSQLCheckpointRepository(session_factory),
    dlq_repo=PostgreSQLDLQRepository(session_factory),
)
```

Or migrate to `DeclarativeProjection` for cleaner code.

### 6. Run Tests

```bash
pytest tests/
```

### 7. Deploy

No database migrations required for basic usage.

## Breaking Changes Summary

| Component | Change | Migration |
|-----------|--------|-----------|
| Import paths | New package | Update imports |
| PostgreSQLEventStore | Session factory required | Use async_sessionmaker |
| AggregateRepository | Parameter renamed | `aggregate_class` -> `aggregate_factory` |
| CheckpointTrackingProjection | Repos required | Pass checkpoint_repo, dlq_repo |
| RedisEventBus | Config object | Use RedisEventBusConfig |

## New Features

### DeclarativeAggregate

Alternative to AggregateRoot using decorators:

```python
class OrderAggregate(DeclarativeAggregate[OrderState]):
    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = OrderState(...)
```

### Transactional Outbox

Reliable event publishing pattern:

```python
store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=True,
)
```

### OpenTelemetry Support

Optional distributed tracing:

```bash
pip install eventsource[telemetry]
```

```python
store = PostgreSQLEventStore(
    session_factory,
    enable_tracing=True,
)
```

### Lag Monitoring

Track projection progress:

```python
metrics = await projection.get_lag_metrics()
print(f"Lag: {metrics['lag_seconds']} seconds")
```

## Getting Help

- Check the [API Reference](api/) for detailed documentation
- Review [Examples](examples/) for common patterns
- Open an issue on GitHub for bugs or questions
