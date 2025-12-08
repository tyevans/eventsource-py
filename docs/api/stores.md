# Event Stores API Reference

This document covers the event store interface and implementations for persisting and retrieving domain events.

## Overview

Event stores are the source of truth in event sourcing architecture. They persist and retrieve domain events for aggregate reconstruction.

```python
from eventsource import (
    # Interface
    EventStore,

    # Implementations
    InMemoryEventStore,
    PostgreSQLEventStore,
    SQLiteEventStore,

    # Data structures
    EventStream,
    AppendResult,
    StoredEvent,
    ReadOptions,
    ReadDirection,
    ExpectedVersion,
)
```

---

## EventStore Interface

The abstract `EventStore` class defines the contract for all event store implementations.

### Core Methods

#### `append_events()`

Append events to an aggregate's event stream with optimistic locking.

```python
async def append_events(
    self,
    aggregate_id: UUID,
    aggregate_type: str,
    events: list[DomainEvent],
    expected_version: int,
) -> AppendResult
```

**Parameters:**
- `aggregate_id`: ID of the aggregate
- `aggregate_type`: Type name (e.g., "Order")
- `events`: Events to append
- `expected_version`: Expected current version for optimistic locking

**Example:**
```python
result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[order_created_event],
    expected_version=0,  # New aggregate
)

if result.success:
    print(f"New version: {result.new_version}")
elif result.conflict:
    print("Concurrent modification detected")
```

#### `get_events()`

Get all events for an aggregate.

```python
async def get_events(
    self,
    aggregate_id: UUID,
    aggregate_type: str | None = None,
    from_version: int = 0,
    from_timestamp: datetime | None = None,
    to_timestamp: datetime | None = None,
) -> EventStream
```

**Example:**
```python
stream = await store.get_events(order_id, "Order")
for event in stream.events:
    aggregate.apply_event(event, is_new=False)
```

#### `get_events_by_type()`

Get all events for a specific aggregate type.

```python
async def get_events_by_type(
    self,
    aggregate_type: str,
    tenant_id: UUID | None = None,
    from_timestamp: float | None = None,
) -> list[DomainEvent]
```

**Example:**
```python
# Get all Order events for a tenant
order_events = await store.get_events_by_type(
    "Order",
    tenant_id=tenant_uuid,
    from_timestamp=last_processed_time,
)
```

#### `event_exists()`

Check if an event exists (for idempotency).

```python
async def event_exists(self, event_id: UUID) -> bool
```

#### `get_stream_version()`

Get the current version of an aggregate.

```python
async def get_stream_version(
    self,
    aggregate_id: UUID,
    aggregate_type: str,
) -> int
```

#### `read_stream()` / `read_all()`

Streaming methods for efficient event processing:

```python
async for stored_event in store.read_stream("order-123:Order"):
    print(f"Event at position {stored_event.stream_position}")

async for stored_event in store.read_all():
    projection.handle(stored_event.event)
```

---

## InMemoryEventStore

In-memory implementation for testing and development.

### Usage

```python
from eventsource import InMemoryEventStore

store = InMemoryEventStore()

# Basic operations
result = await store.append_events(...)
stream = await store.get_events(aggregate_id, "Order")

# Testing utilities
store.clear()  # Reset state between tests
all_events = store.get_all_events()
count = store.get_event_count()
```

### Characteristics

- **Thread-safe**: Uses internal locking
- **Non-persistent**: Events lost when process terminates
- **Fast**: All operations in memory

### When to Use

- Unit testing
- Development environments
- Prototyping
- Single-process applications with ephemeral state

### When NOT to Use

- Production deployments requiring persistence
- Distributed systems
- High-volume event storage

---

## PostgreSQLEventStore

Production-ready PostgreSQL implementation.

### Setup

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource import PostgreSQLEventStore

engine = create_async_engine(
    "postgresql+asyncpg://user:pass@localhost/mydb",
    pool_size=10,
    max_overflow=20,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=True,      # Enable transactional outbox
    enable_tracing=True,      # Enable OpenTelemetry tracing
)
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_factory` | `async_sessionmaker` | Required | SQLAlchemy session factory |
| `event_registry` | `EventRegistry` | Default registry | Event type lookup |
| `outbox_enabled` | `bool` | `False` | Write events to outbox table |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing |

### Features

- **Optimistic locking**: Concurrent modification detection
- **Idempotent writes**: Duplicate events are skipped
- **Transactional outbox**: Atomic event + outbox writes
- **OpenTelemetry tracing**: Optional performance monitoring
- **Partition-aware**: Timestamp filters enable partition pruning
- **Multi-tenancy**: Built-in tenant isolation

### Database Schema

The store expects an `events` table:

```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    tenant_id VARCHAR(36),
    actor_id VARCHAR(255),
    version INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_events_aggregate_version
        UNIQUE (aggregate_id, aggregate_type, version)
);

CREATE INDEX idx_events_aggregate ON events(aggregate_id, aggregate_type);
CREATE INDEX idx_events_type ON events(aggregate_type);
CREATE INDEX idx_events_tenant ON events(tenant_id);
CREATE INDEX idx_events_timestamp ON events(timestamp);
```

For outbox pattern:

```sql
CREATE TABLE event_outbox (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(36),
    event_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    processed_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_status ON event_outbox(status, created_at);
```

---

## SQLiteEventStore

Lightweight SQLite implementation for development, testing, and embedded applications.

### Installation

```bash
pip install eventsource[sqlite]
```

### Setup

```python
from eventsource import SQLiteEventStore

# File-based database
async with SQLiteEventStore("./events.db") as store:
    await store.initialize()
    # ... use store

# In-memory database for testing
async with SQLiteEventStore(":memory:") as store:
    await store.initialize()
    # ... use store

# With tracing disabled (useful for testing without OpenTelemetry overhead)
async with SQLiteEventStore("./events.db", enable_tracing=False) as store:
    await store.initialize()
    # ... use store
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | Required | Path to SQLite file or `:memory:` |
| `event_registry` | `EventRegistry` | Default registry | Event type lookup |
| `wal_mode` | `bool` | `True` | Enable WAL journal mode |
| `busy_timeout` | `int` | `5000` | Timeout in ms for locked database |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing |

### Features

- **Optimistic locking**: Version-based conflict detection
- **Idempotent writes**: Duplicate events are skipped
- **WAL mode**: Better concurrent read performance (optional)
- **Multi-tenancy**: Built-in tenant isolation
- **OpenTelemetry tracing**: Optional performance monitoring (consistent with PostgreSQLEventStore)
- **Full EventStore interface**: Drop-in replacement for other stores

### SQLite-Specific Adaptations

The SQLite implementation handles type differences transparently:

- **UUIDs**: Stored as TEXT (36-character hyphenated format)
- **Timestamps**: Stored as TEXT (ISO 8601 format)
- **JSON**: Stored as TEXT (SQLite lacks native JSONB)
- **Auto-increment**: Uses INTEGER PRIMARY KEY AUTOINCREMENT

### Context Manager

Always use the async context manager for proper resource cleanup:

```python
# Recommended approach
async with SQLiteEventStore("./events.db") as store:
    await store.initialize()
    result = await store.append_events(...)

# Manual approach (requires explicit cleanup)
store = SQLiteEventStore("./events.db")
await store._connect()
await store.initialize()
try:
    result = await store.append_events(...)
finally:
    await store.close()
```

### Properties

```python
store = SQLiteEventStore("./events.db", wal_mode=True, busy_timeout=10000)

# Read-only properties
store.database        # "./events.db"
store.event_registry  # EventRegistry instance
store.is_connected    # True if connection is open
store.wal_mode        # True
store.busy_timeout    # 10000
```

### Database Schema

The store uses the following schema (created by `initialize()`):

```sql
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    tenant_id TEXT,
    actor_id TEXT,
    version INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(aggregate_id, aggregate_type, version)
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate
    ON events(aggregate_id, aggregate_type);
CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(aggregate_type);
CREATE INDEX IF NOT EXISTS idx_events_tenant
    ON events(tenant_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events(timestamp);
```

### Limitations

- **Single writer**: Only one write operation at a time
- **No network access**: Cannot share database across machines
- **Busy timeout**: Concurrent writers may fail after timeout

### When to Use

- Local development without database setup
- Unit and integration testing
- CI/CD pipelines
- Single-instance deployments
- Embedded applications
- Edge computing scenarios

### When NOT to Use

- High-throughput production workloads
- Multi-instance deployments
- Heavy concurrent write loads

### Example: Complete Usage

```python
import asyncio
from uuid import uuid4
from eventsource import SQLiteEventStore, AggregateRepository

async def main():
    async with SQLiteEventStore("./app.db", wal_mode=True) as store:
        await store.initialize()

        # Direct event operations
        order_id = uuid4()
        result = await store.append_events(
            aggregate_id=order_id,
            aggregate_type="Order",
            events=[order_created_event],
            expected_version=0,
        )
        print(f"Appended: version={result.new_version}")

        # Get events
        stream = await store.get_events(order_id, "Order")
        print(f"Stream has {len(stream.events)} events")

        # Check existence
        exists = await store.event_exists(order_created_event.event_id)
        print(f"Event exists: {exists}")

        # Stream all events
        async for stored_event in store.read_all():
            print(f"Position {stored_event.global_position}: {stored_event.event_type}")

asyncio.run(main())
```

See [SQLite Backend Guide](../guides/sqlite-backend.md) for detailed usage patterns and best practices.

---

## Data Structures

### EventStream

Container for events belonging to an aggregate.

```python
@dataclass(frozen=True)
class EventStream:
    aggregate_id: UUID
    aggregate_type: str
    events: list[DomainEvent]
    version: int

    @property
    def is_empty(self) -> bool: ...

    @property
    def latest_event(self) -> DomainEvent | None: ...

    @classmethod
    def empty(cls, aggregate_id: UUID, aggregate_type: str) -> EventStream: ...
```

### AppendResult

Result of appending events.

```python
@dataclass(frozen=True)
class AppendResult:
    success: bool
    new_version: int
    global_position: int = 0
    conflict: bool = False

    @classmethod
    def successful(cls, new_version: int, global_position: int = 0) -> AppendResult: ...

    @classmethod
    def conflicted(cls, current_version: int) -> AppendResult: ...
```

### StoredEvent

Wrapper for persisted events with position metadata.

```python
@dataclass(frozen=True)
class StoredEvent:
    event: DomainEvent
    stream_id: str
    stream_position: int
    global_position: int
    stored_at: datetime

    @property
    def event_id(self) -> UUID: ...

    @property
    def event_type(self) -> str: ...

    @property
    def aggregate_id(self) -> UUID: ...
```

### ReadOptions

Configuration for reading events.

```python
@dataclass(frozen=True)
class ReadOptions:
    direction: ReadDirection = ReadDirection.FORWARD
    from_position: int = 0
    limit: int | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None
```

### ExpectedVersion

Constants for version expectations.

```python
class ExpectedVersion:
    ANY: int = -1          # Skip version check
    NO_STREAM: int = 0     # Stream must not exist
    STREAM_EXISTS: int = -2  # Stream must exist
```

**Usage:**
```python
# New aggregate (must not exist)
await store.append_events(
    aggregate_id=uuid4(),
    aggregate_type="Order",
    events=[order_created],
    expected_version=ExpectedVersion.NO_STREAM,
)

# Update existing (must exist)
await store.append_events(
    aggregate_id=existing_id,
    aggregate_type="Order",
    events=[order_shipped],
    expected_version=ExpectedVersion.STREAM_EXISTS,
)

# Don't check version
await store.append_events(
    aggregate_id=any_id,
    aggregate_type="Order",
    events=[event],
    expected_version=ExpectedVersion.ANY,
)
```

---

## Error Handling

### OptimisticLockError

Raised when version check fails.

```python
from eventsource import OptimisticLockError

try:
    await store.append_events(
        aggregate_id=order_id,
        aggregate_type="Order",
        events=[event],
        expected_version=5,
    )
except OptimisticLockError as e:
    print(f"Aggregate: {e.aggregate_id}")
    print(f"Expected version: {e.expected_version}")
    print(f"Actual version: {e.actual_version}")
    # Retry with fresh state
```

---

## Best Practices

### Optimistic Locking

Always use proper expected versions:

```python
# Load aggregate to get current version
stream = await store.get_events(order_id, "Order")
current_version = stream.version

# Modify and save
result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=new_events,
    expected_version=current_version,
)
```

### Idempotency

Use `event_exists()` for deduplication:

```python
if await store.event_exists(event.event_id):
    print("Event already processed")
    return
```

### Batch Processing

Use streaming methods for large datasets:

```python
options = ReadOptions(limit=100)
async for stored_event in store.read_all(options):
    await process_event(stored_event.event)
```

### Timestamp Filtering

Enable partition pruning in PostgreSQL:

```python
# Only query recent events
from datetime import datetime, timedelta, UTC

recent = datetime.now(UTC) - timedelta(days=7)
stream = await store.get_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    from_timestamp=recent,
)
```
