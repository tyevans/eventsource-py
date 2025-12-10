# Event Stores API Reference

The event store is the heart of event sourcing. It is an append-only database that stores all domain events as the single source of truth.

## What is an Event Store?

In traditional systems, you store the current state and lose the history. Event sourcing flips this: you store the sequence of events that led to the current state, then derive the state by replaying those events.

```
Traditional Database:           Event Store:
+------------------+            +----------------------------------+
| orders           |            | events                           |
|------------------|            |----------------------------------|
| id: 123          |            | OrderCreated { id: 123 }         |
| status: shipped  |            | ItemAdded { sku: "ABC" }         |
| items: [...]     |            | ItemAdded { sku: "XYZ" }         |
+------------------+            | OrderShipped { tracking: "..." } |
                                +----------------------------------+
     (State only)                   (Complete history)
```

The event store provides:

- **Durability**: Events are persisted and never lost
- **Ordering**: Events are stored in strict order (both per-stream and global)
- **Optimistic concurrency**: Prevents concurrent modifications to the same aggregate
- **Replay capability**: Rebuild any aggregate or projection from events

## Quick Start

```python
from eventsource import (
    # Interface
    EventStore,

    # Implementations
    InMemoryEventStore,      # Testing and development
    PostgreSQLEventStore,    # Production
    SQLiteEventStore,        # Lightweight/embedded

    # Data structures
    EventStream,
    AppendResult,
    StoredEvent,
    ReadOptions,
    ReadDirection,
    ExpectedVersion,
)
```

**Choose your store:**

```python
# Development/Testing: In-memory (no persistence)
store = InMemoryEventStore()

# Development/Testing: SQLite (file or memory)
async with SQLiteEventStore("./events.db") as store:
    await store.initialize()

# Production: PostgreSQL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
store = PostgreSQLEventStore(session_factory)
```

---

## Key Concepts

### Stream Position vs Global Position

Every event has two positions:

```
Stream "Order-123":        Global Event Log:
+-------------------+      +--------------------------------+
| Position 1: Created |      | Position 1: User-456 Created   |
| Position 2: ItemAdd |  --> | Position 2: Order-123 Created  |  <-- stream_position=1
| Position 3: Shipped |      | Position 3: Order-123 ItemAdd  |  <-- stream_position=2
+-------------------+      | Position 4: User-789 Updated   |
                           | Position 5: Order-123 Shipped  |  <-- stream_position=3
                           +--------------------------------+
```

- **Stream Position**: Order within a single aggregate's events (used for aggregate reconstruction)
- **Global Position**: Order across ALL events in the store (used for projections and subscriptions)

### Why Integer Positions Instead of UUIDs?

Events have both an `event_id` (UUID) and a `global_position` (integer) because they serve different purposes:

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | UUID | **Identity** — Unique identifier for deduplication and idempotency checks |
| `global_position` | Integer | **Ordering** — Strict sequential ordering for subscriptions and catch-up |

**Why not use UUIDs for ordering?** Standard UUIDs (v1-v4) have no natural sort order. Even UUID v7 (timestamp-based) can have ordering issues with clock skew across distributed writers. Database-assigned auto-increment integers provide:

1. **Guaranteed sequential ordering** — No gaps, no out-of-order events
2. **Atomic assignment** — The database ensures each position is unique
3. **Clock-independent** — Works correctly with multiple writers across machines
4. **Watermark compatibility** — Subscriptions can reliably track "catch up to position X"

This is the standard pattern used by EventStoreDB, Marten, Axon, and other production event stores.

See the [FAQ: Why integers instead of UUIDs?](../faq.md#why-does-eventsource-use-integers-for-global-ordering-instead-of-uuids) for more details.

### Optimistic Concurrency Control

When saving events, you specify the expected version. If another process saved events first, you get a conflict:

```python
# Load aggregate at version 3
order = await repo.load(order_id)  # version = 3

# Meanwhile, another process saves to version 4...

# Your save fails with OptimisticLockError
await repo.save(order)  # expected_version=3, but current is 4!
```

This prevents lost updates in concurrent systems. See [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) for details.

---

## EventStore Interface

The abstract `EventStore` class defines the contract for all implementations.

### `append_events()`

Append events to an aggregate's stream with optimistic locking.

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

| Parameter | Description |
|-----------|-------------|
| `aggregate_id` | UUID of the aggregate |
| `aggregate_type` | Type name (e.g., "Order", "User") |
| `events` | List of events to append (in order) |
| `expected_version` | Current version for optimistic locking (0 for new aggregates) |

**Example:**

```python
from uuid import uuid4
from eventsource import ExpectedVersion, OptimisticLockError

# Create a new aggregate (version 0 -> 1)
result = await store.append_events(
    aggregate_id=uuid4(),
    aggregate_type="Order",
    events=[OrderCreated(...)],
    expected_version=0,
)
assert result.success
assert result.new_version == 1

# Update existing aggregate (version 1 -> 2)
result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[OrderShipped(...)],
    expected_version=1,
)

# Handle concurrent modification
try:
    await store.append_events(...)
except OptimisticLockError as e:
    print(f"Expected version {e.expected_version}, but was {e.actual_version}")
    # Reload and retry
```

### `get_events()`

Retrieve events for an aggregate to reconstruct its state.

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
# Get all events for an aggregate
stream = await store.get_events(order_id, "Order")
print(f"Order has {stream.version} events")

for event in stream.events:
    aggregate._apply(event)

# Get events after version 5 (for partial replay)
stream = await store.get_events(order_id, "Order", from_version=5)

# Get events within a time range (useful for partitioned tables)
from datetime import datetime, timedelta, UTC
one_week_ago = datetime.now(UTC) - timedelta(days=7)
stream = await store.get_events(
    order_id, "Order",
    from_timestamp=one_week_ago,
)
```

### `get_events_by_type()`

Get all events for a specific aggregate type (useful for building projections).

```python
async def get_events_by_type(
    self,
    aggregate_type: str,
    tenant_id: UUID | None = None,
    from_timestamp: datetime | None = None,
) -> list[DomainEvent]
```

**Example:**

```python
from datetime import datetime, UTC, timedelta

# Get all Order events
order_events = await store.get_events_by_type("Order")

# Get Order events for a specific tenant
order_events = await store.get_events_by_type(
    "Order",
    tenant_id=tenant_uuid,
)

# Get recent events only
one_hour_ago = datetime.now(UTC) - timedelta(hours=1)
recent_events = await store.get_events_by_type(
    "Order",
    from_timestamp=one_hour_ago,
)
```

### `event_exists()`

Check if an event exists (for idempotency checks).

```python
async def event_exists(self, event_id: UUID) -> bool
```

**Example:**

```python
# Prevent duplicate processing
if await store.event_exists(event.event_id):
    print("Already processed, skipping")
    return
```

### `get_stream_version()`

Get the current version of an aggregate without fetching all events.

```python
async def get_stream_version(
    self,
    aggregate_id: UUID,
    aggregate_type: str,
) -> int
```

### `get_global_position()`

Get the maximum global position in the event store. Used by subscriptions to determine catch-up completion.

```python
async def get_global_position(self) -> int
```

**Example:**

```python
# Check how far behind a projection is
current_position = await store.get_global_position()
projection_position = await checkpoint_repo.get_position("MyProjection")
lag = current_position - projection_position
print(f"Projection is {lag} events behind")
```

### `read_stream()` / `read_all()`

Streaming methods for efficient event processing (memory-efficient for large datasets):

```python
# Read a specific aggregate's stream
async for stored_event in store.read_stream("order-123:Order"):
    print(f"Event at position {stored_event.stream_position}")

# Read ALL events (for projections)
async for stored_event in store.read_all():
    await projection.handle(stored_event.event)

# Read with options
from eventsource.stores import ReadOptions, ReadDirection

options = ReadOptions(
    from_position=1000,        # Start after position 1000
    limit=100,                 # Process in batches
    direction=ReadDirection.FORWARD,
)
async for stored_event in store.read_all(options):
    print(f"Global position: {stored_event.global_position}")
```

---

## InMemoryEventStore

Fast, thread-safe in-memory implementation for testing and development.

```python
from eventsource import InMemoryEventStore

store = InMemoryEventStore()

# All EventStore operations work
result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[order_created],
    expected_version=0,
)
stream = await store.get_events(order_id, "Order")
```

### Constructor

```python
InMemoryEventStore(enable_tracing: bool = True)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing |

### Testing Utilities

The in-memory store provides extra methods useful for testing:

```python
# Reset state between tests
await store.clear()

# Get all events (useful for assertions)
all_events = await store.get_all_events()

# Count total events
count = await store.get_event_count()

# List all aggregate IDs
aggregate_ids = await store.get_aggregate_ids()

# Get current global position
position = await store.get_global_position()
```

### Characteristics

| Feature | Value |
|---------|-------|
| Persistence | None (data lost on process exit) |
| Concurrency | Thread-safe via asyncio.Lock |
| Performance | Very fast (all in-memory) |
| Global ordering | Fully supported |
| Tracing | OpenTelemetry (optional) |

### When to Use

- Unit tests (fast, isolated, deterministic)
- Integration tests (full event store API)
- Development/prototyping (quick iteration)
- Single-process apps with ephemeral state

### When NOT to Use

- Production (no persistence)
- Distributed systems (no shared state)
- Large event volumes (memory-bound)

---

## PostgreSQLEventStore

Production-ready PostgreSQL implementation with ACID guarantees, optimistic locking, and optional transactional outbox.

### Setup

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource import PostgreSQLEventStore

# Create SQLAlchemy async engine
engine = create_async_engine(
    "postgresql+asyncpg://user:pass@localhost/mydb",
    pool_size=10,
    max_overflow=20,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# Basic setup
store = PostgreSQLEventStore(session_factory)

# Production setup with all features
store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=True,      # Transactional outbox pattern
    enable_tracing=True,      # OpenTelemetry tracing
)
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_factory` | `async_sessionmaker` | Required | SQLAlchemy async session factory |
| `event_registry` | `EventRegistry` | Default | Registry for event deserialization |
| `outbox_enabled` | `bool` | `False` | Write to outbox table atomically |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing |
| `type_converter` | `TypeConverter` | `None` | Custom type converter |
| `uuid_fields` | `set[str]` | `None` | Additional UUID field names |
| `string_id_fields` | `set[str]` | `None` | Fields to exclude from UUID detection |
| `auto_detect_uuid` | `bool` | `True` | Auto-detect UUIDs by `_id` suffix |

### Factory Methods

```python
# Strict UUID detection (no auto-detection, explicit fields only)
store = PostgreSQLEventStore.with_strict_uuid_detection(
    session_factory,
    uuid_fields={"event_id", "aggregate_id", "tenant_id"},
)
```

### Features

| Feature | Description |
|---------|-------------|
| **Optimistic locking** | Version-based conflict detection with unique constraint backup |
| **Idempotent writes** | Duplicate events (same event_id) are silently skipped |
| **Transactional outbox** | Events + outbox written atomically (reliable publishing) |
| **OpenTelemetry tracing** | Spans for all operations with semantic attributes |
| **Partition-aware** | Timestamp filters enable partition pruning |
| **Multi-tenancy** | `tenant_id` field for data isolation |
| **Global ordering** | Auto-increment `id` provides total ordering |

### Properties

```python
store.session_factory    # Get the SQLAlchemy session factory
store.event_registry     # Get the event registry
store.outbox_enabled     # Check if outbox is enabled
```

### Database Schema

The store requires an `events` table. Create it with this schema:

```sql
-- Main events table
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,           -- Global position (auto-increment)
    event_id UUID NOT NULL UNIQUE,      -- Unique event identifier
    event_type VARCHAR(255) NOT NULL,   -- e.g., "OrderCreated"
    aggregate_type VARCHAR(255) NOT NULL, -- e.g., "Order"
    aggregate_id UUID NOT NULL,         -- Aggregate identifier
    tenant_id VARCHAR(36),              -- Multi-tenancy support
    actor_id VARCHAR(255),              -- Who triggered the event
    version INTEGER NOT NULL,           -- Stream position (1-based)
    timestamp TIMESTAMPTZ NOT NULL,     -- When event occurred
    payload JSONB NOT NULL,             -- Event data
    created_at TIMESTAMPTZ DEFAULT NOW(), -- When stored

    -- Optimistic locking constraint
    CONSTRAINT uq_events_aggregate_version
        UNIQUE (aggregate_id, aggregate_type, version)
);

-- Performance indexes
CREATE INDEX idx_events_aggregate ON events(aggregate_id, aggregate_type);
CREATE INDEX idx_events_type ON events(aggregate_type);
CREATE INDEX idx_events_tenant ON events(tenant_id);
CREATE INDEX idx_events_timestamp ON events(timestamp);
```

**For the transactional outbox pattern** (when `outbox_enabled=True`):

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

### Performance Considerations

**Connection Pooling:**
```python
engine = create_async_engine(
    "postgresql+asyncpg://...",
    pool_size=10,          # Base pool size
    max_overflow=20,       # Additional connections under load
    pool_recycle=3600,     # Recycle connections hourly
)
```

**Partition Pruning:**
```python
# For time-partitioned tables, always include timestamp filters
from datetime import datetime, timedelta, UTC

# This enables PostgreSQL to skip irrelevant partitions
stream = await store.get_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    from_timestamp=datetime.now(UTC) - timedelta(days=30),
)
```

**Batch Processing:**
```python
# Use read_all with limits for large datasets
options = ReadOptions(from_position=last_position, limit=1000)
async for event in store.read_all(options):
    await process(event)
    last_position = event.global_position
```

---

## SQLiteEventStore

Lightweight SQLite implementation for development, testing, and embedded applications. Zero configuration required.

### Installation

```bash
pip install eventsource[sqlite]
```

### Setup

```python
from eventsource import SQLiteEventStore

# File-based database (persistent)
async with SQLiteEventStore("./events.db") as store:
    await store.initialize()
    # ... use store

# In-memory database (perfect for tests)
async with SQLiteEventStore(":memory:") as store:
    await store.initialize()
    # ... use store
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | Required | Path to SQLite file or `:memory:` |
| `event_registry` | `EventRegistry` | Default | Registry for event deserialization |
| `wal_mode` | `bool` | `True` | WAL mode for concurrent read performance |
| `busy_timeout` | `int` | `5000` | Milliseconds to wait when database is locked |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing |
| `type_converter` | `TypeConverter` | `None` | Custom type converter |
| `uuid_fields` | `set[str]` | `None` | Additional UUID field names |
| `string_id_fields` | `set[str]` | `None` | Fields to exclude from UUID detection |
| `auto_detect_uuid` | `bool` | `True` | Auto-detect UUIDs by `_id` suffix |

### Factory Methods

```python
# Strict UUID detection (no auto-detection)
store = SQLiteEventStore.with_strict_uuid_detection(
    database="./events.db",
    uuid_fields={"event_id", "aggregate_id", "tenant_id"},
)
```

### Context Manager Usage

Always use the async context manager:

```python
# Recommended: automatic cleanup
async with SQLiteEventStore("./events.db") as store:
    await store.initialize()
    # ... use store
# Connection automatically closed

# Manual (if needed)
store = SQLiteEventStore("./events.db")
await store._connect()
await store.initialize()
try:
    # ... use store
finally:
    await store.close()
```

### Properties

```python
store.database        # "./events.db"
store.event_registry  # EventRegistry instance
store.is_connected    # True if connection is open
store.wal_mode        # True/False
store.busy_timeout    # Timeout in ms
```

### Features

| Feature | Description |
|---------|-------------|
| **Optimistic locking** | Version-based conflict detection |
| **Idempotent writes** | Duplicate events silently skipped |
| **WAL mode** | Better concurrent read performance |
| **Multi-tenancy** | `tenant_id` field for isolation |
| **OpenTelemetry tracing** | Optional performance monitoring |
| **Auto schema creation** | `initialize()` creates tables |

### SQLite Type Adaptations

SQLite stores all data as TEXT, but the store handles conversion transparently:

| Python Type | SQLite Storage | Notes |
|-------------|----------------|-------|
| `UUID` | TEXT | 36-char hyphenated format |
| `datetime` | TEXT | ISO 8601 format |
| `dict` (payload) | TEXT | JSON serialized |
| `int` (id) | INTEGER | Auto-increment |

### Database Schema

Created automatically by `initialize()`:

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Global position
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    tenant_id TEXT,
    actor_id TEXT,
    version INTEGER NOT NULL,              -- Stream position
    timestamp TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(aggregate_id, aggregate_type, version)
);

CREATE INDEX idx_events_aggregate ON events(aggregate_id, aggregate_type);
CREATE INDEX idx_events_type ON events(aggregate_type);
CREATE INDEX idx_events_tenant ON events(tenant_id);
CREATE INDEX idx_events_timestamp ON events(timestamp);
```

### Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| Single writer | Only one concurrent write | Use PostgreSQL for high write loads |
| No network sharing | Cannot access from multiple machines | Use PostgreSQL for distributed systems |
| Busy timeout | Writes may fail under contention | Increase `busy_timeout` or retry |

### When to Use

- Local development (no database setup)
- Unit and integration tests (fast, isolated)
- CI/CD pipelines
- Single-instance deployments
- Embedded/edge applications
- Desktop applications

### When NOT to Use

- High-throughput production
- Multi-instance deployments
- Heavy concurrent writes
- Distributed systems

### Example

```python
import asyncio
from uuid import uuid4
from eventsource import SQLiteEventStore

async def main():
    async with SQLiteEventStore("./app.db") as store:
        await store.initialize()

        order_id = uuid4()

        # Append events
        result = await store.append_events(
            aggregate_id=order_id,
            aggregate_type="Order",
            events=[OrderCreated(aggregate_id=order_id, ...)],
            expected_version=0,
        )
        print(f"Version: {result.new_version}, Position: {result.global_position}")

        # Read events
        stream = await store.get_events(order_id, "Order")
        print(f"Order has {stream.version} events")

        # Stream all events
        async for stored in store.read_all():
            print(f"[{stored.global_position}] {stored.event_type}")

asyncio.run(main())
```

See [SQLite Backend Guide](../guides/sqlite-backend.md) for comprehensive usage patterns.

---

## Choosing an Event Store

Use this comparison to select the right store for your use case:

| Feature | InMemoryEventStore | SQLiteEventStore | PostgreSQLEventStore |
|---------|-------------------|------------------|---------------------|
| **Persistence** | None | File or memory | Full ACID |
| **Setup complexity** | None | None | Database required |
| **Concurrency** | Single process | Single writer | Full multi-writer |
| **Performance** | Fastest | Fast | Production-grade |
| **Scalability** | None | Limited | Horizontal |
| **Distributed** | No | No | Yes |
| **Multi-tenancy** | Yes | Yes | Yes |
| **Tracing** | Optional | Optional | Optional |
| **Best for** | Tests | Dev/Embedded | Production |

### Decision Guide

```
Need persistence?
├── No  → InMemoryEventStore
└── Yes → Need concurrent writes from multiple instances?
          ├── No  → SQLiteEventStore
          └── Yes → PostgreSQLEventStore
```

### Migration Path

All stores implement the same `EventStore` interface, making migration easy:

```python
# Development
store = InMemoryEventStore()

# Testing with persistence
store = SQLiteEventStore(":memory:")

# Production
store = PostgreSQLEventStore(session_factory)

# Your code doesn't change!
repo = AggregateRepository(event_store=store, ...)
```

---

## TypeConverter

When events are stored, UUIDs and datetimes become JSON strings. The `TypeConverter` handles converting them back to proper Python types during deserialization.

```python
from eventsource.stores import (
    TypeConverter,           # Protocol
    DefaultTypeConverter,    # Default implementation
    DEFAULT_UUID_FIELDS,     # Default UUID field names
    DEFAULT_STRING_ID_FIELDS, # Default exclusions
)
```

### TypeConverter Protocol

```python
from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class TypeConverter(Protocol):
    """Protocol for type conversion during event deserialization."""

    def convert_types(self, data: Any) -> Any:
        """Recursively convert string types to proper Python types."""
        ...

    def is_uuid_field(self, key: str) -> bool:
        """Determine if a field should be treated as UUID."""
        ...

    def is_datetime_field(self, key: str) -> bool:
        """Determine if a field should be treated as datetime."""
        ...
```

### DefaultTypeConverter

The default implementation with configurable UUID detection.

#### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uuid_fields` | `set[str]` | `None` | Additional field names to treat as UUIDs |
| `string_id_fields` | `set[str]` | `None` | Field names that should NOT be treated as UUIDs |
| `auto_detect_uuid` | `bool` | `True` | Whether `_id` suffix triggers UUID detection |
| `use_defaults` | `bool` | `True` | Whether to merge with default field sets |

#### Default Field Sets

**DEFAULT_UUID_FIELDS** - Fields always converted to UUID:
```python
{"event_id", "aggregate_id", "tenant_id", "correlation_id",
 "causation_id", "template_id", "issuance_id", "user_id"}
```

**DEFAULT_STRING_ID_FIELDS** - Fields never converted to UUID (exclusions):
```python
{"actor_id", "issuer_id", "recipient_id", "invited_by",
 "assigned_by", "revoked_by", "deactivated_by", "reactivated_by", "removed_by"}
```

#### UUID Detection Logic

1. If field name is in `uuid_fields`, return `True`
2. If field name is in `string_id_fields`, return `False`
3. If `auto_detect_uuid` is `True` and field name ends with `_id`, return `True`
4. Otherwise, return `False`

#### Datetime Detection Logic

1. If field name is `"occurred_at"`, return `True`
2. If field name ends with `"_at"`, return `True`
3. Otherwise, return `False`

### Usage Examples

#### Default Configuration

```python
from eventsource.stores import DefaultTypeConverter

converter = DefaultTypeConverter()
data = {"event_id": "550e8400-e29b-41d4-a716-446655440000"}
result = converter.convert_types(data)
# result["event_id"] is now a UUID object
```

#### Custom UUID Fields

```python
converter = DefaultTypeConverter(
    uuid_fields={"reference_id", "source_id"},
)

# reference_id and source_id will now be converted to UUID
```

#### Excluding String ID Fields

```python
converter = DefaultTypeConverter(
    string_id_fields={"stripe_customer_id", "external_user_id"},
)

# These fields will remain as strings even though they end in _id
```

#### Strict Mode (No Auto-Detection)

```python
converter = DefaultTypeConverter.strict(
    uuid_fields={"event_id", "aggregate_id", "tenant_id"},
)

# ONLY these three fields will be converted to UUID
# No auto-detection based on _id suffix
```

### Using with Event Stores

Both PostgreSQLEventStore and SQLiteEventStore support type converter configuration:

```python
# Direct converter injection
converter = DefaultTypeConverter(uuid_fields={"custom_reference_id"})
store = PostgreSQLEventStore(session_factory, type_converter=converter)

# Or via constructor parameters (creates converter internally)
store = PostgreSQLEventStore(
    session_factory,
    uuid_fields={"custom_reference_id"},
    string_id_fields={"external_id"},
    auto_detect_uuid=True,
)

# Strict mode via factory method
store = SQLiteEventStore.with_strict_uuid_detection(
    database="./events.db",
    uuid_fields={"event_id", "aggregate_id", "tenant_id"},
)
```

### Custom TypeConverter

Implement the protocol for custom type conversion:

```python
class MyCustomConverter:
    """Custom converter for company-specific field naming."""

    def convert_types(self, data: Any) -> Any:
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if isinstance(value, str) and self.is_uuid_field(key):
                    try:
                        result[key] = UUID(value)
                    except ValueError:
                        result[key] = value
                elif isinstance(value, dict):
                    result[key] = self.convert_types(value)
                else:
                    result[key] = value
            return result
        return data

    def is_uuid_field(self, key: str) -> bool:
        # Company convention: UUIDs end with _uuid
        return key.endswith("_uuid")

    def is_datetime_field(self, key: str) -> bool:
        # Company convention: timestamps end with _timestamp
        return key.endswith("_timestamp")

# Use with any event store
store = PostgreSQLEventStore(session_factory, type_converter=MyCustomConverter())
```

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

### Use Repository Pattern

Prefer the repository over direct store access. The repository handles optimistic locking automatically:

```python
# Recommended: Repository pattern
repo = AggregateRepository(event_store=store, ...)
order = await repo.load(order_id)
order.ship(tracking_number)
await repo.save(order)  # Handles versioning internally

# Direct store access (lower-level, manual versioning)
stream = await store.get_events(order_id, "Order")
# ... create events manually ...
await store.append_events(..., expected_version=stream.version)
```

### Handle Concurrency Conflicts

Always handle `OptimisticLockError` with retry logic:

```python
from eventsource import OptimisticLockError

MAX_RETRIES = 3

for attempt in range(MAX_RETRIES):
    try:
        order = await repo.load(order_id)
        order.ship(tracking_number)
        await repo.save(order)
        break
    except OptimisticLockError:
        if attempt == MAX_RETRIES - 1:
            raise  # Give up after max retries
        # Reload and retry
        continue
```

### Idempotency

Use unique event IDs to prevent duplicate processing:

```python
# Each event has a unique event_id
if await store.event_exists(event.event_id):
    return  # Already processed, skip

# Or use deterministic event IDs for idempotent commands
event_id = uuid5(NAMESPACE, f"{command_id}:{command_type}")
```

### Batch Processing for Projections

Use streaming methods for memory efficiency:

```python
# Process in batches
last_position = await checkpoint_repo.get_position("MyProjection")

options = ReadOptions(from_position=last_position, limit=1000)
async for stored_event in store.read_all(options):
    await projection.handle(stored_event.event)
    last_position = stored_event.global_position

await checkpoint_repo.save_position("MyProjection", last_position)
```

### Timestamp Filtering for Partitioned Tables

Include timestamp bounds to enable partition pruning in PostgreSQL:

```python
from datetime import datetime, timedelta, UTC

# Instead of scanning all partitions
stream = await store.get_events(order_id, "Order")

# Scan only relevant partitions
recent = datetime.now(UTC) - timedelta(days=30)
stream = await store.get_events(
    order_id, "Order",
    from_timestamp=recent,
)
```

### Testing Best Practices

```python
import pytest
from eventsource import InMemoryEventStore, SQLiteEventStore

# Fast unit tests: InMemoryEventStore
@pytest.fixture
def store():
    return InMemoryEventStore()

# Integration tests with persistence: SQLiteEventStore in-memory
@pytest.fixture
async def store():
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        yield store

# Reset between tests
@pytest.fixture(autouse=True)
async def reset_store(store):
    yield
    if hasattr(store, 'clear'):
        await store.clear()
```

---

## See Also

- [Architecture Overview](../architecture.md) - How event stores fit in the system
- [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) - Design rationale
- [ADR-0007: Remove Sync Event Store](../adrs/0007-remove-sync-event-store.md) - Async-only design
- [SQLite Backend Guide](../guides/sqlite-backend.md) - SQLite patterns and configuration
- [Repository Pattern Guide](../guides/repository-pattern.md) - Higher-level aggregate access
- [Subscriptions Guide](../guides/subscriptions.md) - Processing events from the store
- [SQLite Usage Examples](../examples/sqlite-usage.md) - Practical code examples
