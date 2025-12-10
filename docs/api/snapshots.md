# Snapshots API Reference

This document covers the snapshot storage interface and implementations for optimizing aggregate loading performance.

## Overview

Snapshots capture point-in-time aggregate state, enabling fast aggregate loading by skipping event replay for events before the snapshot. This is a critical optimization for long-lived aggregates with many events.

```python
from eventsource.snapshots import (
    # Core types
    Snapshot,
    SnapshotStore,

    # Implementations
    InMemorySnapshotStore,
    PostgreSQLSnapshotStore,
    SQLiteSnapshotStore,

    # Exceptions
    SnapshotError,
    SnapshotDeserializationError,
    SnapshotSchemaVersionError,
    SnapshotNotFoundError,
)

# Snapshot strategies (advanced usage)
from eventsource.snapshots.strategies import (
    SnapshotStrategy,
    ThresholdSnapshotStrategy,
    BackgroundSnapshotStrategy,
    NoSnapshotStrategy,
    create_snapshot_strategy,
)
```

---

## Snapshot

Immutable data structure representing captured aggregate state.

### Class Definition

```python
@dataclass(frozen=True)
class Snapshot:
    """Point-in-time capture of aggregate state."""

    aggregate_id: UUID
    aggregate_type: str
    version: int
    state: dict[str, Any]
    schema_version: int
    created_at: datetime
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `aggregate_id` | `UUID` | Unique identifier of the aggregate instance |
| `aggregate_type` | `str` | Type name of the aggregate (e.g., "Order") |
| `version` | `int` | Aggregate version when snapshot was taken |
| `state` | `dict[str, Any]` | Serialized aggregate state as JSON-compatible dictionary |
| `schema_version` | `int` | Version of the aggregate's state schema |
| `created_at` | `datetime` | Timestamp when snapshot was created |

### Example

```python
from eventsource.snapshots import Snapshot
from datetime import datetime, UTC
from uuid import uuid4

snapshot = Snapshot(
    aggregate_id=uuid4(),
    aggregate_type="Order",
    version=100,
    state={"order_id": "...", "status": "shipped", "items": [...]},
    schema_version=1,
    created_at=datetime.now(UTC),
)

print(snapshot)
# Output: Snapshot(Order/550e8400-e29b-41d4-a716-446655440000, v100, schema_v1)
```

### Design Notes

- Snapshots are immutable (`frozen=True`) for consistency
- One snapshot per aggregate at any time (latest only)
- Snapshots can be safely deleted and regenerated from events
- Schema version mismatch triggers full event replay

---

## SnapshotStore Interface

Abstract base class for snapshot storage implementations.

### Class Definition

```python
class SnapshotStore(ABC):
    """Abstract base class for snapshot storage."""

    @abstractmethod
    async def save_snapshot(self, snapshot: Snapshot) -> None: ...

    @abstractmethod
    async def get_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> Snapshot | None: ...

    @abstractmethod
    async def delete_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool: ...

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool: ...

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int: ...
```

### Methods

#### `save_snapshot(snapshot)`

Save or update a snapshot (upsert semantics).

```python
async def save_snapshot(self, snapshot: Snapshot) -> None
```

**Parameters:**
- `snapshot`: The snapshot to save. Must have all fields populated.

**Example:**
```python
from datetime import datetime, UTC
from eventsource.snapshots import Snapshot

snapshot = Snapshot(
    aggregate_id=order.aggregate_id,
    aggregate_type="Order",
    version=order.version,
    state=order.state.model_dump(mode="json"),
    schema_version=1,
    created_at=datetime.now(UTC),
)
await snapshot_store.save_snapshot(snapshot)
```

#### `get_snapshot(aggregate_id, aggregate_type)`

Get the latest snapshot for an aggregate.

```python
async def get_snapshot(
    self,
    aggregate_id: UUID,
    aggregate_type: str,
) -> Snapshot | None
```

**Parameters:**
- `aggregate_id`: Unique identifier of the aggregate
- `aggregate_type`: Type name of the aggregate (e.g., "Order")

**Returns:** The snapshot if found, `None` otherwise.

**Example:**
```python
snapshot = await snapshot_store.get_snapshot(order_id, "Order")
if snapshot:
    print(f"Found snapshot at version {snapshot.version}")
else:
    print("No snapshot, will replay all events")
```

#### `delete_snapshot(aggregate_id, aggregate_type)`

Delete the snapshot for an aggregate.

```python
async def delete_snapshot(
    self,
    aggregate_id: UUID,
    aggregate_type: str,
) -> bool
```

**Returns:** `True` if a snapshot was deleted, `False` if none existed.

**Note:** Deleting a snapshot does NOT affect the aggregate or its events. The snapshot will be regenerated on the next save if snapshotting is enabled.

#### `snapshot_exists(aggregate_id, aggregate_type)`

Check if a snapshot exists for an aggregate.

```python
async def snapshot_exists(
    self,
    aggregate_id: UUID,
    aggregate_type: str,
) -> bool
```

**Returns:** `True` if snapshot exists, `False` otherwise.

#### `delete_snapshots_by_type(aggregate_type, schema_version_below)`

Delete snapshots for all aggregates of a given type.

```python
async def delete_snapshots_by_type(
    self,
    aggregate_type: str,
    schema_version_below: int | None = None,
) -> int
```

**Parameters:**
- `aggregate_type`: Type name of aggregates to delete snapshots for
- `schema_version_below`: If provided, only delete snapshots with schema_version less than this value

**Returns:** Number of snapshots deleted.

**Example:**
```python
# Delete all Order snapshots with old schema
count = await snapshot_store.delete_snapshots_by_type(
    aggregate_type="Order",
    schema_version_below=2,
)
print(f"Invalidated {count} old snapshots")
```

---

## InMemorySnapshotStore

In-memory implementation for testing and development.

### Usage

```python
from eventsource.snapshots import InMemorySnapshotStore

store = InMemorySnapshotStore()

# Save snapshot
await store.save_snapshot(snapshot)

# Retrieve snapshot
loaded = await store.get_snapshot(aggregate_id, "Order")

# Testing utilities
await store.clear()  # Reset state between tests
count = store.snapshot_count  # Get current count
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing (if available) |

### Characteristics

- **Thread-safe**: Uses asyncio.Lock for concurrent access
- **Non-persistent**: Data lost when process terminates
- **Fast**: All operations in memory
- **Full delete_snapshots_by_type support**: Efficient in-memory filtering
- **OpenTelemetry tracing**: Automatic tracing when OpenTelemetry is available

### Additional Methods

#### `clear()`

Clear all snapshots from the store. Useful for test cleanup.

```python
async def clear(self) -> None
```

#### `snapshot_count`

Property that returns the number of snapshots currently stored.

```python
@property
def snapshot_count(self) -> int
```

### When to Use

- Unit testing
- Development environments
- Prototyping
- Single-process applications with ephemeral state

### When NOT to Use

- Production deployments requiring persistence
- Distributed systems
- Multi-process environments

---

## PostgreSQLSnapshotStore

Production-ready PostgreSQL implementation.

### Setup

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource.snapshots import PostgreSQLSnapshotStore

engine = create_async_engine(
    "postgresql+asyncpg://user:pass@localhost/mydb",
    pool_size=10,
    max_overflow=20,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

store = PostgreSQLSnapshotStore(session_factory)
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_factory` | `async_sessionmaker` | Required | SQLAlchemy async session factory |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing (if available) |

### Features

- **Upsert semantics**: INSERT ON CONFLICT UPDATE for atomic save
- **Efficient queries**: Single-row lookups by aggregate_id + aggregate_type
- **Bulk operations**: delete_snapshots_by_type with schema version filtering
- **OpenTelemetry tracing**: Automatic tracing when OpenTelemetry is available
- **Optimized EXISTS**: snapshot_exists uses SQL EXISTS for efficiency

### OpenTelemetry Tracing

Tracing is enabled by default when OpenTelemetry is installed:

```python
# Tracing enabled by default
store = PostgreSQLSnapshotStore(session_factory)

# Explicitly disable tracing
store = PostgreSQLSnapshotStore(session_factory, enable_tracing=False)
```

Traced operations include:
- `eventsource.snapshot.save`
- `eventsource.snapshot.get`
- `eventsource.snapshot.delete`
- `eventsource.snapshot.exists`
- `eventsource.snapshot.delete_by_type`

### Database Schema

Requires the `snapshots` table. Create using migrations:

```python
from eventsource.migrations import get_schema

schema_sql = get_schema("snapshots")
# Execute schema_sql against your database
```

Or create manually:

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_snapshots_aggregate UNIQUE (aggregate_id, aggregate_type)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_lookup
    ON snapshots(aggregate_id, aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type
    ON snapshots(aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version
    ON snapshots(aggregate_type, schema_version);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
    ON snapshots(created_at);
```

---

## SQLiteSnapshotStore

Lightweight SQLite implementation for embedded deployments.

### Installation

```bash
pip install eventsource[sqlite]
```

### Setup

```python
from eventsource.snapshots import SQLiteSnapshotStore

# File-based database
store = SQLiteSnapshotStore("./snapshots.db")

# In-memory database for testing
store = SQLiteSnapshotStore(":memory:")
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database_path` | `str` | Required | Path to SQLite file or `:memory:` |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing (if available) |

### Features

- **File-based persistence**: Data survives process restart
- **No external database**: Self-contained in a single file
- **Async operations**: Uses aiosqlite driver
- **INSERT OR REPLACE**: Efficient upsert for save operations
- **OpenTelemetry tracing**: Automatic tracing when OpenTelemetry is available

### Database Schema

Create the table before use:

```python
from eventsource.migrations import get_schema

schema_sql = get_schema("snapshots", backend="sqlite")
# Execute schema_sql against your database
```

Or create manually:

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(aggregate_id, aggregate_type)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_lookup
    ON snapshots(aggregate_id, aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type
    ON snapshots(aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version
    ON snapshots(aggregate_type, schema_version);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
    ON snapshots(created_at);
```

### Limitations

- **Single writer**: Only one write operation at a time
- **Local only**: Cannot share database across machines

### When to Use

- Local development without database setup
- Integration testing
- Single-instance deployments
- Desktop applications
- Edge computing scenarios

---

## Snapshot Strategies

Strategies encapsulate when and how snapshots are created. The repository uses strategies internally based on the `snapshot_mode` parameter.

### Strategy Protocol

```python
from eventsource.snapshots.strategies import SnapshotStrategy

class SnapshotStrategy(Protocol):
    def should_snapshot(
        self,
        aggregate: AggregateRoot[Any],
        events_since_snapshot: int,
    ) -> bool: ...

    async def execute_snapshot(
        self,
        aggregate: AggregateRoot[Any],
        snapshot_store: SnapshotStore,
        aggregate_type: str,
    ) -> Snapshot | None: ...
```

### ThresholdSnapshotStrategy

Creates snapshots synchronously at threshold boundaries.

```python
from eventsource.snapshots.strategies import ThresholdSnapshotStrategy

strategy = ThresholdSnapshotStrategy(threshold=100)
# Snapshots created at versions 100, 200, 300, etc.
```

**Best for:** Development, testing, low-throughput scenarios.

### BackgroundSnapshotStrategy

Creates snapshots asynchronously in background tasks.

```python
from eventsource.snapshots.strategies import BackgroundSnapshotStrategy

strategy = BackgroundSnapshotStrategy(threshold=100)

# Wait for pending snapshots (useful in tests)
count = await strategy.await_pending()
print(f"Pending: {strategy.pending_count}")
```

**Best for:** High-throughput production scenarios.

### NoSnapshotStrategy

Never creates snapshots automatically (manual mode).

```python
from eventsource.snapshots.strategies import NoSnapshotStrategy

strategy = NoSnapshotStrategy()
strategy.should_snapshot(aggregate, 1000)  # Always returns False
```

**Best for:** Full control over snapshot timing.

### Factory Function

Create strategies from mode strings (used internally by repository):

```python
from eventsource.snapshots.strategies import create_snapshot_strategy

# Equivalent ways to create strategies
strategy = create_snapshot_strategy("sync", threshold=100)
strategy = create_snapshot_strategy("background", threshold=100)
strategy = create_snapshot_strategy("manual")
```

---

## Exceptions

### SnapshotError

Base exception for all snapshot-related errors.

```python
from eventsource.snapshots import SnapshotError

try:
    snapshot = await load_snapshot(aggregate_id)
except SnapshotError:
    # Fall back to full event replay
    pass
```

### SnapshotDeserializationError

Raised when a snapshot cannot be deserialized.

```python
from eventsource.snapshots import SnapshotDeserializationError

try:
    state = state_type.model_validate(snapshot.state)
except SnapshotDeserializationError as e:
    print(f"Failed for {e.aggregate_type}/{e.aggregate_id}")
    print(f"Original error: {e.original_error}")
```

**Attributes:**
- `aggregate_id`: ID of the aggregate whose snapshot failed
- `aggregate_type`: Type of the aggregate
- `original_error`: The underlying deserialization error

### SnapshotSchemaVersionError

Raised when snapshot schema version doesn't match aggregate schema version.

```python
from eventsource.snapshots import SnapshotSchemaVersionError

# When schema mismatch is detected:
# - snapshot_schema_version: Version in stored snapshot
# - expected_schema_version: Version in aggregate class
```

**Attributes:**
- `aggregate_id`: ID of the aggregate
- `aggregate_type`: Type of the aggregate
- `snapshot_schema_version`: Schema version in the snapshot
- `expected_schema_version`: Schema version expected by aggregate

### SnapshotNotFoundError

Raised when a snapshot is expected but not found.

```python
from eventsource.snapshots import SnapshotNotFoundError

# Rarely raised - missing snapshots usually just trigger full replay
```

**Note:** This exception is rarely raised in practice because missing snapshots are handled gracefully with fallback to event replay.

---

## AggregateRepository Snapshot Integration

The `AggregateRepository` has built-in snapshot support.

### Constructor Parameters (Snapshot-Related)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `snapshot_store` | `SnapshotStore \| None` | `None` | Snapshot store for state caching |
| `snapshot_threshold` | `int \| None` | `None` | Events between automatic snapshots |
| `snapshot_mode` | `"sync" \| "background" \| "manual"` | `"sync"` | When to create snapshots |

### Example

```python
from eventsource import AggregateRepository, InMemoryEventStore
from eventsource.snapshots import InMemorySnapshotStore

repo = AggregateRepository(
    event_store=InMemoryEventStore(),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=InMemorySnapshotStore(),
    snapshot_threshold=100,  # Snapshot every 100 events
    snapshot_mode="sync",
)
```

### Snapshot Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `"sync"` | Create snapshot immediately after save | Simple, predictable |
| `"background"` | Create snapshot asynchronously | High-throughput |
| `"manual"` | Only via explicit `create_snapshot()` | Full control |

### Repository Methods

#### `create_snapshot(aggregate)`

Manually create a snapshot.

```python
order = await repo.load(order_id)
snapshot = await repo.create_snapshot(order)
print(f"Created snapshot at version {snapshot.version}")
```

#### `await_pending_snapshots()`

Wait for background snapshot tasks (useful for testing).

```python
await repo.save(aggregate)  # Triggers background snapshot
count = await repo.await_pending_snapshots()
print(f"Waited for {count} background snapshots")
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `snapshot_store` | `SnapshotStore \| None` | The configured snapshot store |
| `snapshot_threshold` | `int \| None` | Events between snapshots |
| `snapshot_mode` | `str` | Current snapshot mode |
| `has_snapshot_support` | `bool` | Whether snapshotting is enabled |
| `pending_snapshot_count` | `int` | Background tasks not yet complete |

---

## AggregateRoot Snapshot Support

Aggregates support snapshotting through inherited methods.

### Class Attribute

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 1  # Increment when OrderState structure changes
```

### Internal Methods

These methods are used internally by `AggregateRepository`:

#### `_serialize_state()`

Convert state to dictionary for snapshot storage.

```python
state_dict = aggregate._serialize_state()
# Returns: {"order_id": "...", "status": "shipped", ...}
```

#### `_restore_from_snapshot(state_dict, version)`

Restore aggregate state from snapshot.

```python
aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
```

#### `_get_state_type()`

Get the TState type for deserialization.

```python
state_type = aggregate._get_state_type()
# Returns: OrderState (the class, not an instance)
```

---

## Best Practices

### Choosing Snapshot Threshold

```python
# Few events per aggregate - higher threshold
snapshot_threshold=500  # For aggregates with ~10 events/day

# Many events per aggregate - lower threshold
snapshot_threshold=50   # For aggregates with ~100 events/day

# Very high event rate - low threshold
snapshot_threshold=20   # For IoT or high-frequency updates
```

### Schema Versioning

Always increment `schema_version` when state structure changes:

```python
# v1: Original state
class OrderState(BaseModel):
    order_id: UUID
    status: str

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 1

# v2: Added new field
class OrderState(BaseModel):
    order_id: UUID
    status: str
    priority: int = 0  # New field

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 2  # Increment to invalidate old snapshots
```

### Production Configuration

```python
# PostgreSQL with background snapshots
from eventsource.snapshots import PostgreSQLSnapshotStore

store = PostgreSQLSnapshotStore(session_factory, tracer=tracer)
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=store,
    snapshot_threshold=100,
    snapshot_mode="background",  # Non-blocking saves
)
```

---

## See Also

- [Snapshotting Guide](../guides/snapshotting.md) - How to enable and use snapshotting
- [Migration Guide](../guides/snapshotting-migration.md) - Adding snapshotting to existing projects
- [Snapshotting Example](../examples/snapshotting.md) - Working code examples
- [Aggregates API](./aggregates.md) - AggregateRoot and AggregateRepository
