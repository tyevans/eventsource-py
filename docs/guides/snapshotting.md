# Snapshotting User Guide

This guide explains how to enable and configure aggregate snapshotting to optimize load performance in your event-sourced application.

## Why Use Snapshotting?

In event sourcing, aggregates are reconstituted by replaying their entire event history. As aggregates accumulate events over time, this replay becomes increasingly expensive:

| Event Count | Load Time (No Snapshot) | Load Time (With Snapshot) |
|-------------|------------------------|---------------------------|
| 100 events | ~50ms | ~15ms |
| 1,000 events | ~500ms | ~20ms |
| 10,000 events | ~5 seconds | ~60ms |
| 100,000 events | ~60 seconds | ~100ms |

Snapshotting captures a point-in-time state of the aggregate, allowing the system to:

1. Load the snapshot (fast - single database read)
2. Replay only events since the snapshot (small subset)
3. Return the fully hydrated aggregate

**Key principle:** Snapshots are optimizations only. Events remain the source of truth. Snapshots can be deleted and regenerated at any time.

---

## Quick Start

### 1. Add Schema Version to Your Aggregate

```python
from eventsource import AggregateRoot
from pydantic import BaseModel

class OrderState(BaseModel):
    order_id: UUID
    status: str = "draft"
    items: list[dict] = []

class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 1  # Add this line

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    # ... rest of implementation
```

### 2. Configure Repository with Snapshot Store

```python
from eventsource import AggregateRepository
from eventsource.snapshots import InMemorySnapshotStore

# Create snapshot store
snapshot_store = InMemorySnapshotStore()

# Configure repository
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Snapshot every 100 events
)
```

### 3. Use Repository Normally

```python
# Load aggregate - automatically uses snapshot if available
order = await repo.load(order_id)

# Make changes
order.add_item(product_id, "Widget", 2, 29.99)

# Save - automatically creates snapshot at threshold
await repo.save(order)
```

That's it! The repository handles snapshot creation and loading transparently.

---

## Choosing a Snapshot Store

### InMemorySnapshotStore

Best for: Testing, development, single-process applications

```python
from eventsource.snapshots import InMemorySnapshotStore

snapshot_store = InMemorySnapshotStore()
```

**Pros:**
- No database setup required
- Very fast operations
- Easy to clear between tests

**Cons:**
- Data lost on process restart
- Not suitable for production

### PostgreSQLSnapshotStore

Best for: Production deployments

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource.snapshots import PostgreSQLSnapshotStore

engine = create_async_engine("postgresql+asyncpg://...")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

snapshot_store = PostgreSQLSnapshotStore(session_factory)
```

**Pros:**
- Production-ready persistence
- Supports distributed deployments
- Optional OpenTelemetry tracing

**Cons:**
- Requires PostgreSQL database
- Additional infrastructure

### SQLiteSnapshotStore

Best for: Embedded applications, single-instance deployments

```python
from eventsource.snapshots import SQLiteSnapshotStore

# File-based database
snapshot_store = SQLiteSnapshotStore("./snapshots.db")

# In-memory database for testing
snapshot_store = SQLiteSnapshotStore(":memory:")
```

**Pros:**
- File-based persistence
- No external database server
- Lightweight
- OpenTelemetry tracing support

**Cons:**
- Single-writer limitation
- Not suitable for distributed deployments

---

## Snapshot Configuration

### Snapshot Threshold

The `snapshot_threshold` controls how often snapshots are created:

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Create snapshot every 100 events
)
```

When `aggregate.version % threshold == 0`, a snapshot is created after save.

**Guidelines for choosing threshold:**

| Scenario | Suggested Threshold | Rationale |
|----------|-------------------|-----------|
| Low event rate (~10/day) | 500 | Events replay quickly anyway |
| Medium event rate (~100/day) | 100 | Balance of storage vs. speed |
| High event rate (~1000/day) | 50 | Frequent snapshots reduce replay |
| Very high rate (IoT) | 20 | Minimize replay at all costs |

### Snapshot Modes

The `snapshot_mode` controls when snapshots are created:

#### Sync Mode (Default)

Snapshot created synchronously after save. Simple and predictable.

```python
repo = AggregateRepository(
    ...,
    snapshot_mode="sync",  # Default
)
```

**Behavior:**
- `save()` completes after snapshot is written
- Guarantees snapshot is available for next load
- Adds latency to save operations (~10-50ms)

**Best for:** Most applications, lower throughput scenarios

#### Background Mode

Snapshot created asynchronously in a background task.

```python
repo = AggregateRepository(
    ...,
    snapshot_mode="background",
)
```

**Behavior:**
- `save()` returns immediately after events are persisted
- Snapshot created in background task
- Small chance of stale snapshot on immediate re-load

**Best for:** High-throughput scenarios, latency-sensitive operations

```python
# Wait for background snapshots (useful in tests)
await repo.await_pending_snapshots()
```

#### Manual Mode

Snapshots only created via explicit `create_snapshot()` call.

```python
repo = AggregateRepository(
    ...,
    snapshot_mode="manual",
)

# Explicitly create snapshot
order = await repo.load(order_id)
snapshot = await repo.create_snapshot(order)
```

**Behavior:**
- No automatic snapshot creation
- Full control over when snapshots are taken

**Best for:**
- Snapshot at specific business milestones
- Complex snapshotting strategies
- Pre-warming frequently accessed aggregates

---

## Schema Versioning

Schema versioning ensures snapshots remain compatible with aggregate state models.

### Why Schema Versioning?

When you change your aggregate's state model, existing snapshots may become incompatible:

```python
# Original state
class OrderState(BaseModel):
    order_id: UUID
    status: str

# Updated state - new required field
class OrderState(BaseModel):
    order_id: UUID
    status: str
    priority: int  # New field - old snapshots don't have this!
```

Loading an old snapshot would fail because it lacks the `priority` field.

### Using Schema Version

Increment `schema_version` when state structure changes:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 1  # Increment when OrderState changes incompatibly
```

When schema version mismatches:
1. Snapshot is ignored (logged as info)
2. Full event replay is performed
3. New snapshot created with current schema version (if threshold met)

### When to Increment Schema Version

**DO increment** when:
- Adding required fields without defaults
- Removing fields
- Changing field types incompatibly
- Renaming fields

**DON'T increment** when:
- Adding optional fields with defaults
- Adding new methods
- Changing validation rules only

### Migration Example

```python
# v1: Original
class OrderState(BaseModel):
    order_id: UUID
    status: str

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 1

# v2: Added required field
class OrderState(BaseModel):
    order_id: UUID
    status: str
    priority: int = 0  # Has default, so compatible

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 1  # No change needed - field has default

# v3: Changed field type
class OrderState(BaseModel):
    order_id: UUID
    status: str
    priority: str  # Changed from int to str!

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 2  # MUST increment - type changed
```

### Bulk Snapshot Invalidation

After schema changes, you can proactively invalidate old snapshots:

```python
# Invalidate all Order snapshots with schema_version < 2
count = await snapshot_store.delete_snapshots_by_type(
    aggregate_type="Order",
    schema_version_below=2,
)
print(f"Invalidated {count} old snapshots")
```

---

## Database Schema Setup

### PostgreSQL

Create the snapshots table:

```python
from eventsource.migrations import get_schema

schema_sql = get_schema("snapshots")
# Execute against your PostgreSQL database
```

Or use the SQL directly:

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

### SQLite

Create the snapshots table:

```python
from eventsource.migrations import get_schema

schema_sql = get_schema("snapshots", backend="sqlite")
# Execute against your SQLite database
```

Or use the SQL directly:

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

---

## How Loading Works

When you call `repo.load(aggregate_id)`:

```
1. Check for snapshot
   |
   v
2. Snapshot exists? ----No----> Get all events (from_version=0)
   |                                    |
   Yes                                  v
   |                            Apply all events
   v                                    |
3. Schema version matches? --No-->     |
   |                                    |
   Yes                                  |
   |                                    |
   v                                    |
4. Restore state from snapshot         |
   |                                    |
   v                                    |
5. Get events since snapshot           |
   (from_version=snapshot.version)     |
   |                                    |
   v                                    |
6. Apply recent events                 |
   |                                    |
   +----------> Return hydrated aggregate <---+
```

### Performance Breakdown

For an aggregate with 10,000 events and snapshot at version 9,900:

**Without snapshot:**
- Query: ~100ms (10,000 rows)
- Deserialize: ~2 seconds
- Apply events: ~3 seconds
- **Total: ~5 seconds**

**With snapshot:**
- Query snapshot: ~5ms (1 row)
- Deserialize snapshot: ~5ms
- Query events since snapshot: ~10ms (100 rows)
- Deserialize events: ~20ms
- Apply events: ~30ms
- **Total: ~70ms (98.6% faster)**

---

## Manual Snapshot Creation

Use `create_snapshot()` for explicit control:

```python
# Create snapshot at business milestone
order = await repo.load(order_id)
order.complete()
await repo.save(order)

# Explicitly create snapshot after completion
snapshot = await repo.create_snapshot(order)
print(f"Snapshot created at version {snapshot.version}")
```

### Use Cases

1. **Business milestones:** Snapshot after order completion, user verification, etc.
2. **Pre-warming:** Create snapshots for frequently accessed aggregates
3. **Before maintenance:** Ensure snapshots are fresh before migrations
4. **Testing:** Verify snapshot creation logic

---

## Best Practices

### 1. Start Without Snapshotting

Don't enable snapshotting until you need it:

```python
# Start simple
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
)

# Add snapshotting when performance requires it
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
)
```

### 2. Keep State Serializable

Ensure your state model serializes cleanly:

```python
class OrderState(BaseModel):
    order_id: UUID
    status: str
    items: list[OrderItem]  # Nested Pydantic models work
    total: Decimal  # Pydantic handles Decimal
    created_at: datetime  # Pydantic handles datetime
```

Avoid:
- Circular references
- Non-serializable objects
- Very large state (> 1MB)

### 3. Monitor Snapshot Performance

Track snapshot metrics:
- Hit rate (loads using snapshot vs. full replay)
- Snapshot size
- Creation time
- Load time improvement

OpenTelemetry tracing is enabled by default when the library is installed:

```python
# Tracing enabled by default
snapshot_store = PostgreSQLSnapshotStore(session_factory)

# Explicitly disable if needed
snapshot_store = PostgreSQLSnapshotStore(session_factory, enable_tracing=False)
```

### 4. Test with Snapshots

Include snapshot scenarios in your tests:

```python
async def test_aggregate_loads_from_snapshot():
    # Create aggregate with many events
    for _ in range(150):  # Past threshold of 100
        order.add_item(...)
    await repo.save(order)

    # Verify snapshot was created
    snapshot = await snapshot_store.get_snapshot(order.aggregate_id, "Order")
    assert snapshot is not None
    assert snapshot.version == 150

    # Load and verify state matches
    loaded = await repo.load(order.aggregate_id)
    assert loaded.state == order.state
```

### 5. Handle Schema Evolution Carefully

When changing state models:

1. Make backward-compatible changes when possible
2. Increment `schema_version` for breaking changes
3. Test that old aggregates load correctly after changes
4. Consider bulk snapshot invalidation for major changes

---

## Troubleshooting

### Snapshots Not Being Created

**Check:**
1. `snapshot_store` is configured
2. `snapshot_threshold` is set
3. `snapshot_mode` is not "manual"
4. Aggregate version is at threshold boundary

```python
# Debug
print(f"Has snapshot support: {repo.has_snapshot_support}")
print(f"Threshold: {repo.snapshot_threshold}")
print(f"Mode: {repo.snapshot_mode}")
print(f"Aggregate version: {aggregate.version}")
print(f"At threshold: {aggregate.version % repo.snapshot_threshold == 0}")
```

### Snapshots Being Ignored

**Check:**
1. Schema version matches
2. Snapshot state is valid
3. No deserialization errors

Check logs for:
```
INFO: Snapshot schema version mismatch for Order/...: snapshot has v1, aggregate expects v2
WARNING: Failed to restore from snapshot for Order/...: ValidationError
```

### Performance Not Improving

**Check:**
1. Snapshot is actually being used (check logs)
2. Threshold is appropriate for your event rate
3. Most time isn't in post-snapshot events

If aggregate has 10,000 events with snapshot at 9,000, you're still replaying 1,000 events. Lower the threshold.

### Background Snapshots Failing

Background snapshot failures are logged but don't fail saves:

```
WARNING: Background snapshot creation failed for Order/...: ConnectionError
```

**To investigate:**
```python
# Wait for and inspect background tasks
count = await repo.await_pending_snapshots()
print(f"Pending snapshots: {repo.pending_snapshot_count}")
```

---

## See Also

- [API Reference](../api/snapshots.md) - Detailed API documentation
- [Migration Guide](./snapshotting-migration.md) - Adding snapshotting to existing projects
- [Snapshotting Example](../examples/snapshotting.md) - Complete working example
