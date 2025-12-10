# Snapshotting Migration Guide

This guide covers how to add snapshotting to an existing event-sourced application with pre-existing aggregates and events.

## Overview

Adding snapshotting to an existing system requires:

1. Database schema updates (if using PostgreSQL/SQLite)
2. Adding `schema_version` to aggregate classes
3. Configuring repositories with snapshot stores
4. Optional: Pre-populating snapshots for existing aggregates

The migration is non-destructive - your existing events remain untouched and the system continues working if snapshots are unavailable.

---

## Step 1: Create the Snapshots Table

### PostgreSQL

Run the migration to create the snapshots table:

```python
from eventsource.migrations import get_schema

# Get the SQL for snapshots table
sql = get_schema("snapshots")
print(sql)
```

Execute the generated SQL:

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

```python
from eventsource.migrations import get_schema

sql = get_schema("snapshots", backend="sqlite")
```

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (aggregate_id, aggregate_type)
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

## Step 2: Add Schema Version to Aggregates

Add the `schema_version` class attribute to each aggregate that will use snapshotting:

### Before

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)
```

### After

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 1  # Add this line

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)
```

**Important:** Start with `schema_version = 1`. Increment this value whenever you make breaking changes to your state model (see [Schema Evolution](#schema-evolution) below).

---

## Step 3: Configure Repository with Snapshot Store

Update your repository configuration to include a snapshot store:

### Before

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
)
```

### After (PostgreSQL)

```python
from eventsource.snapshots import PostgreSQLSnapshotStore

snapshot_store = PostgreSQLSnapshotStore(session_factory)

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Snapshot every 100 events
    snapshot_mode="sync",    # Or "background" for async creation
)
```

### After (SQLite)

```python
from eventsource.snapshots import SQLiteSnapshotStore

snapshot_store = SQLiteSnapshotStore("snapshots.db")

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
)
```

---

## Step 4: Deploy and Verify

After deploying:

1. **Existing aggregates** will load normally (full event replay)
2. **On next save** that crosses a threshold boundary, a snapshot is created
3. **Subsequent loads** will use the snapshot + recent events

No immediate action is required - snapshots will be created organically as aggregates are modified.

---

## Optional: Pre-Populate Snapshots

For large aggregates, you may want to create snapshots proactively rather than waiting for organic updates.

### Batch Snapshot Creation

```python
async def create_snapshots_for_aggregate_type(
    repo: AggregateRepository,
    aggregate_ids: list[UUID],
) -> int:
    """Create snapshots for existing aggregates."""
    count = 0
    for aggregate_id in aggregate_ids:
        try:
            # Load aggregate (full replay)
            aggregate = await repo.load(aggregate_id)

            # Create snapshot manually
            await repo.create_snapshot(aggregate)
            count += 1

            if count % 100 == 0:
                print(f"Created {count} snapshots...")

        except Exception as e:
            print(f"Failed to snapshot {aggregate_id}: {e}")

    return count


# Usage
aggregate_ids = await get_all_order_ids()  # Your query
created = await create_snapshots_for_aggregate_type(repo, aggregate_ids)
print(f"Created {created} snapshots")
```

### Prioritize High-Event Aggregates

Focus on aggregates that will benefit most:

```python
# Query aggregates with many events
high_event_aggregates = await connection.execute("""
    SELECT aggregate_id
    FROM events
    WHERE aggregate_type = 'Order'
    GROUP BY aggregate_id
    HAVING COUNT(*) > 500
    ORDER BY COUNT(*) DESC
""")
```

---

## Schema Evolution

When you change your state model in a breaking way, increment `schema_version`:

### Example: Adding a Required Field

```python
# Version 1 state
class OrderStateV1(BaseModel):
    order_id: UUID
    status: str
    items: list[dict]

# Version 2 state - added customer_id
class OrderStateV2(BaseModel):
    order_id: UUID
    status: str
    items: list[dict]
    customer_id: UUID  # New required field
```

Update aggregate:

```python
class OrderAggregate(AggregateRoot[OrderStateV2]):
    aggregate_type = "Order"
    schema_version = 2  # Increment version

    def _get_initial_state(self) -> OrderStateV2:
        return OrderStateV2(
            order_id=self.aggregate_id,
            status="draft",
            items=[],
            customer_id=UUID("00000000-0000-0000-0000-000000000000"),
        )
```

### What Happens to Old Snapshots?

When an aggregate is loaded:

1. If snapshot exists with `schema_version < current`, it's **ignored**
2. Full event replay occurs
3. New snapshot is created at current schema version

Old snapshots are automatically invalidated - no manual cleanup required.

### Bulk Invalidation (Optional)

To proactively clean up old snapshots:

```python
# Delete all Order snapshots with schema_version < 2
deleted = await snapshot_store.delete_snapshots_by_type(
    "Order",
    schema_version_below=2,
)
print(f"Deleted {deleted} outdated snapshots")
```

---

## Rollback Plan

If issues arise, snapshotting can be disabled instantly:

### Option 1: Remove Snapshot Store

```python
# Simply remove snapshot_store parameter
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    # snapshot_store removed - back to full replay
)
```

### Option 2: Clear All Snapshots

```python
# Delete all snapshots for an aggregate type
await snapshot_store.delete_snapshots_by_type("Order")
```

Events are never modified, so reverting is always safe.

---

## Monitoring the Migration

### Check Snapshot Coverage

```sql
-- PostgreSQL: Count aggregates with/without snapshots
SELECT
    e.aggregate_type,
    COUNT(DISTINCT e.aggregate_id) as total_aggregates,
    COUNT(DISTINCT s.aggregate_id) as with_snapshot,
    COUNT(DISTINCT e.aggregate_id) - COUNT(DISTINCT s.aggregate_id) as without_snapshot
FROM events e
LEFT JOIN snapshots s
    ON e.aggregate_id = s.aggregate_id
    AND e.aggregate_type = s.aggregate_type
GROUP BY e.aggregate_type;
```

### Check Snapshot Freshness

```sql
-- Find stale snapshots (many events since snapshot)
SELECT
    s.aggregate_type,
    s.aggregate_id,
    s.version as snapshot_version,
    MAX(e.version) as current_version,
    MAX(e.version) - s.version as events_since_snapshot
FROM snapshots s
JOIN events e
    ON s.aggregate_id = e.aggregate_id
    AND s.aggregate_type = e.aggregate_type
GROUP BY s.aggregate_type, s.aggregate_id, s.version
HAVING MAX(e.version) - s.version > 100
ORDER BY events_since_snapshot DESC;
```

---

## Troubleshooting

### Snapshots Not Being Created

1. **Check threshold:** Aggregate must have `>= threshold` events
2. **Check schema_version:** Ensure aggregate has `schema_version` attribute
3. **Check logs:** Enable DEBUG logging for `eventsource.aggregates.repository`

```python
import logging
logging.getLogger("eventsource.aggregates.repository").setLevel(logging.DEBUG)
```

### Snapshot Deserialization Errors

If snapshots fail to deserialize (state model changed):

1. The system **automatically falls back** to full event replay
2. A warning is logged
3. A new snapshot is created on next save

To force regeneration:

```python
# Delete specific snapshot
await snapshot_store.delete_snapshot(aggregate_id, "Order")

# Or delete all for a type
await snapshot_store.delete_snapshots_by_type("Order")
```

### Performance Not Improving

1. **Check event count:** Snapshotting helps most with 100+ events
2. **Check snapshot age:** Very old snapshots may have many events to replay
3. **Consider lowering threshold:** Try `snapshot_threshold=50` for faster snapshots

---

## Related Documentation

- [Snapshotting User Guide](./snapshotting.md) - Complete feature documentation
- [Snapshots API Reference](../api/snapshots.md) - Detailed API documentation
- [Snapshotting Examples](../examples/snapshotting.md) - Code examples
