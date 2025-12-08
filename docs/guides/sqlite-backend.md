# SQLite Backend Guide

This guide covers using SQLite as a backend for eventsource. SQLite provides a lightweight, zero-configuration option perfect for development, testing, and embedded applications.

## Overview

The SQLite backend includes:

- **SQLiteEventStore**: Event storage with optimistic locking and full EventStore interface compliance
- **SQLiteCheckpointRepository**: Projection checkpoint tracking
- **SQLiteOutboxRepository**: Transactional outbox pattern support
- **SQLiteDLQRepository**: Dead letter queue for failed event processing

## Installation

Install eventsource with SQLite support:

```bash
pip install eventsource[sqlite]
```

This installs the `aiosqlite` package for async SQLite operations.

## Quick Start

```python
import asyncio
from uuid import uuid4
from eventsource import SQLiteEventStore, AggregateRepository
from my_app.aggregates import OrderAggregate

async def main():
    # Create store with file-based persistence
    async with SQLiteEventStore("./events.db") as store:
        # Initialize schema (creates tables if they don't exist)
        await store.initialize()

        # Create repository
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Use like any other event store
        order = repo.create_new(uuid4())
        order.create(customer_id=uuid4(), email="customer@example.com")
        await repo.save(order)

asyncio.run(main())
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | Required | Path to SQLite file or `:memory:` for in-memory database |
| `event_registry` | `EventRegistry` | Default registry | Registry for event type deserialization |
| `wal_mode` | `bool` | `True` | Enable WAL journal mode for better concurrent read performance |
| `busy_timeout` | `int` | `5000` | Milliseconds to wait when database is locked |

### Database Path Options

```python
# In-memory database (data lost when process exits)
store = SQLiteEventStore(":memory:")

# File-based database (persistent)
store = SQLiteEventStore("./events.db")

# Absolute path
store = SQLiteEventStore("/var/data/myapp/events.db")
```

### WAL Mode

WAL (Write-Ahead Logging) mode improves concurrent read performance while writes are happening:

```python
# Enabled by default (recommended for most cases)
store = SQLiteEventStore("./events.db", wal_mode=True)

# Disable for single-process or read-only scenarios
store = SQLiteEventStore("./events.db", wal_mode=False)
```

### Busy Timeout

When the database is locked by another connection, SQLite can wait before failing:

```python
# Wait up to 10 seconds for locked database
store = SQLiteEventStore("./events.db", busy_timeout=10000)

# Fail immediately if locked
store = SQLiteEventStore("./events.db", busy_timeout=0)
```

## SQLite vs PostgreSQL Comparison

| Feature | SQLite | PostgreSQL |
|---------|--------|------------|
| **Setup** | Zero configuration | Requires server |
| **Installation** | `pip install eventsource[sqlite]` | `pip install eventsource[postgresql]` |
| **Concurrency** | Single writer, concurrent readers | Multiple writers |
| **Scale** | Suitable for millions of events | Suitable for billions of events |
| **Distribution** | Single file | Network accessible |
| **Transactions** | Full ACID | Full ACID |
| **Use Case** | Dev/test/embedded/edge | Production workloads |
| **Deployment** | Embedded in application | Separate service |
| **Backup** | Copy file | pg_dump / replication |
| **Multi-tenancy** | Supported | Supported with better isolation |

### When to Use SQLite

**Best for:**

- Local development without database setup
- CI/CD pipelines and automated testing
- Single-instance deployments
- Embedded applications
- Edge computing scenarios
- Prototyping and proof-of-concept work
- Desktop applications with event sourcing

**Not recommended for:**

- High-throughput production workloads
- Multi-instance deployments (horizontal scaling)
- Applications requiring concurrent writes
- Systems with heavy write loads

### When to Use PostgreSQL

**Best for:**

- Production web applications
- High-concurrency workloads
- Distributed systems
- Multi-instance deployments
- Systems requiring advanced querying
- Enterprise applications

## Using the Repositories

### Checkpoint Repository

Track projection progress:

```python
import aiosqlite
from eventsource.repositories import SQLiteCheckpointRepository

async with aiosqlite.connect("events.db") as db:
    repo = SQLiteCheckpointRepository(db)

    # Update checkpoint after processing an event
    await repo.update_checkpoint(
        projection_name="OrderProjection",
        event_id=event.event_id,
        event_type=event.event_type,
    )

    # Get last processed event ID
    last_event_id = await repo.get_checkpoint("OrderProjection")

    # Get lag metrics
    metrics = await repo.get_lag_metrics(
        "OrderProjection",
        event_types=["OrderCreated", "OrderShipped"],
    )
```

### Outbox Repository

Implement reliable event publishing with the transactional outbox pattern:

```python
import aiosqlite
from eventsource.repositories import SQLiteOutboxRepository

async with aiosqlite.connect("events.db") as db:
    repo = SQLiteOutboxRepository(db)

    # Add event to outbox within your transaction
    outbox_id = await repo.add_event(order_created_event)

    # In your publisher worker:
    pending = await repo.get_pending_events(limit=100)
    for entry in pending:
        # Publish to external system
        await publish_to_message_bus(entry)
        # Mark as published
        await repo.mark_published(entry["id"])
```

### DLQ Repository

Handle failed event processing:

```python
import aiosqlite
from eventsource.repositories import SQLiteDLQRepository

async with aiosqlite.connect("events.db") as db:
    repo = SQLiteDLQRepository(db)

    # Record a failed event
    try:
        await projection.handle(event)
    except Exception as e:
        await repo.add_failed_event(
            event_id=event.event_id,
            projection_name="OrderProjection",
            event_type=event.event_type,
            event_data=event.model_dump(),
            error=e,
            retry_count=1,
        )

    # Get failed events for retry
    failed = await repo.get_failed_events(
        projection_name="OrderProjection",
        status="failed",
        limit=50,
    )

    # Mark as resolved after manual intervention
    await repo.mark_resolved(dlq_id=123, resolution="Fixed data issue")
```

## In-Memory Mode for Testing

The `:memory:` database is ideal for fast, isolated tests:

```python
import pytest
from eventsource import SQLiteEventStore, AggregateRepository

@pytest.fixture
async def event_store():
    """In-memory event store for isolated testing."""
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        yield store

@pytest.fixture
async def order_repo(event_store):
    """Order repository using in-memory store."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

@pytest.mark.asyncio
async def test_order_creation(order_repo):
    order_id = uuid4()
    order = order_repo.create_new(order_id)
    order.create(customer_id=uuid4(), email="test@example.com")
    await order_repo.save(order)

    loaded = await order_repo.load(order_id)
    assert loaded.state.status == "created"
```

### Benefits of In-Memory Testing

- **Speed**: No disk I/O, extremely fast test execution
- **Isolation**: Each test gets a fresh database
- **Cleanup**: Database automatically destroyed when connection closes
- **Parallelism**: Tests can run in parallel without conflicts

## Limitations

### Single Writer Limitation

SQLite allows only one writer at a time. Concurrent write attempts will:

1. Wait for `busy_timeout` milliseconds
2. Raise an error if the timeout expires

```python
# Handle busy database gracefully
import aiosqlite

try:
    result = await store.append_events(
        aggregate_id=order_id,
        aggregate_type="Order",
        events=[event],
        expected_version=current_version,
    )
except aiosqlite.OperationalError as e:
    if "database is locked" in str(e):
        # Retry after a delay
        await asyncio.sleep(0.1)
        # ... retry logic
```

### No Network Access

SQLite is an embedded database. Multiple application instances cannot share the same database file over a network effectively. For multi-instance deployments, use PostgreSQL.

### Limited Query Capabilities

SQLite lacks some PostgreSQL features:

- No `FILTER` clause (use `CASE WHEN` instead)
- No `ANY()` array operator
- No native JSONB type (uses TEXT)
- No table partitioning

The eventsource SQLite implementation handles these differences transparently.

## Migration Path to PostgreSQL

When your application outgrows SQLite, migrate to PostgreSQL:

### 1. Environment-Based Backend Selection

```python
import os
from eventsource import SQLiteEventStore, PostgreSQLEventStore

def create_event_store():
    backend = os.environ.get("EVENT_STORE_BACKEND", "sqlite")

    if backend == "sqlite":
        path = os.environ.get("SQLITE_PATH", "./events.db")
        return SQLiteEventStore(path)

    elif backend == "postgresql":
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

        engine = create_async_engine(os.environ["DATABASE_URL"])
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        return PostgreSQLEventStore(session_factory)

    raise ValueError(f"Unknown backend: {backend}")
```

### 2. Data Migration Script

```python
import asyncio
from eventsource import SQLiteEventStore, PostgreSQLEventStore

async def migrate_events():
    """Migrate events from SQLite to PostgreSQL."""
    async with SQLiteEventStore("./events.db") as sqlite_store:
        await sqlite_store.initialize()

        # Setup PostgreSQL store
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        engine = create_async_engine("postgresql+asyncpg://...")
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        pg_store = PostgreSQLEventStore(session_factory)

        # Migrate events in batches
        async for stored_event in sqlite_store.read_all():
            await pg_store.append_events(
                aggregate_id=stored_event.event.aggregate_id,
                aggregate_type=stored_event.event.aggregate_type,
                events=[stored_event.event],
                expected_version=-1,  # ExpectedVersion.ANY
            )

asyncio.run(migrate_events())
```

### 3. Gradual Migration Strategy

1. **Start with SQLite** during development
2. **Run integration tests** against both backends
3. **Deploy to staging** with PostgreSQL
4. **Migrate production data** using the migration script
5. **Switch production** to PostgreSQL

## Best Practices

### 1. Use Context Manager

Always use the async context manager to ensure proper cleanup:

```python
# Recommended
async with SQLiteEventStore("./events.db") as store:
    await store.initialize()
    # ... use store

# Also works but requires manual cleanup
store = SQLiteEventStore("./events.db")
await store._connect()
await store.initialize()
try:
    # ... use store
finally:
    await store.close()
```

### 2. Initialize Once

Call `initialize()` once at application startup:

```python
async def app_startup():
    global event_store
    event_store = SQLiteEventStore("./events.db")
    await event_store._connect()
    await event_store.initialize()

async def app_shutdown():
    await event_store.close()
```

### 3. Handle Optimistic Lock Errors

```python
from eventsource import OptimisticLockError

async def save_with_retry(repo, aggregate, max_retries=3):
    for attempt in range(max_retries):
        try:
            await repo.save(aggregate)
            return
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            # Reload and replay command
            aggregate = await repo.load(aggregate.aggregate_id)
            # Re-apply your command here
```

### 4. Use WAL Mode for Read-Heavy Workloads

```python
# WAL mode allows concurrent reads during writes
store = SQLiteEventStore(
    "./events.db",
    wal_mode=True,  # Default
)
```

### 5. Set Appropriate Busy Timeout

```python
# Longer timeout for applications with occasional contention
store = SQLiteEventStore(
    "./events.db",
    busy_timeout=10000,  # 10 seconds
)
```

## See Also

- [Event Stores API Reference](../api/stores.md) - Detailed API documentation
- [SQLite Usage Examples](../examples/sqlite-usage.md) - Complete code examples
- [Testing Patterns](../examples/testing.md) - Testing with event stores
- [Getting Started Guide](../getting-started.md) - Introduction to eventsource
