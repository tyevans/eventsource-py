# Tutorial 12: SQLite - Development and Embedded Event Store

**Difficulty:** Intermediate

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- Python 3.10 or higher
- Understanding of async/await
- `pip install eventsource-py[sqlite]`

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Set up SQLite as your event store for development and testing
2. Choose between in-memory and file-based databases
3. Configure SQLite-specific options (WAL mode, busy timeout)
4. Create the events table schema with proper indexes
5. Use SQLiteCheckpointRepository for projections
6. Understand SQLite limitations and when to migrate to PostgreSQL
7. Write pytest fixtures for SQLite-based testing
8. Migrate from SQLite to PostgreSQL when scaling up

---

## Why SQLite for Development?

SQLite is a zero-configuration, serverless database perfect for local development, testing, and embedded applications. Here's why:

**Advantages:**
- **Zero configuration**: No server setup or administration required
- **Single file**: Entire database in one portable file
- **In-memory mode**: Lightning-fast ephemeral databases for testing
- **Embedded**: Ships with Python, no external dependencies
- **ACID compliance**: Full transactional guarantees
- **Fast for single-process**: Excellent performance for local development
- **Easy cleanup**: Delete file to reset database

**When to use:**
- Local development and testing
- CI/CD pipelines (fast, isolated test databases)
- Desktop and CLI applications
- Edge computing and IoT devices
- Single-instance deployments with modest traffic
- Prototyping and MVPs

**When NOT to use:**
- Multi-process production deployments
- High-concurrency write workloads
- Network-accessible event stores
- Distributed systems requiring horizontal scaling

---

## Installation

Install eventsource-py with SQLite support:

```bash
pip install eventsource-py[sqlite]
```

This installs the `aiosqlite` package for async SQLite operations.

**Verify installation:**

```python
from eventsource import SQLITE_AVAILABLE

if SQLITE_AVAILABLE:
    print("SQLite backend is ready!")
else:
    print("Please install: pip install eventsource-py[sqlite]")
```

---

## In-Memory Databases

In-memory databases exist only in RAM and are automatically destroyed when the connection closes. Perfect for tests!

### Basic In-Memory Usage

```python
import asyncio
from uuid import uuid4

from eventsource import (
    SQLiteEventStore,
    AggregateRepository,
    DomainEvent,
    register_event,
    AggregateRoot,
)
from pydantic import BaseModel


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: str
    total: float


class OrderState(BaseModel):
    order_id: str
    customer_id: str = ""
    total: float = 0.0
    status: str = "draft"


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=str(self.aggregate_id),
                customer_id=event.customer_id,
                total=event.total,
                status="placed",
            )

    def place(self, customer_id: str, total: float) -> None:
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            total=total,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


async def in_memory_example():
    """Demonstrate in-memory SQLite usage."""
    # Create in-memory store - exists only while connection is open
    store = SQLiteEventStore(":memory:")

    # Use async context manager for proper lifecycle
    async with store:
        # Initialize creates the schema
        await store.initialize()

        # Create repository
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create and save an order
        order_id = uuid4()
        order = repo.create_new(order_id)
        order.place(customer_id="customer-123", total=99.99)
        await repo.save(order)

        print(f"Order created: {order_id}")
        print(f"Version: {order.version}")

        # Load it back
        loaded = await repo.load(order_id)
        print(f"Loaded: {loaded.state.customer_id} - ${loaded.state.total}")

    # After exiting context, database is gone
    print("Database automatically cleaned up")


asyncio.run(in_memory_example())
```

**Key points:**
- Database path is `":memory:"` for in-memory mode
- Always use `async with` for automatic connection management
- Schema is created with `await store.initialize()`
- Database disappears when context manager exits
- Perfect isolation between tests

### Why In-Memory for Tests?

```python
# Each test gets a fresh, isolated database
async def test_order_creation():
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        # Test here - completely isolated from other tests
        # No cleanup needed - database disappears automatically
```

Benefits:
- **Fast**: No disk I/O overhead
- **Isolated**: Each test has its own database
- **Clean**: No leftover data between tests
- **Simple**: No cleanup or teardown needed

---

## File-Based Databases

File-based databases persist to disk, surviving process restarts. Use for development and embedded applications.

### Basic File-Based Usage

```python
import asyncio
import os
from uuid import uuid4
from eventsource import SQLiteEventStore


async def file_based_example():
    """Demonstrate file-based SQLite usage."""
    db_path = "./my_events.db"

    # First session - create and save
    async with SQLiteEventStore(db_path) as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        order_id = uuid4()
        order = repo.create_new(order_id)
        order.place("customer-456", 149.99)
        await repo.save(order)

        print(f"Created order: {order_id}")
        print(f"Database saved to: {db_path}")

    # Database persists after context manager exits
    print(f"File exists: {os.path.exists(db_path)}")  # True

    # Second session - load persisted data
    async with SQLiteEventStore(db_path) as store:
        await store.initialize()  # Safe to call again (idempotent)

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Load the previously saved order
        loaded = await repo.load(order_id)
        print(f"Loaded from disk: ${loaded.state.total}")


asyncio.run(file_based_example())
```

**File locations:**
- **Development**: `./data/events.db` (version controlled `.gitignore`)
- **Production**: `/var/lib/myapp/events.db` (persistent storage)
- **User data**: `~/.config/myapp/events.db` (user-specific)

---

## Database Schema

### Schema Creation

The `initialize()` method creates all required tables:

```python
async with SQLiteEventStore("events.db") as store:
    await store.initialize()  # Creates tables if they don't exist
```

This creates five tables:
1. **events** - Core event store
2. **event_outbox** - Transactional outbox pattern
3. **projection_checkpoints** - Projection position tracking
4. **dead_letter_queue** - Failed event handling
5. **snapshots** - Aggregate state snapshots

### Events Table Structure

```sql
CREATE TABLE IF NOT EXISTS events (
    -- Global position for ordered replay
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Unique event identifier
    event_id TEXT NOT NULL UNIQUE,

    -- Aggregate identification
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,

    -- Event metadata
    event_type TEXT NOT NULL,

    -- Multi-tenancy support (optional)
    tenant_id TEXT,

    -- Actor who triggered the event
    actor_id TEXT,

    -- Optimistic concurrency control
    version INTEGER NOT NULL,

    -- Event timestamp (ISO 8601 string)
    timestamp TEXT NOT NULL,

    -- Event payload (JSON as TEXT)
    payload TEXT NOT NULL,

    -- Technical metadata
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Prevent concurrent modifications
    UNIQUE (aggregate_id, aggregate_type, version)
);
```

### SQLite vs PostgreSQL Schema Differences

| Aspect | PostgreSQL | SQLite |
|--------|-----------|--------|
| **UUID** | Native UUID type | TEXT (36 chars) |
| **Timestamps** | TIMESTAMPTZ | TEXT (ISO 8601) |
| **JSON** | JSONB (indexed) | TEXT (string) |
| **Auto-increment** | BIGSERIAL | INTEGER PRIMARY KEY AUTOINCREMENT |
| **Constraints** | Rich CHECK constraints | Basic constraints |

**Important:** UUIDs are stored as hyphenated strings (`"550e8400-e29b-41d4-a716-446655440000"`) in SQLite, not binary.

### Essential Indexes

```sql
-- Load aggregate event streams
CREATE INDEX idx_events_aggregate_id ON events (aggregate_id);

-- Query events by aggregate type
CREATE INDEX idx_events_aggregate_type ON events (aggregate_type);

-- Query events by event type
CREATE INDEX idx_events_event_type ON events (event_type);

-- Time-based queries
CREATE INDEX idx_events_timestamp ON events (timestamp);

-- Multi-tenant queries (partial index)
CREATE INDEX idx_events_tenant_id ON events (tenant_id) WHERE tenant_id IS NOT NULL;

-- Composite index for projections
CREATE INDEX idx_events_type_tenant_timestamp ON events (aggregate_type, tenant_id, timestamp);

-- Aggregate stream loading
CREATE INDEX idx_events_aggregate_version ON events (aggregate_id, aggregate_type, version);
```

These indexes optimize common event sourcing query patterns.

---

## Configuring SQLiteEventStore

### Constructor Parameters

```python
from eventsource import SQLiteEventStore

store = SQLiteEventStore(
    database="./events.db",           # Required: path or ":memory:"
    event_registry=None,              # Optional: custom event registry
    wal_mode=True,                    # Optional: enable WAL mode (default True)
    busy_timeout=5000,                # Optional: lock timeout in ms (default 5000)
    enable_tracing=True,              # Optional: OpenTelemetry tracing (default True)
    uuid_fields=None,                 # Optional: additional UUID fields
    string_id_fields=None,            # Optional: exclude from UUID detection
    auto_detect_uuid=True,            # Optional: auto-detect UUID fields (default True)
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | str | Required | Path to file or `":memory:"` |
| `event_registry` | EventRegistry | default_registry | Event type lookup |
| `wal_mode` | bool | True | Enable Write-Ahead Logging |
| `busy_timeout` | int | 5000 | Milliseconds to wait for locked database |
| `enable_tracing` | bool | True | OpenTelemetry tracing support |
| `uuid_fields` | set[str] | None | Additional UUID field names |
| `string_id_fields` | set[str] | None | Fields to exclude from UUID detection |
| `auto_detect_uuid` | bool | True | Auto-detect fields ending in '_id' as UUIDs |

### WAL Mode (Write-Ahead Logging)

WAL mode improves concurrency by allowing readers while a write is in progress.

```python
# Enable WAL mode (recommended for file-based databases)
store = SQLiteEventStore("./events.db", wal_mode=True)
```

**WAL mode benefits:**
- **Better concurrency**: Readers don't block writers
- **Faster writes**: No need to lock entire database
- **Crash recovery**: More robust against corruption

**WAL mode creates additional files:**
- `events.db` - Main database
- `events.db-wal` - Write-ahead log
- `events.db-shm` - Shared memory for WAL

**Important:** WAL mode is NOT supported for `:memory:` databases:

```python
# Correct - WAL mode disabled for in-memory
store = SQLiteEventStore(":memory:", wal_mode=False)

# Incorrect - WAL mode not supported for in-memory
# store = SQLiteEventStore(":memory:", wal_mode=True)  # Will be ignored
```

### Busy Timeout

Controls how long to wait when database is locked:

```python
# Wait up to 10 seconds for database lock
store = SQLiteEventStore("./events.db", busy_timeout=10000)

# Short timeout for fast-fail behavior
store = SQLiteEventStore("./events.db", busy_timeout=1000)
```

**When to increase:**
- High contention scenarios
- Background workers and CLI tools running simultaneously
- Long-running read queries

**When to decrease:**
- Fast-fail requirements
- Testing scenarios

### UUID Field Detection

Configure which fields are treated as UUIDs:

```python
# Default: auto-detect fields ending in '_id'
store = SQLiteEventStore("./events.db")
# Automatically treats: event_id, aggregate_id, tenant_id, user_id, etc.

# Add custom UUID fields
store = SQLiteEventStore(
    "./events.db",
    uuid_fields={"parent_id", "reference_id"},
)

# Exclude string IDs from auto-detection
store = SQLiteEventStore(
    "./events.db",
    string_id_fields={"stripe_customer_id", "external_api_id"},
)

# Strict mode: only explicit UUID fields
store = SQLiteEventStore.with_strict_uuid_detection(
    database="./events.db",
    uuid_fields={"event_id", "aggregate_id", "tenant_id"},
)
```

---

## Using with Repositories

Repository usage is identical across all event store backends:

```python
from uuid import uuid4
from eventsource import SQLiteEventStore, AggregateRepository

async def main():
    # Create SQLite event store
    async with SQLiteEventStore("./events.db") as store:
        await store.initialize()

        # Create repository
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create aggregate
        order_id = uuid4()
        order = repo.create_new(order_id)
        order.place(customer_id="cust-789", total=249.99)
        await repo.save(order)

        # Load aggregate
        loaded = await repo.load(order_id)
        print(f"Order status: {loaded.state.status}")
```

**Key insight:** Your application code doesn't need to change when switching between SQLite and PostgreSQL!

---

## SQLite Checkpoint Repository

Track projection progress with `SQLiteCheckpointRepository`:

```python
import aiosqlite
from eventsource.repositories.checkpoint import SQLiteCheckpointRepository

async def checkpoint_example():
    """Demonstrate SQLite checkpoint repository."""
    async with aiosqlite.connect("events.db") as db:
        checkpoint_repo = SQLiteCheckpointRepository(db)

        # Save checkpoint
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=1000,
            event_id=event.event_id,
            event_type=event.event_type,
        )

        # Resume from checkpoint
        last_position = await checkpoint_repo.get_position("OrderProjection")
        print(f"Resume from position: {last_position + 1}")

        # Get all checkpoints
        checkpoints = await checkpoint_repo.get_all_checkpoints()
        for ckpt in checkpoints:
            print(f"{ckpt.projection_name}: position {ckpt.global_position}")
```

**Features:**
- Automatic UPSERT (safe to call multiple times)
- Tracks global position and event ID
- Monitors projection lag
- Supports projection resets

See [Tutorial 10: Checkpoints](10-checkpoints.md) for detailed usage.

---

## Pytest Fixtures for Testing

Create reusable pytest fixtures for SQLite-based tests:

```python
# conftest.py
import pytest
import pytest_asyncio
from eventsource import (
    SQLiteEventStore,
    EventRegistry,
    AggregateRepository,
    InMemoryCheckpointRepository,
)
from my_app.aggregates import OrderAggregate


@pytest_asyncio.fixture
async def event_store():
    """Provide fresh in-memory event store for each test."""
    store = SQLiteEventStore(
        database=":memory:",
        wal_mode=False,  # WAL not supported for in-memory
    )
    async with store:
        await store.initialize()
        yield store


@pytest_asyncio.fixture
async def order_repository(event_store):
    """Provide order repository using the event store fixture."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )


@pytest_asyncio.fixture
async def checkpoint_repo(event_store):
    """Provide checkpoint repository for projection tests."""
    # Note: For SQLite checkpoint repo, you'd need the aiosqlite connection
    # For simplicity in tests, use InMemoryCheckpointRepository
    return InMemoryCheckpointRepository()
```

### Using the Fixtures

```python
# test_orders.py
import pytest
from uuid import uuid4


class TestOrderAggregate:
    @pytest.mark.asyncio
    async def test_create_order(self, order_repository):
        """Test order creation with isolated database."""
        order_id = uuid4()
        order = order_repository.create_new(order_id)
        order.place("customer-123", 99.99)
        await order_repository.save(order)

        assert order.version == 1
        assert order.state.total == 99.99

    @pytest.mark.asyncio
    async def test_load_order(self, order_repository):
        """Test order loading with isolated database."""
        # Create
        order_id = uuid4()
        order = order_repository.create_new(order_id)
        order.place("customer-456", 149.99)
        await order_repository.save(order)

        # Load
        loaded = await order_repository.load(order_id)
        assert loaded.state.customer_id == "customer-456"
        assert loaded.state.total == 149.99

    @pytest.mark.asyncio
    async def test_isolation(self, event_store):
        """Verify each test gets a fresh database."""
        # Check that event store is empty
        count = 0
        async for _ in event_store.read_all():
            count += 1
        assert count == 0  # Fresh database per test
```

**Benefits:**
- Each test gets a fresh, isolated database
- No cleanup needed (in-memory databases auto-cleanup)
- Fast test execution (no disk I/O)
- Deterministic test behavior

---

## File Cleanup for File-Based Databases

When using file-based databases in tests or development, clean up WAL files:

```python
import os


def cleanup_sqlite_db(db_path: str) -> None:
    """
    Remove SQLite database and WAL files.

    Args:
        db_path: Path to the database file
    """
    for suffix in ["", "-wal", "-shm"]:
        path = f"{db_path}{suffix}"
        if os.path.exists(path):
            os.remove(path)
            print(f"Removed: {path}")


# Usage
cleanup_sqlite_db("./test_events.db")
```

**When to use:**
- Cleaning up after integration tests
- Resetting development environment
- CI/CD cleanup steps

---

## Limitations and Considerations

### SQLite Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| **Single writer** | Only one write at a time | Use PostgreSQL for high concurrency |
| **No network access** | Local file access only | Use PostgreSQL for distributed systems |
| **Lock contention** | Busy errors under high load | Increase `busy_timeout` or use PostgreSQL |
| **No true parallelism** | GIL limits multi-threading | Use multi-process with PostgreSQL |
| **File size limits** | Practical limit ~281 TB (enough for most cases) | Partition or archive old events |

### When to Migrate to PostgreSQL

**Signs you've outgrown SQLite:**
- Multiple processes need to write events
- Deploying to multiple servers
- Lock contention errors appearing frequently
- Need for advanced PostgreSQL features (JSONB queries, replication)
- Scaling beyond single instance

**Migration strategy:**
1. Develop and test with SQLite
2. Monitor event volume and concurrency
3. When reaching limits, migrate to PostgreSQL
4. Use the migration script (see below)

### SQLite is Perfect For:

- **Local development**: Fast, zero-config, easy to reset
- **CI/CD testing**: Isolated, ephemeral databases
- **Desktop applications**: Single-user, embedded database
- **CLI tools**: Simple, portable data storage
- **Edge devices**: IoT and embedded systems
- **Prototypes**: Quick iteration without infrastructure

---

## Migration to PostgreSQL

When scaling up, migrate data from SQLite to PostgreSQL:

```python
import asyncio
from uuid import UUID
from eventsource import SQLiteEventStore, PostgreSQLEventStore, ExpectedVersion
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


async def migrate_sqlite_to_postgresql(
    sqlite_path: str,
    pg_connection_string: str,
):
    """
    Migrate all events from SQLite to PostgreSQL.

    Args:
        sqlite_path: Path to SQLite database file
        pg_connection_string: PostgreSQL connection string
    """
    print(f"Starting migration from {sqlite_path} to PostgreSQL...")

    # Open SQLite source
    async with SQLiteEventStore(sqlite_path) as sqlite_store:
        await sqlite_store.initialize()

        # Open PostgreSQL destination
        engine = create_async_engine(pg_connection_string)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        pg_store = PostgreSQLEventStore(session_factory)

        # Migrate events
        count = 0
        batch_size = 1000

        async for stored_event in sqlite_store.read_all():
            event = stored_event.event

            # Append to PostgreSQL (skip version check for migration)
            await pg_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type=event.aggregate_type,
                events=[event],
                expected_version=ExpectedVersion.ANY,
            )

            count += 1
            if count % batch_size == 0:
                print(f"Migrated {count} events...")

        await engine.dispose()

    print(f"Migration complete: {count} events transferred")


# Usage
asyncio.run(migrate_sqlite_to_postgresql(
    sqlite_path="./events.db",
    pg_connection_string="postgresql+asyncpg://user:pass@localhost/mydb"
))
```

**Migration checklist:**
1. Stop writes to SQLite database
2. Run migration script
3. Verify event counts match
4. Test application with PostgreSQL
5. Update configuration to use PostgreSQL
6. Keep SQLite backup until confident

---

## Complete Working Example

```python
"""
Tutorial 12: SQLite Event Store

Complete example demonstrating SQLite for development and testing.

Prerequisites:
- pip install eventsource-py[sqlite]

Run with: python tutorial_12_sqlite.py
"""

import asyncio
import os
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    SQLiteEventStore,
    AggregateRepository,
)


# =============================================================================
# Events
# =============================================================================

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: str
    total: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


# =============================================================================
# Aggregate
# =============================================================================

class OrderState(BaseModel):
    order_id: UUID
    customer_id: str = ""
    total: float = 0.0
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
                total=event.total,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                })

    def place(self, customer_id: str, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            total=total,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Demo Functions
# =============================================================================

async def in_memory_demo():
    """Demonstrate in-memory SQLite for testing."""
    print("=" * 70)
    print("In-Memory SQLite Demo")
    print("=" * 70)

    async with SQLiteEventStore(":memory:", wal_mode=False) as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create order
        order_id = uuid4()
        order = repo.create_new(order_id)
        order.place("customer-123", 99.99)
        await repo.save(order)

        print(f"\n1. Created order: {order_id}")
        print(f"   Version: {order.version}")
        print(f"   Customer: {order.state.customer_id}")
        print(f"   Total: ${order.state.total}")

        # Load and modify
        loaded = await repo.load(order_id)
        loaded.ship("TRACK-ABC123")
        await repo.save(loaded)

        print(f"\n2. Shipped order")
        print(f"   Version: {loaded.version}")
        print(f"   Status: {loaded.state.status}")
        print(f"   Tracking: {loaded.state.tracking_number}")

        # Verify
        verified = await repo.load(order_id)
        print(f"\n3. Verified order version: {verified.version}")

    print("\n4. Database cleaned up automatically (in-memory)")
    print()


async def file_based_demo():
    """Demonstrate file-based SQLite for development."""
    print("=" * 70)
    print("File-Based SQLite Demo")
    print("=" * 70)

    db_path = "./tutorial_orders.db"

    # First session - create
    async with SQLiteEventStore(db_path, wal_mode=True) as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        order_id = uuid4()
        order = repo.create_new(order_id)
        order.place("customer-456", 149.99)
        await repo.save(order)

        print(f"\n1. Created order: {order_id}")
        print(f"   Saved to: {db_path}")
        print(f"   File exists: {os.path.exists(db_path)}")

    # Second session - load (simulates restart)
    print("\n2. Reopening database (simulating restart)...")
    async with SQLiteEventStore(db_path, wal_mode=True) as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        loaded = await repo.load(order_id)
        print(f"   Loaded order: {loaded.state.customer_id}")
        print(f"   Total: ${loaded.state.total}")
        print(f"   Version: {loaded.version}")

    # Cleanup
    print("\n3. Cleaning up database files...")
    for suffix in ["", "-wal", "-shm"]:
        path = f"{db_path}{suffix}"
        if os.path.exists(path):
            os.remove(path)
            print(f"   Removed: {path}")
    print()


async def configuration_demo():
    """Demonstrate configuration options."""
    print("=" * 70)
    print("Configuration Options Demo")
    print("=" * 70)

    # Default configuration
    print("\n1. Default configuration:")
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        print(f"   Database: {store.database}")
        print(f"   WAL mode: {store.wal_mode}")
        print(f"   Busy timeout: {store.busy_timeout}ms")
        print(f"   Connected: {store.is_connected}")

    # Custom configuration
    print("\n2. Custom configuration:")
    async with SQLiteEventStore(
        ":memory:",
        wal_mode=False,
        busy_timeout=10000,
        enable_tracing=False,
    ) as store:
        await store.initialize()
        print(f"   WAL mode: {store.wal_mode}")
        print(f"   Busy timeout: {store.busy_timeout}ms")

    # Strict UUID detection
    print("\n3. Strict UUID detection:")
    store = SQLiteEventStore.with_strict_uuid_detection(
        database=":memory:",
        uuid_fields={"event_id", "aggregate_id", "tenant_id"},
    )
    async with store:
        await store.initialize()
        print(f"   Using explicit UUID fields only")

    print()


async def main():
    """Run all demos."""
    await in_memory_demo()
    await file_based_demo()
    await configuration_demo()

    print("=" * 70)
    print("Tutorial Complete!")
    print("=" * 70)
    print("\nKey Takeaways:")
    print("- Use :memory: for tests (fast, isolated)")
    print("- Use file-based for development (persistent)")
    print("- WAL mode improves concurrency (file-based only)")
    print("- Schema created with initialize() (idempotent)")
    print("- Migrate to PostgreSQL when scaling up")


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
======================================================================
In-Memory SQLite Demo
======================================================================

1. Created order: [UUID]
   Version: 1
   Customer: customer-123
   Total: $99.99

2. Shipped order
   Version: 2
   Status: shipped
   Tracking: TRACK-ABC123

3. Verified order version: 2

4. Database cleaned up automatically (in-memory)

======================================================================
File-Based SQLite Demo
======================================================================

1. Created order: [UUID]
   Saved to: ./tutorial_orders.db
   File exists: True

2. Reopening database (simulating restart)...
   Loaded order: customer-456
   Total: $149.99
   Version: 1

3. Cleaning up database files...
   Removed: ./tutorial_orders.db
   Removed: ./tutorial_orders.db-wal
   Removed: ./tutorial_orders.db-shm

======================================================================
Configuration Options Demo
======================================================================

1. Default configuration:
   Database: :memory:
   WAL mode: False
   Busy timeout: 5000ms
   Connected: True

2. Custom configuration:
   WAL mode: False
   Busy timeout: 10000ms

3. Strict UUID detection:
   Using explicit UUID fields only

======================================================================
Tutorial Complete!
======================================================================

Key Takeaways:
- Use :memory: for tests (fast, isolated)
- Use file-based for development (persistent)
- WAL mode improves concurrency (file-based only)
- Schema created with initialize() (idempotent)
- Migrate to PostgreSQL when scaling up
```

---

## Performance Considerations

### SQLite is Fast For:

- **Single process**: Excellent local performance
- **Read-heavy workloads**: Multiple readers don't block
- **Small to medium datasets**: <100GB works great
- **Development**: No network latency

### Optimize SQLite Performance:

```python
# Enable WAL mode for better concurrency
store = SQLiteEventStore("./events.db", wal_mode=True)

# Increase busy timeout to reduce lock errors
store = SQLiteEventStore("./events.db", busy_timeout=10000)

# For read-only operations, open multiple connections
# (each connection can read in parallel)
```

### Connection Pooling

SQLite doesn't benefit from connection pooling like PostgreSQL. Each `SQLiteEventStore` instance manages a single connection.

For multiple processes accessing the same database, each process gets its own connection.

---

## Common Patterns

### Development vs Production Configuration

```python
import os


def create_event_store():
    """Create event store based on environment."""
    env = os.environ.get("APP_ENV", "development")

    if env == "development":
        # File-based SQLite for development
        return SQLiteEventStore("./data/events.db", wal_mode=True)
    elif env == "test":
        # In-memory SQLite for tests
        return SQLiteEventStore(":memory:", wal_mode=False)
    else:
        # PostgreSQL for production
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        engine = create_async_engine(os.environ["DATABASE_URL"])
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        return PostgreSQLEventStore(session_factory)
```

### Temporary Databases for Tests

```python
import tempfile
import os


async def test_with_temp_database():
    """Use temporary file for test database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        async with SQLiteEventStore(db_path) as store:
            await store.initialize()
            # Test here
    finally:
        # Cleanup
        for suffix in ["", "-wal", "-shm"]:
            path = f"{db_path}{suffix}"
            if os.path.exists(path):
                os.remove(path)
```

---

## Key Takeaways

1. **SQLite is perfect for development and testing**: Zero configuration, fast, isolated
2. **Two modes**: In-memory (`:memory:`) for tests, file-based for development
3. **WAL mode improves concurrency**: But only for file-based databases
4. **Schema creation is idempotent**: Safe to call `initialize()` multiple times
5. **Identical API to PostgreSQL**: Easy migration when scaling up
6. **Limitations to consider**: Single writer, no network access, no horizontal scaling
7. **Use async context manager**: `async with store:` for proper lifecycle
8. **Clean up WAL files**: Remove `-wal` and `-shm` files when deleting database
9. **Pytest fixtures**: Create isolated in-memory databases for each test
10. **Migrate when needed**: Use migration script to move to PostgreSQL

---

## Next Steps

Now that you understand SQLite for development and testing, continue to:

**Tutorial 13: Subscription Management** to learn about:
- Coordinated event subscriptions
- Catch-up processing for projections
- Real-time event delivery
- Managing multiple subscribers

For production deployments, revisit:
- [Tutorial 11: PostgreSQL - Production Event Store](11-postgresql.md)
- [Tutorial 10: Checkpoints](10-checkpoints.md)
- [Tutorial 6: Projections](06-projections.md)
