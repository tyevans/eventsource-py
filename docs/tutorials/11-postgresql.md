# Tutorial 11: PostgreSQL - Production Event Store

**Difficulty:** Intermediate

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- Python 3.10 or higher
- PostgreSQL 12 or higher
- Understanding of async/await and SQLAlchemy
- `pip install eventsource-py[postgresql]`

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Set up PostgreSQL as your production event store backend
2. Create the events table schema with proper indexes
3. Configure PostgreSQLEventStore with connection pooling
4. Use the outbox pattern for reliable event publishing
5. Set up PostgreSQLCheckpointRepository for projections
6. Understand PostgreSQL-specific features like JSONB and partitioning
7. Tune connection pool settings for different workloads
8. Monitor and maintain your PostgreSQL event store

---

## Why PostgreSQL for Production?

PostgreSQL is the recommended event store for production deployments. Here's why:

**Advantages:**
- **ACID guarantees**: Full transactional consistency across events
- **JSONB storage**: Native JSON querying for event payloads
- **Row-level locking**: Optimistic concurrency control via unique constraints
- **Replication**: Built-in streaming replication for high availability
- **Partitioning**: Scale to billions of events with table partitioning
- **Multi-process**: Safe concurrent access from multiple workers
- **Observability**: Rich monitoring via pg_stat tables

**When to use:**
- Production applications requiring durability
- Multi-process or distributed deployments
- Applications needing complex event queries
- Systems requiring audit trails and compliance
- High-throughput event sourcing workloads

---

## PostgreSQL Setup

### Option 1: Docker (Recommended for Development)

```bash
docker run -d --name eventsource-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=eventsource_tutorial \
  -p 5432:5432 \
  postgres:15

# Verify connection
docker exec -it eventsource-postgres psql -U postgres -d eventsource_tutorial
```

### Option 2: Local PostgreSQL

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE eventsource_tutorial;

# Verify
\c eventsource_tutorial
```

### Option 3: Cloud PostgreSQL

For production, use managed PostgreSQL services:

- **AWS RDS for PostgreSQL**: Managed with automated backups
- **Google Cloud SQL**: Fully managed PostgreSQL
- **Azure Database for PostgreSQL**: Enterprise-grade managed service
- **Supabase**: PostgreSQL with built-in auth and APIs

**Connection string format:**

```python
# Local development
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"

# Production with SSL
DATABASE_URL = "postgresql+asyncpg://user:pass@prod-host:5432/mydb?ssl=require"

# Cloud providers
AWS_URL = "postgresql+asyncpg://user:pass@mydb.123.us-east-1.rds.amazonaws.com:5432/mydb?ssl=require"
```

---

## Database Schema

### Core Events Table

The events table is the heart of your event store:

```sql
CREATE TABLE IF NOT EXISTS events (
    -- Global position for ordered replay across all streams
    global_position BIGSERIAL PRIMARY KEY,

    -- Unique event identifier (UUID v4)
    event_id UUID NOT NULL UNIQUE,

    -- Aggregate identification
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,

    -- Event metadata
    event_type VARCHAR(255) NOT NULL,

    -- Multi-tenancy support (optional)
    tenant_id UUID,

    -- Actor/user who triggered the event
    actor_id VARCHAR(255),

    -- Optimistic concurrency control
    -- Version starts at 1 and increments for each event
    version INTEGER NOT NULL,

    -- Event timestamp (when it occurred in the domain)
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Event payload stored as JSONB for flexible querying
    payload JSONB NOT NULL,

    -- Technical metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Ensure no two events have the same version for an aggregate
    -- This prevents concurrent modifications
    CONSTRAINT uq_events_aggregate_version
        UNIQUE (aggregate_id, aggregate_type, version)
);
```

### Essential Indexes

These indexes optimize common event sourcing query patterns:

```sql
-- Load aggregate event streams
CREATE INDEX IF NOT EXISTS idx_events_aggregate_id
    ON events (aggregate_id);

-- Query events by aggregate type (for projections)
CREATE INDEX IF NOT EXISTS idx_events_aggregate_type
    ON events (aggregate_type);

-- Query events by event type
CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events (event_type);

-- Time-based queries for replay and auditing
CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events (timestamp);

-- Multi-tenant queries (partial index saves space)
CREATE INDEX IF NOT EXISTS idx_events_tenant_id
    ON events (tenant_id)
    WHERE tenant_id IS NOT NULL;

-- Composite index for projection queries
CREATE INDEX IF NOT EXISTS idx_events_type_tenant_timestamp
    ON events (aggregate_type, tenant_id, timestamp);

-- Aggregate stream loading with version ordering
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, aggregate_type, version);
```

### Running Migrations

**Option 1: Use the complete schema file**

```bash
# Download the complete schema
psql -U postgres -d eventsource_tutorial -f src/eventsource/migrations/schemas/all.sql

# Or from the repository
wget https://raw.githubusercontent.com/tyevans/eventsource-py/main/src/eventsource/migrations/schemas/all.sql
psql -U postgres -d eventsource_tutorial -f all.sql
```

The complete schema includes:
- Events table with indexes
- Event outbox table (for reliable publishing)
- Projection checkpoints table
- Dead letter queue table
- Snapshots table

**Option 2: Create tables programmatically**

```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

async def setup_database(database_url: str):
    """Create tables programmatically."""
    engine = create_async_engine(database_url)

    async with engine.begin() as conn:
        # Create events table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                global_position BIGSERIAL PRIMARY KEY,
                event_id UUID NOT NULL UNIQUE,
                aggregate_id UUID NOT NULL,
                aggregate_type VARCHAR(255) NOT NULL,
                event_type VARCHAR(255) NOT NULL,
                tenant_id UUID,
                actor_id VARCHAR(255),
                version INTEGER NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_events_aggregate_version
                    UNIQUE (aggregate_id, aggregate_type, version)
            )
        """))

        # Create essential indexes
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_events_aggregate_id
            ON events(aggregate_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_events_aggregate_type
            ON events(aggregate_type)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_events_timestamp
            ON events(timestamp)
        """))

    await engine.dispose()

# Usage
await setup_database(DATABASE_URL)
```

**Option 3: Use Alembic for version control**

For production, use Alembic to manage schema changes:

```bash
# Initialize Alembic
alembic init migrations

# Edit alembic.ini with your database URL
# Edit migrations/env.py to configure async SQLAlchemy

# Create initial migration
alembic revision -m "Initial event store schema"

# Apply migrations
alembic upgrade head
```

See `src/eventsource/migrations/templates/alembic/` for Alembic templates.

---

## Configuring PostgreSQLEventStore

### Basic Configuration

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from eventsource import PostgreSQLEventStore

# Create async engine
engine = create_async_engine(
    "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial",
    echo=False,  # Set True to see SQL queries
)

# Create session factory
# IMPORTANT: expire_on_commit=False is REQUIRED for async
session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

# Create event store
event_store = PostgreSQLEventStore(session_factory)

# Remember to dispose engine on shutdown
await engine.dispose()
```

### Connection Pooling

Configure connection pool based on your workload:

```python
from sqlalchemy.ext.asyncio import create_async_engine

# Light workload (2-5 connections)
# Good for: development, single-worker apps, low traffic
engine = create_async_engine(
    database_url,
    pool_size=2,        # Baseline connections
    max_overflow=3,     # Additional connections under load
    pool_timeout=30,    # Wait up to 30s for a connection
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_pre_ping=True, # Test connections before use
)

# Medium workload (5-10 connections)
# Good for: typical web apps, moderate traffic
engine = create_async_engine(
    database_url,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)

# Heavy workload (10-20 connections)
# Good for: high-traffic apps, background workers, multi-tenant
engine = create_async_engine(
    database_url,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)
```

**Connection pool parameters:**

| Parameter | Description | Default | Recommendation |
|-----------|-------------|---------|----------------|
| `pool_size` | Baseline connections kept open | 5 | Start low, monitor, increase if exhausted |
| `max_overflow` | Additional connections under load | 10 | 2x pool_size |
| `pool_timeout` | Seconds to wait for connection | 30 | 30s for web apps, 60s for batch jobs |
| `pool_recycle` | Recycle connections after N seconds | -1 (never) | 1800 (30 min) to handle network issues |
| `pool_pre_ping` | Test connections before use | False | True for production (handles stale connections) |
| `echo` | Log all SQL statements | False | True for debugging only |

**Monitoring connection pool:**

```python
# Check pool status
print(f"Pool size: {engine.pool.size()}")
print(f"Checked out: {engine.pool.checkedout()}")
print(f"Overflow: {engine.pool.overflow()}")
print(f"Checked in: {engine.pool.checkedin()}")

# If checkedout == pool_size + max_overflow, increase pool_size
```

### Constructor Parameters

```python
event_store = PostgreSQLEventStore(
    session_factory,                    # Required: async session factory
    event_registry=custom_registry,     # Optional: custom event registry
    outbox_enabled=True,                # Optional: enable outbox pattern
    enable_tracing=True,                # Optional: OpenTelemetry tracing
    uuid_fields={"parent_id"},          # Optional: additional UUID fields
    string_id_fields={"stripe_id"},     # Optional: exclude from UUID detection
    auto_detect_uuid=True,              # Optional: auto-detect UUID fields
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_factory` | async_sessionmaker | Required | SQLAlchemy session factory |
| `event_registry` | EventRegistry | default_registry | Event type lookup |
| `outbox_enabled` | bool | False | Enable outbox pattern |
| `enable_tracing` | bool | True | OpenTelemetry tracing |
| `uuid_fields` | set[str] | None | Additional UUID field names |
| `string_id_fields` | set[str] | None | Fields to exclude from UUID detection |
| `auto_detect_uuid` | bool | True | Auto-detect fields ending in '_id' as UUIDs |

### UUID Field Detection

PostgreSQL stores UUIDs efficiently as 128-bit values. The event store automatically detects UUID fields:

```python
# Default behavior: auto-detect fields ending in '_id'
event_store = PostgreSQLEventStore(session_factory)
# Treats as UUIDs: event_id, aggregate_id, tenant_id, user_id, etc.

# Add custom UUID fields
event_store = PostgreSQLEventStore(
    session_factory,
    uuid_fields={"parent_id", "reference_id"},
)

# Exclude string IDs from auto-detection
event_store = PostgreSQLEventStore(
    session_factory,
    string_id_fields={"stripe_customer_id", "external_api_id"},
)

# Strict mode: explicit UUID fields only (no auto-detection)
event_store = PostgreSQLEventStore.with_strict_uuid_detection(
    session_factory,
    uuid_fields={"event_id", "aggregate_id", "tenant_id"},
)
```

---

## Using with Repositories

Repository usage is identical across all event store backends:

```python
from uuid import uuid4
from eventsource import AggregateRepository

async def main():
    # Create event store
    event_store = PostgreSQLEventStore(session_factory)

    # Create repository
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

    # Create aggregate
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(customer_id="cust-123", total=299.99)
    await repo.save(order)

    # Load aggregate
    loaded = await repo.load(order_id)
    loaded.ship(tracking_number="TRACK-456")
    await repo.save(loaded)

    # Verify
    final = await repo.load(order_id)
    print(f"Order status: {final.state.status}")
```

The beauty of the event store abstraction: change backends without changing your application code!

---

## The Outbox Pattern

The **outbox pattern** guarantees event publishing by writing events to both the events table and an outbox table in the same transaction.

### Why Use the Outbox Pattern?

**Problem:** Event publishing can fail if the message broker is unavailable:

```python
# Without outbox: events saved but publishing fails
await event_store.append_events(...)  # Saved to database
await event_bus.publish(...)          # Fails! Broker down!
# Events lost in the event bus!
```

**Solution:** The outbox pattern ensures at-least-once delivery:

```python
# With outbox: events and outbox entries saved together
await event_store.append_events(...)  # Saves events + outbox entries
# Background worker publishes from outbox
# If broker is down, events remain in outbox for retry
```

### Enabling the Outbox Pattern

```python
# Create event store with outbox enabled
event_store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=True,
)
```

### Outbox Table Schema

The outbox table tracks events pending publication:

```sql
CREATE TABLE IF NOT EXISTS event_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    tenant_id UUID,
    event_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    CONSTRAINT chk_outbox_status CHECK (status IN ('pending', 'published', 'failed'))
);

-- Indexes for outbox worker
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON event_outbox (created_at)
    WHERE status = 'pending';
```

### Outbox Worker

A background worker polls the outbox and publishes events:

```python
import asyncio
from datetime import datetime, UTC
from sqlalchemy import text

async def outbox_worker(session_factory, event_bus, interval_seconds: int = 5):
    """
    Background worker that publishes events from the outbox.

    Args:
        session_factory: SQLAlchemy session factory
        event_bus: Event bus for publishing
        interval_seconds: Polling interval
    """
    while True:
        try:
            async with session_factory() as session:
                # Get pending events (with row locking)
                result = await session.execute(
                    text("""
                        SELECT id, event_id, event_type, event_data
                        FROM event_outbox
                        WHERE status = 'pending'
                        ORDER BY created_at ASC
                        LIMIT 100
                        FOR UPDATE SKIP LOCKED
                    """)
                )
                pending = result.fetchall()

                for row in pending:
                    outbox_id, event_id, event_type, event_data = row

                    try:
                        # Deserialize event
                        event_class = event_registry.get(event_type)
                        event = event_class.model_validate(event_data)

                        # Publish to event bus
                        await event_bus.publish(event)

                        # Mark as published
                        await session.execute(
                            text("""
                                UPDATE event_outbox
                                SET status = 'published',
                                    published_at = NOW()
                                WHERE id = :id
                            """),
                            {"id": outbox_id}
                        )

                    except Exception as e:
                        # Increment retry count and log error
                        await session.execute(
                            text("""
                                UPDATE event_outbox
                                SET retry_count = retry_count + 1,
                                    last_error = :error,
                                    status = CASE
                                        WHEN retry_count >= 5 THEN 'failed'
                                        ELSE 'pending'
                                    END
                                WHERE id = :id
                            """),
                            {"id": outbox_id, "error": str(e)}
                        )

                await session.commit()

        except Exception as e:
            print(f"Outbox worker error: {e}")

        # Wait before next poll
        await asyncio.sleep(interval_seconds)

# Run worker in background
asyncio.create_task(outbox_worker(session_factory, event_bus))
```

### When to Use the Outbox Pattern

**Use when:**
- Event publishing must be guaranteed (financial transactions, orders)
- Message broker may be temporarily unavailable
- You need exactly-once or at-least-once delivery semantics
- Events trigger critical downstream processes

**Skip when:**
- Best-effort publishing is acceptable
- Development/testing environments
- Message broker is highly available with redundancy
- Events are for non-critical notifications only

---

## PostgreSQL Checkpoint Repository

Use `PostgreSQLCheckpointRepository` to track projection positions:

```python
from eventsource.repositories.checkpoint import PostgreSQLCheckpointRepository

# Create checkpoint repository
checkpoint_repo = PostgreSQLCheckpointRepository(engine)

# Save checkpoint
await checkpoint_repo.save_position(
    subscription_id="OrderProjection",
    position=1000,
    event_id=last_event.event_id,
    event_type=last_event.event_type,
)

# Resume from checkpoint
last_position = await checkpoint_repo.get_position("OrderProjection")
# Start from position 1001
```

**Features:**
- Automatic upsert (idempotent)
- Tracks global position and event ID
- Monitors projection lag
- Supports projection resets

See [Tutorial 10: Checkpoints](10-checkpoints.md) for detailed usage.

---

## Multi-Tenancy Support

PostgreSQL event store has built-in multi-tenancy via the `tenant_id` column:

```python
from uuid import UUID

# Events with tenant_id
@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: str
    total: float
    tenant_id: UUID | None = None  # Multi-tenancy support

# Query events by tenant
from eventsource import ReadOptions

tenant_id = UUID("...")
options = ReadOptions(tenant_id=tenant_id)

async for stored_event in event_store.read_all(options):
    # Only events for this tenant
    print(stored_event.event_type)
```

**Multi-tenancy patterns:**

| Pattern | Description | Use Case |
|---------|-------------|----------|
| **Shared schema** | All tenants share events table, filtered by tenant_id | Cost-effective, easy to manage |
| **Separate schemas** | Each tenant has own schema in same database | Better isolation, moderate cost |
| **Separate databases** | Each tenant has own database | Maximum isolation, highest cost |

For shared schema, the partial index on `tenant_id` keeps queries fast.

---

## Performance Tuning

### Connection Pool Tuning

Monitor connection pool usage:

```python
import asyncio

async def monitor_pool(engine):
    """Monitor connection pool every 30 seconds."""
    while True:
        pool = engine.pool
        print(f"Pool stats:")
        print(f"  Size: {pool.size()}")
        print(f"  Checked out: {pool.checkedout()}")
        print(f"  Overflow: {pool.overflow()}")
        print(f"  Checked in: {pool.checkedin()}")

        # Alert if pool is exhausted
        if pool.checkedout() >= pool.size() + pool._max_overflow:
            print("WARNING: Connection pool exhausted!")

        await asyncio.sleep(30)

# Run in background
asyncio.create_task(monitor_pool(engine))
```

**Tuning guidelines:**
- Start with small pool (2-5 connections)
- Monitor `checkedout` vs `pool_size + max_overflow`
- If frequently exhausted, increase `pool_size`
- Set `pool_recycle=1800` to handle stale connections
- Always use `pool_pre_ping=True` in production

### Table Partitioning

For high-volume deployments (millions+ events), use table partitioning:

```sql
-- See src/eventsource/migrations/templates/events_partitioned.sql
-- Partitioned by timestamp (monthly partitions)

CREATE TABLE events (
    -- Same columns as before
) PARTITION BY RANGE (timestamp);

-- Create partitions
CREATE TABLE events_2024_01 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE events_2024_02 PARTITION OF events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Benefits:
-- - Faster queries (partition pruning)
-- - Easier archival (drop old partitions)
-- - Better vacuum performance
```

**Partitioning strategies:**

| Strategy | Partition By | Use Case |
|----------|--------------|----------|
| **Time-based** | timestamp (monthly/daily) | Most common, enables archival |
| **Tenant-based** | tenant_id | Multi-tenant isolation |
| **Hybrid** | tenant_id + timestamp | Large multi-tenant systems |

### Index Optimization

Monitor index usage:

```sql
-- Check index usage
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan AS scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE tablename = 'events'
ORDER BY idx_scan DESC;

-- Unused indexes (scans = 0) may be candidates for removal
```

### JSONB Query Optimization

Create indexes on JSONB fields for fast queries:

```sql
-- Index on specific JSONB field
CREATE INDEX idx_events_payload_customer_id
    ON events ((payload->>'customer_id'));

-- GIN index for JSONB containment queries
CREATE INDEX idx_events_payload_gin
    ON events USING GIN (payload);

-- Query with JSONB index
SELECT * FROM events
WHERE payload->>'customer_id' = 'cust-123';
```

---

## Complete Working Example

```python
"""
Tutorial 11: PostgreSQL Event Store

Complete example demonstrating PostgreSQL as production event store.

Prerequisites:
- PostgreSQL running on localhost:5432
- Database 'eventsource_tutorial' created
- pip install eventsource-py[postgresql]

Run with: python tutorial_11_postgresql.py
"""

import asyncio
from uuid import uuid4

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    PostgreSQLEventStore,
    AggregateRepository,
)


# =============================================================================
# Events
# =============================================================================

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
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
    order_id: str
    customer_id: str | None = None
    total: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=str(self.aggregate_id),
                customer_id=event.customer_id,
                total=event.total,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                })

    def create(self, customer_id: str, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            total=total,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# PostgreSQL Configuration
# =============================================================================

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"


async def setup_database(engine):
    """Create the events table and indexes."""
    async with engine.begin() as conn:
        # Create events table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                global_position BIGSERIAL PRIMARY KEY,
                event_id UUID NOT NULL UNIQUE,
                aggregate_id UUID NOT NULL,
                aggregate_type VARCHAR(255) NOT NULL,
                event_type VARCHAR(255) NOT NULL,
                tenant_id UUID,
                actor_id VARCHAR(255),
                version INTEGER NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_events_aggregate_version
                    UNIQUE (aggregate_id, aggregate_type, version)
            )
        """))

        # Create indexes
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_events_aggregate_id
            ON events(aggregate_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_events_aggregate_type
            ON events(aggregate_type)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_events_timestamp
            ON events(timestamp)
        """))

    print("Database schema created successfully!")


def create_event_store(database_url: str):
    """Create PostgreSQL event store with connection pooling."""
    # Create async engine with connection pool
    engine = create_async_engine(
        database_url,
        pool_size=5,              # Baseline connections
        max_overflow=10,          # Additional connections under load
        pool_timeout=30,          # Wait 30s for connection
        pool_recycle=1800,        # Recycle connections after 30 min
        pool_pre_ping=True,       # Test connections before use
        echo=False,               # Set True to see SQL queries
    )

    # Create session factory
    # IMPORTANT: expire_on_commit=False is REQUIRED for async
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    # Create event store
    event_store = PostgreSQLEventStore(
        session_factory,
        outbox_enabled=False,     # Set True to enable outbox pattern
        enable_tracing=True,      # OpenTelemetry tracing
    )

    return event_store, engine


# =============================================================================
# Main Demo
# =============================================================================

async def main():
    print("=" * 70)
    print("Tutorial 11: PostgreSQL Event Store")
    print("=" * 70)

    # Create event store and engine
    print("\n1. Creating PostgreSQL event store...")
    event_store, engine = create_event_store(DATABASE_URL)

    try:
        # Setup database schema
        print("2. Setting up database schema...")
        await setup_database(engine)

        # Create repository
        print("3. Creating aggregate repository...")
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create an order
        print("\n4. Creating an order...")
        order_id = uuid4()

        order = repo.create_new(order_id)
        order.create(customer_id="customer-456", total=299.99)
        await repo.save(order)

        print(f"   Order created: {order_id}")
        print(f"   Version: {order.version}")
        print(f"   Status: {order.state.status}")
        print(f"   Customer: {order.state.customer_id}")
        print(f"   Total: ${order.state.total}")

        # Load and modify
        print("\n5. Loading and shipping order...")
        loaded = await repo.load(order_id)
        loaded.ship(tracking_number="TRACK-123456")
        await repo.save(loaded)

        print(f"   Order shipped!")
        print(f"   Version: {loaded.version}")
        print(f"   Status: {loaded.state.status}")
        print(f"   Tracking: {loaded.state.tracking_number}")

        # Verify persistence
        print("\n6. Verifying persistence...")
        verified = await repo.load(order_id)
        print(f"   Loaded order version: {verified.version}")
        print(f"   Loaded status: {verified.state.status}")

        # Query events from store
        print("\n7. Querying events from PostgreSQL...")
        stream = await event_store.get_events(order_id, "Order")
        print(f"   Found {len(stream.events)} events:")
        for event in stream.events:
            print(f"     - v{event.aggregate_version}: {event.event_type}")

        # Check connection pool stats
        print("\n8. Connection pool statistics...")
        pool = engine.pool
        print(f"   Pool size: {pool.size()}")
        print(f"   Checked out: {pool.checkedout()}")
        print(f"   Overflow: {pool.overflow()}")

        print("\n" + "=" * 70)
        print("SUCCESS: PostgreSQL event store is working!")
        print("=" * 70)

    finally:
        # Always dispose engine to close connections
        await engine.dispose()
        print("\nConnection pool disposed.")


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
======================================================================
Tutorial 11: PostgreSQL Event Store
======================================================================

1. Creating PostgreSQL event store...
2. Setting up database schema...
Database schema created successfully!
3. Creating aggregate repository...

4. Creating an order...
   Order created: [UUID]
   Version: 1
   Status: created
   Customer: customer-456
   Total: $299.99

5. Loading and shipping order...
   Order shipped!
   Version: 2
   Status: shipped
   Tracking: TRACK-123456

6. Verifying persistence...
   Loaded order version: 2
   Loaded status: shipped

7. Querying events from PostgreSQL...
   Found 2 events:
     - v1: OrderCreated
     - v2: OrderShipped

8. Connection pool statistics...
   Pool size: 5
   Checked out: 0
   Overflow: 0

======================================================================
SUCCESS: PostgreSQL event store is working!
======================================================================

Connection pool disposed.
```

---

## Monitoring and Maintenance

### Monitoring Event Store Health

```sql
-- Total events in store
SELECT COUNT(*) AS total_events FROM events;

-- Events by aggregate type
SELECT aggregate_type, COUNT(*) AS count
FROM events
GROUP BY aggregate_type
ORDER BY count DESC;

-- Events per day (last 7 days)
SELECT DATE(timestamp) AS date, COUNT(*) AS events
FROM events
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- Largest aggregates (most events)
SELECT aggregate_id, aggregate_type, COUNT(*) AS event_count
FROM events
GROUP BY aggregate_id, aggregate_type
ORDER BY event_count DESC
LIMIT 10;

-- Table size
SELECT
    pg_size_pretty(pg_total_relation_size('events')) AS total_size,
    pg_size_pretty(pg_relation_size('events')) AS table_size,
    pg_size_pretty(pg_total_relation_size('events') - pg_relation_size('events')) AS index_size;
```

### Maintenance Tasks

```sql
-- Vacuum events table (reclaim space)
VACUUM ANALYZE events;

-- Reindex if queries are slow
REINDEX TABLE events;

-- Update table statistics
ANALYZE events;
```

### Archiving Old Events

```sql
-- Archive events older than 1 year to archive table
CREATE TABLE events_archive (LIKE events INCLUDING ALL);

INSERT INTO events_archive
SELECT * FROM events
WHERE timestamp < NOW() - INTERVAL '1 year';

DELETE FROM events
WHERE timestamp < NOW() - INTERVAL '1 year';

VACUUM ANALYZE events;
```

---

## Common Patterns

### Transaction Management

```python
# Manual transaction control
async with session_factory() as session:
    async with session.begin():
        # Append events
        result = await event_store.append_events(...)

        # Do other database work in same transaction
        await session.execute(...)

        # Commit happens automatically on context exit
```

### Querying JSONB Payloads

```python
from sqlalchemy import text

# Query events by JSONB field
async with session_factory() as session:
    result = await session.execute(
        text("""
            SELECT event_id, event_type, payload
            FROM events
            WHERE payload->>'customer_id' = :customer_id
        """),
        {"customer_id": "cust-123"}
    )
    rows = result.fetchall()
```

### Handling Connection Errors

```python
from sqlalchemy.exc import DBAPIError
import asyncio

async def save_with_retry(repo, aggregate, max_retries: int = 3):
    """Save with automatic retry on connection errors."""
    for attempt in range(max_retries):
        try:
            await repo.save(aggregate)
            return
        except DBAPIError as e:
            if attempt == max_retries - 1:
                raise
            print(f"Connection error, retrying... ({attempt + 1}/{max_retries})")
            await asyncio.sleep(1)
```

---

## Key Takeaways

1. **PostgreSQL is production-ready**: Full ACID guarantees, multi-process support, high performance
2. **Connection pooling is essential**: Configure pool_size and max_overflow based on workload
3. **JSONB enables rich queries**: Store structured event data, query with JSON operators
4. **Indexes optimize performance**: All essential indexes included in schema
5. **Outbox pattern guarantees publishing**: At-least-once delivery even if broker is down
6. **Partitioning scales to billions**: Use table partitioning for high-volume deployments
7. **expire_on_commit=False is required**: Critical for async SQLAlchemy sessions
8. **Monitor connection pool**: Watch for exhaustion and increase pool_size if needed
9. **Use checkpoints for projections**: PostgreSQLCheckpointRepository tracks positions
10. **Dispose engine on shutdown**: Always call `await engine.dispose()` to close connections

---

## Next Steps

Now that you understand PostgreSQL for production, explore alternative backends:

Continue to [Tutorial 12: SQLite Event Store](12-sqlite.md) to learn about:
- Setting up SQLite for development and embedded applications
- WAL mode for better concurrency
- When to use SQLite vs PostgreSQL
- Migration strategies between backends

For production deployments, see:
- Tutorial 13: Event Bus Integration (Redis, RabbitMQ, Kafka)
- Tutorial 14: Snapshots for Performance Optimization
- Tutorial 15: Multi-Tenancy Patterns
