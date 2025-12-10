# Tutorial 11: Using PostgreSQL Event Store

**Estimated Time:** 45-60 minutes
**Difficulty:** Intermediate
**Progress:** Tutorial 11 of 21 | Phase 3: Production Readiness

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 10: Checkpoint Management](10-checkpoints.md) (Phase 2 complete)
- Understanding of event stores from [Tutorial 4: Event Stores](04-event-stores.md)
- Understanding of repositories from [Tutorial 5: Repositories](05-repositories.md)
- PostgreSQL installed and running (see setup notes below)
- Python 3.11+ installed
- eventsource-py with PostgreSQL support installed:

```bash
pip install eventsource-py[postgresql]
```

This tutorial introduces `PostgreSQLEventStore`, the recommended event store backend for production deployments.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Configure PostgreSQL as your event store backend
2. Set up proper connection pooling for async operations
3. Use the outbox pattern for reliable event publishing
4. Understand the database schema and migration process
5. Configure UUID field handling for your domain

---

## Why PostgreSQL for Event Sourcing?

PostgreSQL is the recommended backend for production event sourcing because it provides:

### ACID Guarantees

Event sourcing relies on atomic writes with optimistic locking. PostgreSQL's transaction support ensures:

- **Atomicity**: All events in a batch are written together or not at all
- **Consistency**: Version constraints prevent concurrent modifications
- **Isolation**: Concurrent writers don't see partial results
- **Durability**: Committed events survive crashes

### Production-Ready Features

PostgreSQL offers features critical for production deployments:

| Feature | Benefit |
|---------|---------|
| **JSONB storage** | Efficient, queryable event payloads |
| **Partial indexes** | Fast queries for specific event patterns |
| **Row-level locking** | High concurrency without conflicts |
| **Replication** | Read replicas for projections |
| **Point-in-time recovery** | Disaster recovery for your event log |
| **Partitioning** | Scale to billions of events |

### When to Use PostgreSQL

Use `PostgreSQLEventStore` when you need:

- **Production deployments** with durability requirements
- **Multi-process/multi-node** applications
- **Auditable event history** that must survive restarts
- **Complex queries** on event data
- **Integration** with existing PostgreSQL infrastructure

For development and testing, consider `InMemoryEventStore` (fast, no setup) or `SQLiteEventStore` (persistent, lightweight).

---

## PostgreSQL Setup

### Option 1: Docker (Recommended for Development)

```bash
# Start PostgreSQL with Docker
docker run -d \
  --name eventsource-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=eventsource_tutorial \
  -p 5432:5432 \
  postgres:15

# Verify it's running
docker logs eventsource-postgres
```

### Option 2: Local Installation

If you have PostgreSQL installed locally:

```sql
-- Connect as superuser and create the database
CREATE DATABASE eventsource_tutorial;
```

### Connection String Format

```
postgresql+asyncpg://user:password@host:port/database
```

Examples:

```python
# Local development
LOCAL_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"

# Docker Compose (service name as host)
DOCKER_URL = "postgresql+asyncpg://postgres:postgres@postgres:5432/eventsource_tutorial"

# Production (with SSL)
PROD_URL = "postgresql+asyncpg://user:pass@prod-host:5432/mydb?ssl=require"
```

---

## Database Schema

Before using `PostgreSQLEventStore`, you need to create the required tables. The library provides SQL templates you can use.

### Core Events Table

The events table stores all domain events:

```sql
CREATE TABLE IF NOT EXISTS events (
    -- Primary event identifier (UUID v4 recommended)
    event_id UUID PRIMARY KEY,

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
    version INTEGER NOT NULL,

    -- Event timestamp (when the event occurred)
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Event payload stored as JSONB
    payload JSONB NOT NULL,

    -- Technical metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Ensure no two events have the same version for an aggregate
    CONSTRAINT uq_events_aggregate_version
        UNIQUE (aggregate_id, aggregate_type, version)
);

-- Essential indexes
CREATE INDEX IF NOT EXISTS idx_events_aggregate_id ON events (aggregate_id);
CREATE INDEX IF NOT EXISTS idx_events_aggregate_type ON events (aggregate_type);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, aggregate_type, version);
```

### Running Migrations

You have several options for setting up the schema:

**Option 1: Use the provided SQL file**

```bash
# From the eventsource-py source
psql -h localhost -U postgres -d eventsource_tutorial \
  -f src/eventsource/migrations/schemas/all.sql
```

**Option 2: Run SQL directly**

```python
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

async def setup_schema():
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"
    )

    async with engine.begin() as conn:
        # Create events table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                event_id UUID PRIMARY KEY,
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
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_events_aggregate_id ON events (aggregate_id)"
        ))
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_events_aggregate_type ON events (aggregate_type)"
        ))
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type)"
        ))
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp)"
        ))

    await engine.dispose()
    print("Schema created successfully!")

asyncio.run(setup_schema())
```

**Option 3: Use Alembic (Recommended for Production)**

The library includes Alembic migration templates in `src/eventsource/migrations/templates/alembic/`. Copy these to your project's migration directory.

---

## Configuring PostgreSQLEventStore

### Step 1: Create the Async Engine

SQLAlchemy's async engine manages database connections:

```python
from sqlalchemy.ext.asyncio import create_async_engine

# Basic engine
engine = create_async_engine(
    "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"
)
```

### Step 2: Configure Connection Pooling

Connection pooling is essential for async performance. Configure it based on your workload:

```python
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine(
    "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial",

    # Connection pool settings
    pool_size=5,           # Connections to keep open
    max_overflow=10,       # Additional connections when pool is full
    pool_timeout=30,       # Seconds to wait for available connection
    pool_recycle=1800,     # Recycle connections after 30 minutes
    pool_pre_ping=True,    # Verify connections before use

    # Performance settings
    echo=False,            # Set True for SQL debugging
)
```

**Pool Size Guidelines:**

| Workload | pool_size | max_overflow |
|----------|-----------|--------------|
| Light (dev/test) | 2-5 | 5 |
| Medium (typical API) | 5-10 | 10 |
| Heavy (high throughput) | 10-20 | 20 |

**Important:** Total connections = `pool_size + max_overflow`. Don't exceed your PostgreSQL `max_connections` limit across all application instances.

### Step 3: Create the Session Factory

The session factory creates database sessions for each operation:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,  # IMPORTANT: Required for async
    class_=AsyncSession,
)
```

**Critical Setting:** Always set `expire_on_commit=False` for async SQLAlchemy. Without this, accessing attributes after commit raises errors.

### Step 4: Initialize PostgreSQLEventStore

```python
from eventsource import PostgreSQLEventStore

event_store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=False,    # Enable for reliable publishing
    enable_tracing=True,     # OpenTelemetry support
)
```

### Complete Setup Example

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from eventsource import PostgreSQLEventStore

def create_event_store(database_url: str) -> tuple[PostgreSQLEventStore, any]:
    """
    Create a configured PostgreSQL event store.

    Returns:
        Tuple of (event_store, engine) - keep engine reference for cleanup
    """
    # Step 1: Create engine with connection pooling
    engine = create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
    )

    # Step 2: Create session factory
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    # Step 3: Create event store
    event_store = PostgreSQLEventStore(session_factory)

    return event_store, engine


# Usage
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"
event_store, engine = create_event_store(DATABASE_URL)

# Don't forget cleanup when shutting down
# await engine.dispose()
```

---

## Using with Repositories

The `PostgreSQLEventStore` works seamlessly with `AggregateRepository`. Your repository code doesn't change - only the event store backend:

```python
from uuid import uuid4
from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    AggregateRepository,
    PostgreSQLEventStore,
)


# Define events (same as before)
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


# Define aggregate (same as before)
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


# Create repository with PostgreSQL backend
async def main():
    event_store, engine = create_event_store(DATABASE_URL)

    try:
        # Repository code is identical to InMemoryEventStore usage!
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create order
        order_id = uuid4()
        order = repo.create_new(order_id)
        order.create(customer_id="cust-123", total=299.99)
        await repo.save(order)

        # Load and update
        loaded = await repo.load(order_id)
        loaded.ship(tracking_number="TRACK-456")
        await repo.save(loaded)

        print(f"Order {order_id} shipped!")
        print(f"Version: {loaded.version}")
        print(f"Status: {loaded.state.status}")

    finally:
        await engine.dispose()
```

---

## PostgreSQLEventStore Constructor Parameters

The `PostgreSQLEventStore` constructor accepts several parameters for customization:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_factory` | `async_sessionmaker` | Required | SQLAlchemy session factory |
| `event_registry` | `EventRegistry` | `None` | Custom registry for deserialization |
| `outbox_enabled` | `bool` | `False` | Enable outbox pattern |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing |
| `uuid_fields` | `set[str]` | `None` | Additional UUID field names |
| `string_id_fields` | `set[str]` | `None` | Fields to NOT treat as UUIDs |
| `auto_detect_uuid` | `bool` | `True` | Auto-detect UUID fields |

### UUID Field Configuration

By default, the event store auto-detects UUID fields by name patterns (fields ending in `_id`). You can customize this:

```python
# Add custom UUID fields
event_store = PostgreSQLEventStore(
    session_factory,
    uuid_fields={"custom_reference_id", "parent_id"},
)

# Exclude string IDs from auto-detection
event_store = PostgreSQLEventStore(
    session_factory,
    string_id_fields={"stripe_customer_id", "external_api_id"},
)

# Disable auto-detection entirely (explicit control)
event_store = PostgreSQLEventStore.with_strict_uuid_detection(
    session_factory,
    uuid_fields={"event_id", "aggregate_id", "tenant_id"},
)
```

---

## The Outbox Pattern

The **outbox pattern** ensures reliable event publishing even when message brokers fail. Events are written to both the events table and an outbox table in the same transaction.

### How It Works

```
1. Application writes event
   |
   v
2. Transaction begins
   |
   +--> Write to events table
   |
   +--> Write to event_outbox table
   |
   v
3. Transaction commits (both or neither)
   |
   v
4. Background worker polls outbox
   |
   v
5. Worker publishes to message broker
   |
   v
6. Worker marks event as published
```

### Enabling the Outbox

```python
# Enable outbox when creating the event store
event_store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=True,
)
```

### Outbox Table Schema

When using the outbox pattern, you need the outbox table:

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

-- Index for polling pending events
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON event_outbox (created_at)
    WHERE status = 'pending';
```

The full outbox schema with helper functions is in `src/eventsource/migrations/templates/outbox.sql`.

### When to Use the Outbox

Use the outbox pattern when:

- Event publishing must be **guaranteed** (eventual consistency)
- Your message broker may be **temporarily unavailable**
- You need **exactly-once** delivery semantics
- Events must be **transactionally consistent** with state changes

Skip the outbox when:

- Event publishing is **best-effort** (can tolerate loss)
- You're in **development/testing**
- Performance is critical and broker is highly available

---

## Multi-Tenancy Support

PostgreSQLEventStore supports multi-tenant deployments through the `tenant_id` field:

```python
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    tenant_id: UUID  # Set this for multi-tenant
    customer_id: str
    total: float


# Query events by tenant
events = await event_store.get_events_by_type(
    aggregate_type="Order",
    tenant_id=tenant_uuid,
)
```

The schema includes a partial index on `tenant_id` for efficient tenant-scoped queries.

---

## Performance Considerations

### Connection Pool Tuning

Monitor these metrics to tune your pool:

- **Pool exhaustion**: Increase `pool_size` if requests wait for connections
- **Connection errors**: Enable `pool_pre_ping` if connections go stale
- **Memory usage**: Decrease pool size if using too much memory

### Query Optimization

The provided indexes optimize common patterns:

| Query Pattern | Supporting Index |
|--------------|------------------|
| Load aggregate events | `idx_events_aggregate_id` |
| Query by aggregate type | `idx_events_aggregate_type` |
| Query by event type | `idx_events_event_type` |
| Time-based queries | `idx_events_timestamp` |
| Version ordering | `idx_events_aggregate_version` |

### Partitioning for Scale

For very high event volumes (millions+ events), consider table partitioning. The library provides a partitioned schema template in `src/eventsource/migrations/templates/events_partitioned.sql`.

---

## Complete Example

Here is a complete, runnable example demonstrating PostgreSQL event store usage:

```python
"""
Tutorial 11: Using PostgreSQL Event Store

This example demonstrates configuring PostgreSQL as your event store backend.
Run with: python tutorial_11_postgresql.py

Prerequisites:
- PostgreSQL running on localhost:5432
- Database 'eventsource_tutorial' created
- pip install eventsource-py[postgresql]
"""
import asyncio
from uuid import UUID, uuid4

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
    """Create the events table if it doesn't exist."""
    async with engine.begin() as conn:
        # Create events table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                event_id UUID PRIMARY KEY,
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

    print("Database schema ready!")


def create_event_store(database_url: str):
    """Create PostgreSQL event store with proper configuration."""
    # Create async engine with connection pooling
    engine = create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        echo=False,
    )

    # Create session factory (expire_on_commit=False is REQUIRED)
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    # Create the event store
    event_store = PostgreSQLEventStore(
        session_factory,
        outbox_enabled=False,
        enable_tracing=True,
    )

    return event_store, engine


# =============================================================================
# Main Demo
# =============================================================================

async def main():
    print("=== PostgreSQL Event Store Tutorial ===\n")

    # Create event store
    print("1. Creating PostgreSQL event store...")
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

        # Load and modify
        print("\n5. Loading and shipping order...")
        loaded = await repo.load(order_id)
        loaded.ship(tracking_number="TRACK-123456")
        await repo.save(loaded)

        print(f"   Order shipped!")
        print(f"   Version: {loaded.version}")
        print(f"   Status: {loaded.state.status}")
        print(f"   Tracking: {loaded.state.tracking_number}")

        # Verify persistence by loading again
        print("\n6. Verifying persistence...")
        verified = await repo.load(order_id)
        print(f"   Loaded order version: {verified.version}")
        print(f"   Loaded status: {verified.state.status}")

        # Query events directly
        print("\n7. Querying events from store...")
        stream = await event_store.get_events(order_id, "Order")
        print(f"   Found {len(stream.events)} events:")
        for event in stream.events:
            print(f"     - {event.event_type}")

        print("\n=== SUCCESS: PostgreSQL event store working! ===")

    finally:
        # Always cleanup
        await engine.dispose()
        print("\nConnection pool disposed.")


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_11_postgresql.py` and run it:

```bash
python tutorial_11_postgresql.py
```

Expected output:

```
=== PostgreSQL Event Store Tutorial ===

1. Creating PostgreSQL event store...
2. Setting up database schema...
Database schema ready!
3. Creating aggregate repository...

4. Creating an order...
   Order created: a1b2c3d4-...
   Version: 1
   Status: created

5. Loading and shipping order...
   Order shipped!
   Version: 2
   Status: shipped
   Tracking: TRACK-123456

6. Verifying persistence...
   Loaded order version: 2
   Loaded status: shipped

7. Querying events from store...
   Found 2 events:
     - OrderCreated
     - OrderShipped

=== SUCCESS: PostgreSQL event store working! ===

Connection pool disposed.
```

---

## Exercises

### Exercise 1: PostgreSQL Event Store Setup

**Objective:** Set up a complete PostgreSQL-backed event sourcing system.

**Time:** 20-30 minutes

**Requirements:**

1. Create a PostgreSQL database (or use Docker)
2. Set up the schema using the provided SQL
3. Configure `PostgreSQLEventStore` with proper connection pooling
4. Create an aggregate repository
5. Create, modify, and load aggregates
6. Verify events are persisted in the database

**Starter Code:**

```python
"""
Tutorial 11 - Exercise 1: PostgreSQL Event Store Setup

Your task: Set up a complete PostgreSQL event sourcing system.

Requirements:
1. Configure connection pooling appropriately
2. Create the events schema
3. Create a Product aggregate with ProductCreated and PriceUpdated events
4. Save and load products
5. Verify events are stored in PostgreSQL
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


DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"


# TODO: Define ProductCreated event
# TODO: Define PriceUpdated event
# TODO: Define ProductState
# TODO: Define ProductAggregate with create() and update_price() methods


async def main():
    print("=== Exercise 11-1: PostgreSQL Event Store Setup ===\n")

    # TODO: Step 1 - Create engine with connection pooling
    # TODO: Step 2 - Create session factory
    # TODO: Step 3 - Set up database schema
    # TODO: Step 4 - Create PostgreSQLEventStore
    # TODO: Step 5 - Create AggregateRepository
    # TODO: Step 6 - Create, modify, and load a product
    # TODO: Step 7 - Verify events in database


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `async_sessionmaker` with `expire_on_commit=False`
- The constraint `uq_events_aggregate_version` is essential for optimistic locking
- Use `text()` from SQLAlchemy for raw SQL queries
- Remember to dispose the engine when done

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 11 - Exercise 1 Solution: PostgreSQL Event Store Setup
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


DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"


# Events
@register_event
class ProductCreated(DomainEvent):
    event_type: str = "ProductCreated"
    aggregate_type: str = "Product"
    name: str
    price: float


@register_event
class PriceUpdated(DomainEvent):
    event_type: str = "PriceUpdated"
    aggregate_type: str = "Product"
    old_price: float
    new_price: float


# State
class ProductState(BaseModel):
    product_id: str
    name: str = ""
    price: float = 0.0


# Aggregate
class ProductAggregate(AggregateRoot[ProductState]):
    aggregate_type = "Product"

    def _get_initial_state(self) -> ProductState:
        return ProductState(product_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ProductCreated):
            self._state = ProductState(
                product_id=str(self.aggregate_id),
                name=event.name,
                price=event.price,
            )
        elif isinstance(event, PriceUpdated):
            if self._state:
                self._state = self._state.model_copy(update={"price": event.new_price})

    def create(self, name: str, price: float) -> None:
        self.apply_event(ProductCreated(
            aggregate_id=self.aggregate_id,
            name=name,
            price=price,
            aggregate_version=self.get_next_version(),
        ))

    def update_price(self, new_price: float) -> None:
        if not self.state:
            raise ValueError("Product not created")
        self.apply_event(PriceUpdated(
            aggregate_id=self.aggregate_id,
            old_price=self.state.price,
            new_price=new_price,
            aggregate_version=self.get_next_version(),
        ))


async def main():
    print("=== Exercise 11-1: PostgreSQL Event Store Setup ===\n")

    # Step 1: Create engine with connection pooling
    print("Step 1: Creating database engine...")
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
    )

    # Step 2: Create session factory
    print("Step 2: Creating session factory...")
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    try:
        # Step 3: Set up database schema
        print("Step 3: Setting up database schema...")
        async with engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id UUID PRIMARY KEY,
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
        print("   Schema ready!")

        # Step 4: Create PostgreSQLEventStore
        print("\nStep 4: Creating PostgreSQLEventStore...")
        event_store = PostgreSQLEventStore(session_factory)
        print("   Event store created!")

        # Step 5: Create AggregateRepository
        print("\nStep 5: Creating AggregateRepository...")
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=ProductAggregate,
            aggregate_type="Product",
        )
        print("   Repository ready!")

        # Step 6: Create, modify, and load a product
        print("\nStep 6: Testing aggregate operations...")

        # Create
        product_id = uuid4()
        product = repo.create_new(product_id)
        product.create("Widget Pro", 49.99)
        await repo.save(product)
        print(f"   Created: {product.state.name} @ ${product.state.price}")

        # Modify
        product.update_price(39.99)
        await repo.save(product)
        print(f"   Updated price: ${product.state.price}")

        # Load
        loaded = await repo.load(product_id)
        print(f"   Loaded: {loaded.state.name} @ ${loaded.state.price}")
        print(f"   Version: {loaded.version}")

        # Step 7: Verify events in database
        print("\nStep 7: Verifying events in database...")
        async with session_factory() as session:
            result = await session.execute(text(
                "SELECT event_type, version FROM events "
                "WHERE aggregate_id = :id ORDER BY version"
            ), {"id": str(product_id)})
            events = result.fetchall()

        print(f"   Found {len(events)} events:")
        for event_type, version in events:
            print(f"     v{version}: {event_type}")

        print("\n=== Exercise Complete! ===")

    finally:
        await engine.dispose()
        print("\nConnection pool disposed.")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is also available at: `docs/tutorials/exercises/solutions/11-1.py`

---

## Summary

In this tutorial, you learned:

- **PostgreSQL** provides ACID guarantees essential for production event stores
- **Connection pooling** via SQLAlchemy is configured with `create_async_engine()`
- **Session factory** must use `expire_on_commit=False` for async operations
- **PostgreSQLEventStore** requires only the session factory to initialize
- **The outbox pattern** enables reliable event publishing with transactional consistency
- **UUID field configuration** controls how string fields are deserialized
- **Repository code** is identical regardless of event store backend

---

## Key Takeaways

!!! note "Remember"
    - Always use `expire_on_commit=False` in your session factory for async SQLAlchemy
    - Configure connection pool size based on your workload and PostgreSQL limits
    - Run migrations before using the event store
    - Use the outbox pattern when event publishing must be guaranteed

!!! tip "Best Practice"
    Start with reasonable pool settings (`pool_size=5`, `max_overflow=10`) and tune based on monitoring. Too many connections wastes resources; too few causes request queuing.

!!! warning "Common Mistake"
    Forgetting to create indexes on the events table will cause severe performance degradation as your event count grows. Always include the essential indexes from the schema.

---

## Next Steps

Continue to [Tutorial 12: Using SQLite for Development](12-sqlite.md) to learn about a lightweight alternative event store perfect for local development and testing.

---

## Related Documentation

- [Production Deployment Guide](../guides/production.md) - Production patterns and tuning
- [API Reference: PostgreSQLEventStore](../api/stores.md) - Complete API documentation
- [Tutorial 4: Event Stores](04-event-stores.md) - Event store concepts
- [Tutorial 5: Repositories](05-repositories.md) - Repository pattern
- [Tutorial 15: Production Deployment](15-production.md) - Advanced PostgreSQL optimization
