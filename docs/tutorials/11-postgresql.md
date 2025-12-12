# Tutorial 11: Using PostgreSQL Event Store

**Difficulty:** Intermediate | **Progress:** Tutorial 11 of 21 | Phase 3: Production Readiness

PostgreSQL is the recommended event store for production deployments, providing ACID guarantees, JSONB storage, row-level locking, replication, and partitioning support. Use it for production deployments, multi-process applications, and when you need durable, queryable event history.

**Prerequisites:** Completed Tutorial 10, PostgreSQL installed, Python 3.11+, and `pip install eventsource-py[postgresql]`

---

## PostgreSQL Setup

**Docker (recommended):**

```bash
docker run -d --name eventsource-postgres \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=eventsource_tutorial -p 5432:5432 postgres:15
```

**Local:** `CREATE DATABASE eventsource_tutorial;`

**Connection string format:** `postgresql+asyncpg://user:password@host:port/database`

```python
# Examples
LOCAL_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"
PROD_URL = "postgresql+asyncpg://user:pass@prod-host:5432/mydb?ssl=require"
```

---

## Database Schema

```sql
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
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate_id ON events (aggregate_id);
CREATE INDEX IF NOT EXISTS idx_events_aggregate_type ON events (aggregate_type);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, aggregate_type, version);
```

**Run migrations:** Use `src/eventsource/migrations/schemas/all.sql`, run SQL via psql, or use Alembic templates from `src/eventsource/migrations/templates/alembic/`.

---

## Configuring PostgreSQLEventStore

**Connection pool guidelines:** Light (2-5/5), Medium (5-10/10), Heavy (10-20/20) for pool_size/max_overflow.

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from eventsource import PostgreSQLEventStore

def create_event_store(database_url: str) -> tuple[PostgreSQLEventStore, any]:
    # Create engine with connection pooling
    engine = create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
    )

    # Create session factory (expire_on_commit=False is REQUIRED for async)
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    # Create event store
    event_store = PostgreSQLEventStore(session_factory)
    return event_store, engine

# Usage
DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"
event_store, engine = create_event_store(DATABASE_URL)
# Remember: await engine.dispose() on shutdown
```

---

## Using with Repositories

Repository code is identical regardless of event store backend:

```python
from uuid import uuid4
from eventsource import AggregateRepository

async def main():
    event_store, engine = create_event_store(DATABASE_URL)

    try:
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
    finally:
        await engine.dispose()
```

---

## Constructor Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `session_factory` | Required | SQLAlchemy async session factory |
| `outbox_enabled` | `False` | Enable outbox pattern for reliable publishing |
| `enable_tracing` | `True` | OpenTelemetry tracing support |
| `uuid_fields` | `None` | Additional UUID field names to detect |
| `string_id_fields` | `None` | Fields to exclude from UUID auto-detection |

**UUID field configuration:**

```python
# Add custom UUID fields
event_store = PostgreSQLEventStore(session_factory, uuid_fields={"parent_id"})

# Exclude string IDs from auto-detection
event_store = PostgreSQLEventStore(session_factory, string_id_fields={"stripe_customer_id"})
```

---

## The Outbox Pattern

The outbox pattern guarantees event publishing by writing events to both the events table and an outbox table in the same transaction. A background worker polls the outbox and publishes events to message brokers.

```python
event_store = PostgreSQLEventStore(session_factory, outbox_enabled=True)
```

**Outbox table schema:**

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

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON event_outbox (created_at)
    WHERE status = 'pending';
```

Full schema at `src/eventsource/migrations/templates/outbox.sql`.

**Use when:** Event publishing must be guaranteed, broker may be unavailable, need exactly-once delivery.
**Skip when:** Best-effort publishing is acceptable, development/testing, broker is highly available.

---

## Multi-Tenancy & Performance

**Multi-tenancy:** Add `tenant_id: UUID` to events and query with `tenant_id` parameter.

**Performance tips:**
- Tune connection pool based on monitoring (increase pool_size if exhausted, enable pool_pre_ping for stale connections)
- All essential indexes are included in the schema
- For millions+ events, use partitioning template at `src/eventsource/migrations/templates/events_partitioned.sql`

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

---

## Next Steps

Continue to [Tutorial 12: Using SQLite for Development](12-sqlite.md) to learn about a lightweight alternative event store perfect for local development and testing.
