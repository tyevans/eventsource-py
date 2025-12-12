"""
Tutorial 11 - Exercise 1 Solution: PostgreSQL Event Store Setup

This solution demonstrates setting up a complete PostgreSQL-backed
event sourcing system with proper connection pooling and schema.

Prerequisites:
- PostgreSQL running on localhost:5432
- Database 'eventsource_tutorial' created
- pip install eventsource-py[postgresql]

Run with: python 11-1.py
"""

import asyncio
from uuid import uuid4

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    PostgreSQLEventStore,
    register_event,
)

# =============================================================================
# Configuration
# =============================================================================

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/eventsource_tutorial"


# =============================================================================
# Events
# =============================================================================


@register_event
class ProductCreated(DomainEvent):
    """Event raised when a product is created."""

    event_type: str = "ProductCreated"
    aggregate_type: str = "Product"
    name: str
    price: float


@register_event
class PriceUpdated(DomainEvent):
    """Event raised when a product's price is updated."""

    event_type: str = "PriceUpdated"
    aggregate_type: str = "Product"
    old_price: float
    new_price: float


# =============================================================================
# State
# =============================================================================


class ProductState(BaseModel):
    """Represents the current state of a product."""

    product_id: str
    name: str = ""
    price: float = 0.0


# =============================================================================
# Aggregate
# =============================================================================


class ProductAggregate(AggregateRoot[ProductState]):
    """
    Product aggregate that manages product lifecycle.

    Supports creating products and updating prices.
    """

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
        elif isinstance(event, PriceUpdated) and self._state:
            self._state = self._state.model_copy(update={"price": event.new_price})

    def create(self, name: str, price: float) -> None:
        """Create a new product with the given name and price."""
        if self.version > 0:
            raise ValueError("Product already exists")
        if price < 0:
            raise ValueError("Price cannot be negative")

        self.apply_event(
            ProductCreated(
                aggregate_id=self.aggregate_id,
                name=name,
                price=price,
                aggregate_version=self.get_next_version(),
            )
        )

    def update_price(self, new_price: float) -> None:
        """Update the product's price."""
        if not self.state:
            raise ValueError("Product not created")
        if new_price < 0:
            raise ValueError("Price cannot be negative")
        if new_price == self.state.price:
            return  # No change needed

        self.apply_event(
            PriceUpdated(
                aggregate_id=self.aggregate_id,
                old_price=self.state.price,
                new_price=new_price,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Database Setup
# =============================================================================


async def setup_schema(engine) -> None:
    """Create the events table and indexes."""
    async with engine.begin() as conn:
        # Create events table
        await conn.execute(
            text("""
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
        """)
        )

        # Create indexes for common query patterns
        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_events_aggregate_id
            ON events(aggregate_id)
        """)
        )
        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_events_aggregate_type
            ON events(aggregate_type)
        """)
        )
        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_events_event_type
            ON events(event_type)
        """)
        )
        await conn.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_events_timestamp
            ON events(timestamp)
        """)
        )


# =============================================================================
# Main Demo
# =============================================================================


async def main():
    print("=== Exercise 11-1: PostgreSQL Event Store Setup ===\n")

    # Step 1: Create engine with connection pooling
    print("Step 1: Creating database engine with connection pooling...")
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=5,  # Keep 5 connections open
        max_overflow=10,  # Allow 10 more when busy
        pool_timeout=30,  # Wait up to 30s for connection
        pool_recycle=1800,  # Recycle connections after 30 min
        pool_pre_ping=True,  # Verify connections before use
    )
    print("   Engine created with pool_size=5, max_overflow=10")

    # Step 2: Create session factory
    print("\nStep 2: Creating async session factory...")
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,  # Required for async!
        class_=AsyncSession,
    )
    print("   Session factory created (expire_on_commit=False)")

    try:
        # Step 3: Set up database schema
        print("\nStep 3: Setting up database schema...")
        await setup_schema(engine)
        print("   Schema and indexes created!")

        # Step 4: Create PostgreSQLEventStore
        print("\nStep 4: Creating PostgreSQLEventStore...")
        event_store = PostgreSQLEventStore(
            session_factory,
            outbox_enabled=False,
            enable_tracing=True,
        )
        print("   Event store ready!")

        # Step 5: Create AggregateRepository
        print("\nStep 5: Creating AggregateRepository...")
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=ProductAggregate,
            aggregate_type="Product",
        )
        print("   Repository configured for Product aggregates")

        # Step 6: Create, modify, and load a product
        print("\nStep 6: Testing aggregate operations...")

        # Create a product
        product_id = uuid4()
        print(f"\n   Creating product {product_id}...")
        product = repo.create_new(product_id)
        product.create("Widget Pro", 49.99)
        await repo.save(product)
        print(f"   Created: {product.state.name} @ ${product.state.price:.2f}")
        print(f"   Version: {product.version}")

        # Update the price
        print("\n   Updating price...")
        product.update_price(39.99)
        await repo.save(product)
        print(f"   New price: ${product.state.price:.2f}")
        print(f"   Version: {product.version}")

        # Update again
        product.update_price(44.99)
        await repo.save(product)
        print(f"   New price: ${product.state.price:.2f}")
        print(f"   Version: {product.version}")

        # Load from database (fresh instance)
        print("\n   Loading product from database...")
        loaded = await repo.load(product_id)
        print(f"   Loaded: {loaded.state.name} @ ${loaded.state.price:.2f}")
        print(f"   Version: {loaded.version}")

        # Step 7: Verify events in database
        print("\nStep 7: Verifying events stored in PostgreSQL...")
        async with session_factory() as session:
            result = await session.execute(
                text("""
                SELECT event_type, version, timestamp
                FROM events
                WHERE aggregate_id = :id
                ORDER BY version
            """),
                {"id": str(product_id)},
            )
            events = result.fetchall()

        print(f"\n   Found {len(events)} events in database:")
        for event_type, version, timestamp in events:
            print(f"     v{version}: {event_type} at {timestamp}")

        # Bonus: Show raw event data
        print("\n   Raw event payloads:")
        async with session_factory() as session:
            result = await session.execute(
                text("""
                SELECT event_type, payload
                FROM events
                WHERE aggregate_id = :id
                ORDER BY version
            """),
                {"id": str(product_id)},
            )
            raw_events = result.fetchall()

        for event_type, payload in raw_events:
            print(f"     {event_type}: {payload}")

        print("\n=== Exercise Complete! ===")
        print("\nKey learnings:")
        print("  - Connection pooling is configured on the engine")
        print("  - expire_on_commit=False is required for async")
        print("  - Events are stored as JSONB in PostgreSQL")
        print("  - The unique constraint prevents concurrent modifications")

    finally:
        # Always dispose the engine to clean up connections
        await engine.dispose()
        print("\nConnection pool disposed.")


if __name__ == "__main__":
    asyncio.run(main())
