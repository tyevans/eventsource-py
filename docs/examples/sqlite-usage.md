# SQLite Usage Examples

This page provides practical examples for using eventsource with SQLite backends.

## Basic Event Store Usage

### Creating and Using the Store

```python
import asyncio
from uuid import uuid4
from eventsource import SQLiteEventStore, DomainEvent, register_event

# Define your events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total_amount: float

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

async def main():
    # Create and initialize the store
    async with SQLiteEventStore("./orders.db") as store:
        await store.initialize()

        order_id = uuid4()

        # Append events
        result = await store.append_events(
            aggregate_id=order_id,
            aggregate_type="Order",
            events=[
                OrderCreated(
                    aggregate_id=order_id,
                    customer_id="cust-123",
                    total_amount=99.99,
                    aggregate_version=1,
                ),
            ],
            expected_version=0,  # New aggregate
        )

        print(f"Success: {result.success}")
        print(f"New version: {result.new_version}")
        print(f"Global position: {result.global_position}")

        # Retrieve events
        stream = await store.get_events(order_id, "Order")
        print(f"Events in stream: {len(stream.events)}")
        print(f"Current version: {stream.version}")

        for event in stream.events:
            print(f"  - {event.event_type}: {event.event_id}")

asyncio.run(main())
```

### Reading All Events (for Projections)

```python
async def rebuild_projection():
    async with SQLiteEventStore("./orders.db") as store:
        await store.initialize()

        # Read all events in global order
        async for stored_event in store.read_all():
            print(f"Position {stored_event.global_position}: "
                  f"{stored_event.event_type}")

            # Process for your projection
            # projection.handle(stored_event.event)

asyncio.run(rebuild_projection())
```

### Streaming from a Specific Position

```python
from eventsource.stores import ReadOptions, ReadDirection

async def catch_up_from_position(last_position: int):
    async with SQLiteEventStore("./orders.db") as store:
        await store.initialize()

        options = ReadOptions(
            from_position=last_position,
            direction=ReadDirection.FORWARD,
            limit=100,  # Process in batches
        )

        async for stored_event in store.read_all(options):
            print(f"Processing event at position {stored_event.global_position}")
            # ... process event

asyncio.run(catch_up_from_position(last_position=1000))
```

## Repository Pattern Usage

### Complete Aggregate Example

```python
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    AggregateRepository,
    SQLiteEventStore,
)

# Events
@register_event
class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "Account"
    owner_name: str
    initial_balance: float

@register_event
class MoneyDeposited(DomainEvent):
    event_type: str = "MoneyDeposited"
    aggregate_type: str = "Account"
    amount: float

@register_event
class MoneyWithdrawn(DomainEvent):
    event_type: str = "MoneyWithdrawn"
    aggregate_type: str = "Account"
    amount: float

# State
class AccountState(BaseModel):
    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0
    is_open: bool = False

# Aggregate
class AccountAggregate(AggregateRoot[AccountState]):
    aggregate_type = "Account"

    def _get_initial_state(self) -> AccountState:
        return AccountState(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = AccountState(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
                is_open=True,
            )
        elif isinstance(event, MoneyDeposited):
            if self._state:
                self._state = self._state.model_copy(
                    update={"balance": self._state.balance + event.amount}
                )
        elif isinstance(event, MoneyWithdrawn):
            if self._state:
                self._state = self._state.model_copy(
                    update={"balance": self._state.balance - event.amount}
                )

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        if self.version > 0:
            raise ValueError("Account already exists")
        if initial_balance < 0:
            raise ValueError("Initial balance cannot be negative")

        self.apply_event(AccountOpened(
            aggregate_id=self.aggregate_id,
            owner_name=owner_name,
            initial_balance=initial_balance,
            aggregate_version=self.get_next_version(),
        ))

    def deposit(self, amount: float) -> None:
        if not self.state or not self.state.is_open:
            raise ValueError("Account not open")
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")

        self.apply_event(MoneyDeposited(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))

    def withdraw(self, amount: float) -> None:
        if not self.state or not self.state.is_open:
            raise ValueError("Account not open")
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive")
        if amount > self.state.balance:
            raise ValueError("Insufficient funds")

        self.apply_event(MoneyWithdrawn(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))

# Usage
async def main():
    async with SQLiteEventStore("./bank.db") as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=AccountAggregate,
            aggregate_type="Account",
        )

        # Open new account
        account_id = uuid4()
        account = repo.create_new(account_id)
        account.open("Alice Smith", initial_balance=100.00)
        await repo.save(account)
        print(f"Opened account {account_id} with balance ${account.state.balance:.2f}")

        # Make transactions
        account = await repo.load(account_id)
        account.deposit(50.00)
        account.withdraw(25.00)
        await repo.save(account)

        # Check final state
        account = await repo.load(account_id)
        print(f"Final balance: ${account.state.balance:.2f}")
        print(f"Total events: {account.version}")

asyncio.run(main())
```

## Checkpoint Repository Usage

```python
import asyncio
import aiosqlite
from uuid import uuid4
from eventsource import SQLiteEventStore
from eventsource.repositories import SQLiteCheckpointRepository

async def main():
    async with SQLiteEventStore("./app.db") as store:
        await store.initialize()

        # Use the same connection for checkpoint repo
        async with aiosqlite.connect("./app.db") as db:
            checkpoint_repo = SQLiteCheckpointRepository(db)

            # Simulate processing events
            projection_name = "OrderSummaryProjection"

            # Get last checkpoint (None if first run)
            last_checkpoint = await checkpoint_repo.get_checkpoint(projection_name)
            print(f"Last checkpoint: {last_checkpoint}")

            # Process some events...
            event_id = uuid4()
            event_type = "OrderCreated"

            # Update checkpoint after processing
            await checkpoint_repo.update_checkpoint(
                projection_name=projection_name,
                event_id=event_id,
                event_type=event_type,
            )
            print(f"Updated checkpoint to: {event_id}")

            # Get lag metrics
            metrics = await checkpoint_repo.get_lag_metrics(
                projection_name,
                event_types=["OrderCreated", "OrderShipped"],
            )
            if metrics:
                print(f"Events processed: {metrics.events_processed}")
                print(f"Lag seconds: {metrics.lag_seconds}")

asyncio.run(main())
```

## Outbox Repository Usage

```python
import asyncio
import aiosqlite
from uuid import uuid4
from eventsource import SQLiteEventStore, DomainEvent, register_event
from eventsource.repositories import SQLiteOutboxRepository

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_email: str
    total: float

async def add_to_outbox():
    """Add events to outbox within a transaction."""
    async with SQLiteEventStore("./app.db") as store:
        await store.initialize()

        async with aiosqlite.connect("./app.db") as db:
            outbox = SQLiteOutboxRepository(db)

            # Create event
            event = OrderPlaced(
                aggregate_id=uuid4(),
                customer_email="customer@example.com",
                total=149.99,
                aggregate_version=1,
            )

            # Add to outbox
            outbox_id = await outbox.add_event(event)
            print(f"Added to outbox: {outbox_id}")

async def publish_outbox_events():
    """Worker that publishes pending outbox events."""
    async with aiosqlite.connect("./app.db") as db:
        outbox = SQLiteOutboxRepository(db)

        while True:
            # Get pending events
            pending = await outbox.get_pending_events(limit=10)

            if not pending:
                print("No pending events")
                await asyncio.sleep(1)
                continue

            for entry in pending:
                try:
                    # Simulate publishing to message bus
                    print(f"Publishing event {entry.event_type}")
                    # await message_bus.publish(entry.event_data)

                    # Mark as published
                    await outbox.mark_published(entry.id)
                    print(f"Marked as published: {entry.id}")

                except Exception as e:
                    # Increment retry count on failure
                    await outbox.increment_retry(entry.id, str(e))
                    print(f"Failed to publish: {e}")

            break  # Exit loop for example

async def main():
    await add_to_outbox()
    await publish_outbox_events()

asyncio.run(main())
```

## Dead Letter Queue Usage

```python
import asyncio
import aiosqlite
from uuid import uuid4
from eventsource import SQLiteEventStore
from eventsource.repositories import SQLiteDLQRepository

async def main():
    async with SQLiteEventStore("./app.db") as store:
        await store.initialize()

        async with aiosqlite.connect("./app.db") as db:
            dlq = SQLiteDLQRepository(db)

            # Simulate a failed event
            event_id = uuid4()
            try:
                raise ValueError("Processing failed: invalid data format")
            except Exception as e:
                await dlq.add_failed_event(
                    event_id=event_id,
                    projection_name="OrderProjection",
                    event_type="OrderCreated",
                    event_data={"order_id": str(uuid4()), "total": 99.99},
                    error=e,
                    retry_count=1,
                )
                print(f"Added to DLQ: {event_id}")

            # View failed events
            failed_events = await dlq.get_failed_events(
                projection_name="OrderProjection",
                status="failed",
                limit=10,
            )

            print(f"\nFailed events: {len(failed_events)}")
            for entry in failed_events:
                print(f"  - Event: {entry['event_type']}")
                print(f"    Error: {entry['error_message']}")
                print(f"    Retry count: {entry['retry_count']}")

            # Get statistics
            stats = await dlq.get_statistics()
            print(f"\nDLQ Statistics: {stats}")

            # Mark as resolved after fixing
            if failed_events:
                await dlq.mark_resolved(
                    dlq_id=failed_events[0]["id"],
                    resolution="Fixed data issue manually",
                )
                print("Marked as resolved")

asyncio.run(main())
```

## Testing Setup with pytest

### Basic Test Fixture

```python
import pytest
from uuid import uuid4
from eventsource import SQLiteEventStore, AggregateRepository

# Import your domain classes
# from my_app.aggregates import OrderAggregate
# from my_app.events import OrderCreated

@pytest.fixture
async def sqlite_store():
    """In-memory SQLite store for fast, isolated tests."""
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        yield store

@pytest.fixture
async def order_repo(sqlite_store):
    """Repository using in-memory store."""
    return AggregateRepository(
        event_store=sqlite_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

@pytest.mark.asyncio
async def test_create_order(order_repo):
    order_id = uuid4()
    customer_id = uuid4()

    order = order_repo.create_new(order_id)
    order.create(customer_id=customer_id, email="test@example.com")

    assert order.version == 1
    assert order.state.customer_id == customer_id

@pytest.mark.asyncio
async def test_save_and_load_order(order_repo):
    order_id = uuid4()

    # Create and save
    order = order_repo.create_new(order_id)
    order.create(customer_id=uuid4(), email="test@example.com")
    await order_repo.save(order)

    # Load and verify
    loaded = await order_repo.load(order_id)
    assert loaded.version == 1
    assert loaded.state.status == "created"
```

### Testing Projections

```python
import pytest
import aiosqlite
from eventsource import SQLiteEventStore
from eventsource.repositories import (
    SQLiteCheckpointRepository,
    SQLiteDLQRepository,
)
from eventsource.projections import DeclarativeProjection, handles

# Mock projection for testing
class TestProjection(DeclarativeProjection):
    def __init__(self, checkpoint_repo, dlq_repo):
        super().__init__()
        self.orders = {}
        self._checkpoint_repo = checkpoint_repo
        self._dlq_repo = dlq_repo

    @handles(OrderCreated)
    async def _on_order_created(self, event):
        self.orders[event.aggregate_id] = {
            "id": event.aggregate_id,
            "status": "created",
        }

    async def _truncate_read_models(self):
        self.orders.clear()

@pytest.fixture
async def sqlite_db():
    """Shared in-memory database for all repos."""
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        async with aiosqlite.connect(":memory:") as db:
            # Create tables in the same connection
            schema = """
            CREATE TABLE IF NOT EXISTS projection_checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_event_id TEXT,
                last_event_type TEXT,
                last_processed_at TEXT,
                events_processed INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            );
            """
            await db.executescript(schema)
            yield {"store": store, "db": db}

@pytest.fixture
async def projection(sqlite_db):
    """Projection with test dependencies."""
    checkpoint_repo = SQLiteCheckpointRepository(sqlite_db["db"])
    dlq_repo = SQLiteDLQRepository(sqlite_db["db"])
    return TestProjection(checkpoint_repo, dlq_repo)

@pytest.mark.asyncio
async def test_projection_handles_event(projection):
    event = OrderCreated(
        aggregate_id=uuid4(),
        customer_id=uuid4(),
        email="test@example.com",
        aggregate_version=1,
    )

    await projection.handle(event)

    assert event.aggregate_id in projection.orders
    assert projection.orders[event.aggregate_id]["status"] == "created"
```

### Integration Test with Full Stack

For production-like testing, use `SubscriptionManager` to manage projections:

```python
import asyncio
import pytest
from uuid import uuid4
from eventsource import (
    SQLiteEventStore,
    AggregateRepository,
    InMemoryEventBus,
    InMemoryCheckpointRepository,
)
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

@pytest.fixture
async def full_stack():
    """Complete stack for integration testing with SubscriptionManager."""
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()

        bus = InMemoryEventBus()
        checkpoint_repo = InMemoryCheckpointRepository()
        projection = OrderListProjection()

        # Set up SubscriptionManager for proper event delivery
        manager = SubscriptionManager(
            event_store=store,
            event_bus=bus,
            checkpoint_repo=checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config=config, name="OrderList")
        await manager.start()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
            event_publisher=bus,
        )

        yield {
            "store": store,
            "bus": bus,
            "projection": projection,
            "repo": repo,
            "manager": manager,
        }

        # Cleanup
        await manager.stop()

@pytest.mark.asyncio
async def test_full_order_flow(full_stack):
    repo = full_stack["repo"]
    projection = full_stack["projection"]

    # Create order
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(customer_id=uuid4(), email="customer@test.com")
    order.add_item(product_id=uuid4(), name="Widget", quantity=2, price=25.00)
    order.submit()
    await repo.save(order)

    # Wait for events to be processed
    await asyncio.sleep(0.1)

    # Verify aggregate
    loaded = await repo.load(order_id)
    assert loaded.state.status == "submitted"
    assert loaded.state.total == 50.00

    # Verify projection was updated
    assert order_id in projection.orders
    assert projection.orders[order_id]["status"] == "submitted"
```

## File-Based vs In-Memory Databases

### When to Use In-Memory (`:memory:`)

```python
# Fast, isolated tests
store = SQLiteEventStore(":memory:")

# Temporary processing
store = SQLiteEventStore(":memory:")

# Each connection gets a new database
```

### When to Use File-Based

```python
# Development with persistent data
store = SQLiteEventStore("./dev_events.db")

# Local testing with inspection
store = SQLiteEventStore("/tmp/test_events.db")

# Production single-instance
store = SQLiteEventStore("/var/data/myapp/events.db")
```

### Shared In-Memory Database

For testing scenarios where you need multiple connections to the same in-memory database:

```python
# Use URI with shared cache
store = SQLiteEventStore("file::memory:?cache=shared")
```

## See Also

- [SQLite Backend Guide](../guides/sqlite-backend.md) - Configuration and best practices
- [Event Stores API Reference](../api/stores.md) - Complete API documentation
- [Testing Patterns](testing.md) - General testing strategies
- [Basic Order Example](basic-order.md) - Domain modeling example
