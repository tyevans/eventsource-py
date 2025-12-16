# Testing Patterns

This guide covers testing patterns for event-sourced applications using eventsource.

## Overview

Event sourcing makes testing easier because:
- Events are the source of truth
- State is derived from events
- In-memory implementations enable fast tests

---

## Testing Module (Recommended)

The `eventsource.testing` module provides comprehensive testing utilities that reduce boilerplate and improve test readability.

### Installation

The testing module is included with eventsource - no extra dependencies needed:

```python
from eventsource.testing import (
    EventBuilder,
    InMemoryTestHarness,
    EventAssertions,
    given_events,
    when_command,
    then_event_published,
)
```

### EventBuilder - Fluent Event Construction

Build test events with minimal boilerplate:

```python
from uuid import uuid4
from eventsource.testing import EventBuilder
from your_app.events import OrderCreated, OrderShipped

# Basic usage - auto-generates IDs and version
event = EventBuilder(OrderCreated).build(
    customer_id=uuid4(),
    total=99.99,
)

# Fluent API for configuration
order_id = uuid4()
event = (
    EventBuilder(OrderCreated)
    .with_aggregate_id(order_id)
    .with_version(1)
    .with_tenant_id(tenant_id)
    .build(customer_id=uuid4(), total=99.99)
)

# Build a sequence of events for the same aggregate
events = (
    EventBuilder(OrderCreated)
    .with_aggregate_id(order_id)
    .build_sequence([
        {"customer_id": uuid4(), "total": 100.0},  # Version 1
    ])
)

shipped_event = (
    EventBuilder(OrderShipped)
    .with_aggregate_id(order_id)
    .with_version(2)
    .build(tracking_number="TRACK-001")
)
```

### InMemoryTestHarness - Pre-configured Infrastructure

Get a complete test environment with one line:

```python
import pytest
from eventsource.testing import InMemoryTestHarness
from your_app.aggregates import OrderAggregate

@pytest.fixture
async def harness():
    """Provides pre-configured in-memory infrastructure."""
    h = InMemoryTestHarness()
    await h.setup()
    yield h
    await h.teardown()

@pytest.mark.asyncio
async def test_order_creation(harness):
    # Harness provides: event_store, event_bus, checkpoint_repo, dlq_repo
    repo = harness.create_repository(OrderAggregate, "Order")

    order = repo.create_new(uuid4())
    order.create(customer_id=uuid4(), total=100.0)
    await repo.save(order)

    # Access infrastructure for assertions
    events = await harness.event_store.get_events(order.aggregate_id, "Order")
    assert len(events.events) == 1

@pytest.mark.asyncio
async def test_multiple_scenarios(harness):
    # Clear between scenarios
    await harness.clear()

    # ... run next scenario
```

### BDD-Style Helpers

Write expressive, readable tests:

```python
import pytest
from eventsource.testing import (
    given_events,
    when_command,
    then_event_published,
    then_no_events_published,
    then_event_sequence,
    then_event_count,
    InMemoryTestHarness,
    EventBuilder,
)

@pytest.fixture
async def harness():
    h = InMemoryTestHarness()
    await h.setup()
    yield h
    await h.teardown()

@pytest.mark.asyncio
async def test_ship_order_bdd_style(harness):
    order_id = uuid4()

    # Given: Order exists and is paid
    await given_events(harness, [
        EventBuilder(OrderCreated)
            .with_aggregate_id(order_id)
            .build(customer_id=uuid4(), total=100.0),
        EventBuilder(OrderPaid)
            .with_aggregate_id(order_id)
            .with_version(2)
            .build(),
    ])

    # When: Ship order command
    async def ship_command():
        repo = harness.create_repository(OrderAggregate, "Order")
        order = await repo.load(order_id)
        order.ship(tracking_number="TRACK-123")
        await repo.save(order)

    await when_command(harness, ship_command)

    # Then: OrderShipped event was published
    then_event_published(harness, OrderShipped, aggregate_id=order_id)
    then_event_count(harness, 3)  # Created + Paid + Shipped

@pytest.mark.asyncio
async def test_cannot_ship_unpaid_order(harness):
    order_id = uuid4()

    await given_events(harness, [
        EventBuilder(OrderCreated)
            .with_aggregate_id(order_id)
            .build(customer_id=uuid4(), total=100.0),
    ])

    async def ship_unpaid():
        repo = harness.create_repository(OrderAggregate, "Order")
        order = await repo.load(order_id)
        order.ship(tracking_number="TRACK-123")  # Should raise
        await repo.save(order)

    with pytest.raises(ValueError, match="must be paid"):
        await when_command(harness, ship_unpaid)

    # No shipping event should have been published
    then_no_events_published(harness, event_type=OrderShipped)
```

### EventAssertions - Detailed Assertions

For more control over assertions:

```python
from eventsource.testing import EventAssertions

assertions = EventAssertions(harness.event_store, harness.event_bus)

# Assert specific event was published
assertions.assert_event_published(
    OrderShipped,
    aggregate_id=order_id,
    tracking_number="TRACK-123",
)

# Assert event sequence
assertions.assert_event_sequence([
    (OrderCreated, {"total": 100.0}),
    (OrderPaid, {}),
    (OrderShipped, {"tracking_number": "TRACK-123"}),
])

# Assert aggregate state
assertions.assert_aggregate_version(order_id, expected_version=3)
```

### Complete Example

```python
import pytest
from uuid import uuid4
from eventsource.testing import (
    InMemoryTestHarness,
    EventBuilder,
    given_events,
    when_command,
    then_event_published,
)
from your_app.events import OrderCreated, OrderPaid, OrderShipped
from your_app.aggregates import OrderAggregate

@pytest.fixture
async def harness():
    h = InMemoryTestHarness()
    await h.setup()
    yield h
    await h.teardown()

class TestOrderShipping:
    """Tests for order shipping functionality."""

    @pytest.mark.asyncio
    async def test_ship_paid_order(self, harness):
        """Paid orders can be shipped."""
        order_id = uuid4()
        customer_id = uuid4()

        # Arrange
        await given_events(harness, [
            EventBuilder(OrderCreated)
                .with_aggregate_id(order_id)
                .build(customer_id=customer_id, total=50.0),
            EventBuilder(OrderPaid)
                .with_aggregate_id(order_id)
                .with_version(2)
                .build(),
        ])

        # Act
        async def ship():
            repo = harness.create_repository(OrderAggregate, "Order")
            order = await repo.load(order_id)
            order.ship("TRACK-001")
            await repo.save(order)

        await when_command(harness, ship)

        # Assert
        then_event_published(harness, OrderShipped, tracking_number="TRACK-001")

    @pytest.mark.asyncio
    async def test_ship_order_updates_state(self, harness):
        """Shipping updates order status to shipped."""
        order_id = uuid4()

        await given_events(harness, [
            EventBuilder(OrderCreated)
                .with_aggregate_id(order_id)
                .build(customer_id=uuid4(), total=50.0),
            EventBuilder(OrderPaid)
                .with_aggregate_id(order_id)
                .with_version(2)
                .build(),
        ])

        repo = harness.create_repository(OrderAggregate, "Order")
        order = await repo.load(order_id)
        order.ship("TRACK-002")
        await repo.save(order)

        # Reload and verify state
        reloaded = await repo.load(order_id)
        assert reloaded.state.status == "shipped"
        assert reloaded.state.tracking_number == "TRACK-002"
```

---

## Manual Testing Patterns

For simpler tests or when you prefer manual control, you can use the patterns below.

## Unit Testing Aggregates

### Testing Commands

```python
import pytest
from uuid import uuid4
from eventsource import InMemoryEventStore, AggregateRepository

# Test fixtures
@pytest.fixture
def event_store():
    return InMemoryEventStore()

@pytest.fixture
def order_repo(event_store):
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

# Test creating an order
@pytest.mark.asyncio
async def test_create_order(order_repo):
    order_id = uuid4()
    customer_id = uuid4()

    order = order_repo.create_new(order_id)
    order.create(customer_id, "test@example.com")

    assert order.version == 1
    assert order.state.customer_id == customer_id
    assert order.state.status == "created"

# Test business rule validation
@pytest.mark.asyncio
async def test_cannot_add_item_to_submitted_order(order_repo):
    order_id = uuid4()

    # Create and submit order
    order = order_repo.create_new(order_id)
    order.create(uuid4(), "test@example.com")
    order.add_item(uuid4(), "Product", 1, 10.00)
    order.submit()
    await order_repo.save(order)

    # Try to add item
    loaded = await order_repo.load(order_id)
    with pytest.raises(ValueError, match="Can only add items"):
        loaded.add_item(uuid4(), "Another", 1, 5.00)

# Test idempotency
@pytest.mark.asyncio
async def test_cannot_create_twice(order_repo):
    order = order_repo.create_new(uuid4())
    order.create(uuid4(), "test@example.com")

    with pytest.raises(ValueError, match="already exists"):
        order.create(uuid4(), "another@example.com")
```

### Testing Event Application

```python
def test_order_created_event_application():
    order = OrderAggregate(uuid4())
    customer_id = uuid4()

    event = OrderCreated(
        aggregate_id=order.aggregate_id,
        customer_id=customer_id,
        customer_email="test@example.com",
        aggregate_version=1,
    )
    order.apply_event(event, is_new=False)

    assert order.state.customer_id == customer_id
    assert order.state.status == "created"
    assert order.version == 1

def test_state_reconstruction_from_events():
    order_id = uuid4()
    customer_id = uuid4()

    events = [
        OrderCreated(
            aggregate_id=order_id,
            customer_id=customer_id,
            customer_email="test@example.com",
            aggregate_version=1,
        ),
        OrderItemAdded(
            aggregate_id=order_id,
            product_id=uuid4(),
            product_name="Widget",
            quantity=2,
            unit_price=25.00,
            aggregate_version=2,
        ),
        OrderSubmitted(
            aggregate_id=order_id,
            total_amount=50.00,
            aggregate_version=3,
        ),
    ]

    order = OrderAggregate(order_id)
    order.load_from_history(events)

    assert order.version == 3
    assert order.state.status == "submitted"
    assert len(order.state.items) == 1
    assert order.state.total_amount == 50.00
```

## Unit Testing Projections

```python
import pytest
from eventsource.projections import DeclarativeProjection, handles
from eventsource.repositories import InMemoryCheckpointRepository, InMemoryDLQRepository

class TestOrderProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        self.orders = {}

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        self.orders[event.aggregate_id] = {
            "id": event.aggregate_id,
            "customer_email": event.customer_email,
            "status": "created",
        }

    @handles(OrderSubmitted)
    async def _on_submitted(self, event: OrderSubmitted) -> None:
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id]["status"] = "submitted"

    async def _truncate_read_models(self) -> None:
        self.orders.clear()

# Tests

@pytest.fixture
def projection():
    return TestOrderProjection()

@pytest.mark.asyncio
async def test_projection_handles_order_created(projection):
    event = OrderCreated(
        aggregate_id=uuid4(),
        customer_id=uuid4(),
        customer_email="test@example.com",
        aggregate_version=1,
    )

    await projection.handle(event)

    assert event.aggregate_id in projection.orders
    assert projection.orders[event.aggregate_id]["status"] == "created"

@pytest.mark.asyncio
async def test_projection_handles_multiple_events(projection):
    order_id = uuid4()

    await projection.handle(OrderCreated(
        aggregate_id=order_id,
        customer_id=uuid4(),
        customer_email="test@example.com",
        aggregate_version=1,
    ))
    await projection.handle(OrderSubmitted(
        aggregate_id=order_id,
        total_amount=100.00,
        aggregate_version=2,
    ))

    assert projection.orders[order_id]["status"] == "submitted"

@pytest.mark.asyncio
async def test_projection_reset(projection):
    await projection.handle(OrderCreated(
        aggregate_id=uuid4(),
        customer_id=uuid4(),
        customer_email="test@example.com",
        aggregate_version=1,
    ))

    assert len(projection.orders) == 1

    await projection.reset()

    assert len(projection.orders) == 0
```

## Integration Testing

### Testing Event Store Operations

```python
@pytest.mark.asyncio
async def test_append_and_retrieve_events():
    store = InMemoryEventStore()
    aggregate_id = uuid4()

    event1 = OrderCreated(
        aggregate_id=aggregate_id,
        customer_id=uuid4(),
        customer_email="test@example.com",
        aggregate_version=1,
    )
    event2 = OrderItemAdded(
        aggregate_id=aggregate_id,
        product_id=uuid4(),
        product_name="Widget",
        quantity=1,
        unit_price=10.00,
        aggregate_version=2,
    )

    # Append events
    result = await store.append_events(
        aggregate_id=aggregate_id,
        aggregate_type="Order",
        events=[event1, event2],
        expected_version=0,
    )

    assert result.success
    assert result.new_version == 2

    # Retrieve events
    stream = await store.get_events(aggregate_id, "Order")
    assert len(stream.events) == 2
    assert stream.version == 2

@pytest.mark.asyncio
async def test_optimistic_locking():
    store = InMemoryEventStore()
    aggregate_id = uuid4()

    # Initial event
    await store.append_events(
        aggregate_id=aggregate_id,
        aggregate_type="Order",
        events=[OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            customer_email="test@example.com",
            aggregate_version=1,
        )],
        expected_version=0,
    )

    # Try to append with wrong version
    from eventsource import OptimisticLockError

    with pytest.raises(OptimisticLockError):
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[OrderSubmitted(
                aggregate_id=aggregate_id,
                total_amount=100.00,
                aggregate_version=2,
            )],
            expected_version=0,  # Should be 1
        )
```

### Testing Full Flow

```python
@pytest.mark.asyncio
async def test_full_order_flow():
    # Setup
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    projection = TestOrderProjection()

    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=bus,
    )

    bus.subscribe_all(projection)

    # Create order
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(uuid4(), "customer@test.com")
    order.add_item(uuid4(), "Product", 2, 25.00)
    order.submit()
    await repo.save(order)

    # Verify aggregate
    loaded = await repo.load(order_id)
    assert loaded.state.status == "submitted"
    assert loaded.state.total_amount == 50.00

    # Verify projection
    assert order_id in projection.orders
    assert projection.orders[order_id]["status"] == "submitted"

    # Verify event store
    stream = await store.get_events(order_id, "Order")
    assert len(stream.events) == 3  # created, item_added, submitted
```

## Testing with PostgreSQL (Integration)

```python
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource import PostgreSQLEventStore

@pytest.fixture
async def pg_store():
    """Create PostgreSQL event store for integration tests."""
    engine = create_async_engine(
        "postgresql+asyncpg://test:test@localhost/test_db"
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    store = PostgreSQLEventStore(session_factory)

    yield store

    # Cleanup
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE events"))

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgresql_event_store(pg_store):
    aggregate_id = uuid4()

    result = await pg_store.append_events(
        aggregate_id=aggregate_id,
        aggregate_type="Order",
        events=[OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            customer_email="test@example.com",
            aggregate_version=1,
        )],
        expected_version=0,
    )

    assert result.success

    stream = await pg_store.get_events(aggregate_id, "Order")
    assert len(stream.events) == 1
```

## Test Helpers

### Event Builder

```python
class EventBuilder:
    """Helper for building test events."""

    def __init__(self, aggregate_id: UUID | None = None):
        self.aggregate_id = aggregate_id or uuid4()
        self.version = 0

    def order_created(self, **kwargs) -> OrderCreated:
        self.version += 1
        return OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=kwargs.get("customer_id", uuid4()),
            customer_email=kwargs.get("email", "test@example.com"),
            aggregate_version=self.version,
        )

    def item_added(self, **kwargs) -> OrderItemAdded:
        self.version += 1
        return OrderItemAdded(
            aggregate_id=self.aggregate_id,
            product_id=kwargs.get("product_id", uuid4()),
            product_name=kwargs.get("name", "Test Product"),
            quantity=kwargs.get("quantity", 1),
            unit_price=kwargs.get("price", 10.00),
            aggregate_version=self.version,
        )

# Usage in tests
def test_with_builder():
    builder = EventBuilder()

    events = [
        builder.order_created(email="customer@test.com"),
        builder.item_added(name="Widget", quantity=2, price=15.00),
        builder.item_added(name="Gadget", quantity=1, price=30.00),
    ]

    order = OrderAggregate(builder.aggregate_id)
    order.load_from_history(events)

    assert order.version == 3
    assert len(order.state.items) == 2
```

### Aggregate Test Helper

```python
class AggregateTestHelper:
    """Helper for testing aggregates in isolation."""

    def __init__(self, aggregate_class, aggregate_type: str):
        self.aggregate_class = aggregate_class
        self.aggregate_type = aggregate_type
        self.store = InMemoryEventStore()
        self.repo = AggregateRepository(
            self.store, aggregate_class, aggregate_type
        )

    def create(self, aggregate_id: UUID | None = None):
        return self.repo.create_new(aggregate_id or uuid4())

    async def save(self, aggregate):
        await self.repo.save(aggregate)

    async def load(self, aggregate_id: UUID):
        return await self.repo.load(aggregate_id)

    def get_events(self, aggregate_id: UUID):
        return self.store._events.get(aggregate_id, [])

# Usage
@pytest.fixture
def order_helper():
    return AggregateTestHelper(OrderAggregate, "Order")

@pytest.mark.asyncio
async def test_order_flow(order_helper):
    order = order_helper.create()
    order.create(uuid4(), "test@example.com")
    await order_helper.save(order)

    events = order_helper.get_events(order.aggregate_id)
    assert len(events) == 1
```

## Best Practices

1. **Use InMemoryEventStore for unit tests** - Fast and deterministic
2. **Test event application separately** - Verify state transitions work correctly
3. **Test business rules** - Ensure commands validate properly
4. **Test idempotency** - Commands should be safe to retry
5. **Test projections in isolation** - Feed events directly
6. **Use fixtures** - Share setup code between tests
7. **Mark integration tests** - Separate slow tests from fast ones

## See Also

- [Testing Strategy Guide](../development/testing.md) - Comprehensive testing documentation with advanced patterns, integration testing setup, and CI/CD examples
