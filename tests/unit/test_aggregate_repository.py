"""
Comprehensive unit tests for AggregateRepository.

Tests cover:
- Repository initialization and properties
- Loading aggregates from event history (load, load_or_create, get_or_raise)
- Saving aggregates with uncommitted events
- Optimistic concurrency control
- Event publishing after save
- Existence checking (exists, get_version)
- Creating new aggregate instances (create_new)
- Error handling (AggregateNotFoundError, OptimisticLockError)
- Integration with InMemoryEventStore
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field

from eventsource.aggregates.base import AggregateRoot
from eventsource.aggregates.repository import AggregateRepository
from eventsource.events.base import DomainEvent
from eventsource.exceptions import AggregateNotFoundError, OptimisticLockError
from eventsource.stores.in_memory import InMemoryEventStore

# =============================================================================
# Test fixtures: State models and Events
# =============================================================================


class CounterState(BaseModel):
    """Simple state for testing."""

    counter_id: UUID
    value: int = 0
    name: str = ""


class OrderState(BaseModel):
    """More complex state model for testing."""

    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"
    total: float = 0.0
    items: list[str] = Field(default_factory=list)


class CounterIncremented(DomainEvent):
    """Event for incrementing counter."""

    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"
    increment: int = 1


class CounterDecremented(DomainEvent):
    """Event for decrementing counter."""

    event_type: str = "CounterDecremented"
    aggregate_type: str = "Counter"
    decrement: int = 1


class OrderCreated(DomainEvent):
    """Event for order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID


class OrderItemAdded(DomainEvent):
    """Event for adding item to order."""

    event_type: str = "OrderItemAdded"
    aggregate_type: str = "Order"
    item_name: str
    price: float


class OrderShipped(DomainEvent):
    """Event for shipping order."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


# =============================================================================
# Test Aggregate Implementations
# =============================================================================


class CounterAggregate(AggregateRoot[CounterState]):
    """Simple counter aggregate for testing."""

    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={"value": self._state.value + event.increment}
            )
        elif isinstance(event, CounterDecremented):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={"value": self._state.value - event.decrement}
            )

    def increment(self, amount: int = 1) -> None:
        """Command: Increment the counter."""
        event = CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            increment=amount,
        )
        self.apply_event(event)

    def decrement(self, amount: int = 1) -> None:
        """Command: Decrement the counter."""
        event = CounterDecremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            decrement=amount,
        )
        self.apply_event(event)


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate for testing."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, OrderItemAdded):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "items": [*self._state.items, event.item_name],
                        "total": self._state.total + event.price,
                    }
                )
        elif isinstance(event, OrderShipped) and self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})

    def create(self, customer_id: UUID) -> None:
        """Command: Create the order."""
        if self.version > 0:
            raise ValueError("Order already exists")
        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            customer_id=customer_id,
        )
        self.apply_event(event)

    def add_item(self, item_name: str, price: float) -> None:
        """Command: Add an item to the order."""
        if not self._state or self._state.status == "shipped":
            raise ValueError("Cannot add items to this order")
        event = OrderItemAdded(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            item_name=item_name,
            price=price,
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        """Command: Ship the order."""
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot ship order in current state")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            tracking_number=tracking_number,
        )
        self.apply_event(event)


# =============================================================================
# Mock Event Publisher for testing
# =============================================================================


class MockEventPublisher:
    """Mock event publisher for testing."""

    def __init__(self) -> None:
        self.published_events: list[DomainEvent] = []

    async def publish(self, events: list[DomainEvent]) -> None:
        self.published_events.extend(events)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore for each test."""
    return InMemoryEventStore()


@pytest.fixture
def counter_repository(event_store: InMemoryEventStore) -> AggregateRepository[CounterAggregate]:
    """Create a repository for CounterAggregate."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=CounterAggregate,
        aggregate_type="Counter",
    )


@pytest.fixture
def order_repository(event_store: InMemoryEventStore) -> AggregateRepository[OrderAggregate]:
    """Create a repository for OrderAggregate."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )


@pytest.fixture
def mock_publisher() -> MockEventPublisher:
    """Create a mock event publisher."""
    return MockEventPublisher()


@pytest.fixture
def counter_repository_with_publisher(
    event_store: InMemoryEventStore, mock_publisher: MockEventPublisher
) -> AggregateRepository[CounterAggregate]:
    """Create a repository with event publisher."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=CounterAggregate,
        aggregate_type="Counter",
        event_publisher=mock_publisher,
    )


# =============================================================================
# Test Classes
# =============================================================================


class TestRepositoryInitialization:
    """Tests for repository initialization and properties."""

    def test_repository_stores_event_store(self, event_store: InMemoryEventStore) -> None:
        """Repository should store the event store reference."""
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=CounterAggregate,
            aggregate_type="Counter",
        )
        assert repo.event_store is event_store

    def test_repository_stores_aggregate_type(self, event_store: InMemoryEventStore) -> None:
        """Repository should store the aggregate type."""
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=CounterAggregate,
            aggregate_type="Counter",
        )
        assert repo.aggregate_type == "Counter"

    def test_repository_stores_event_publisher(
        self, event_store: InMemoryEventStore, mock_publisher: MockEventPublisher
    ) -> None:
        """Repository should store the event publisher if provided."""
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=CounterAggregate,
            aggregate_type="Counter",
            event_publisher=mock_publisher,
        )
        assert repo.event_publisher is mock_publisher

    def test_repository_without_publisher(self, event_store: InMemoryEventStore) -> None:
        """Repository without publisher should have None event_publisher."""
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=CounterAggregate,
            aggregate_type="Counter",
        )
        assert repo.event_publisher is None


class TestLoadAggregate:
    """Tests for loading aggregates from event store."""

    @pytest.mark.asyncio
    async def test_load_existing_aggregate(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """load() should reconstitute aggregate from events."""
        agg_id = uuid4()

        # Create events directly in event store
        events = [
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=10,
            ),
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=2,
                increment=5,
            ),
            CounterDecremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=3,
                decrement=3,
            ),
        ]
        await event_store.append_events(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            events=events,
            expected_version=0,
        )

        # Load through repository
        aggregate = await counter_repository.load(agg_id)

        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 12  # 10 + 5 - 3

    @pytest.mark.asyncio
    async def test_load_nonexistent_aggregate_raises_error(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """load() should raise AggregateNotFoundError for nonexistent aggregate."""
        nonexistent_id = uuid4()

        with pytest.raises(AggregateNotFoundError) as exc_info:
            await counter_repository.load(nonexistent_id)

        assert exc_info.value.aggregate_id == nonexistent_id
        assert exc_info.value.aggregate_type == "Counter"

    @pytest.mark.asyncio
    async def test_load_aggregate_has_no_uncommitted_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """Loaded aggregate should have no uncommitted events."""
        agg_id = uuid4()

        event = CounterIncremented(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        await event_store.append_events(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            events=[event],
            expected_version=0,
        )

        aggregate = await counter_repository.load(agg_id)

        assert not aggregate.has_uncommitted_events
        assert len(aggregate.uncommitted_events) == 0


class TestLoadOrCreate:
    """Tests for load_or_create method."""

    @pytest.mark.asyncio
    async def test_load_or_create_loads_existing(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """load_or_create() should load existing aggregate."""
        agg_id = uuid4()

        event = CounterIncremented(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=10,
        )
        await event_store.append_events(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            events=[event],
            expected_version=0,
        )

        aggregate = await counter_repository.load_or_create(agg_id)

        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 10

    @pytest.mark.asyncio
    async def test_load_or_create_creates_new(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """load_or_create() should create new aggregate if not found."""
        new_id = uuid4()

        aggregate = await counter_repository.load_or_create(new_id)

        assert aggregate.aggregate_id == new_id
        assert aggregate.version == 0
        assert aggregate.state is None

    @pytest.mark.asyncio
    async def test_load_or_create_new_aggregate_can_be_modified(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """New aggregate from load_or_create() should be usable."""
        new_id = uuid4()

        aggregate = await counter_repository.load_or_create(new_id)
        aggregate.increment(5)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 5
        assert aggregate.has_uncommitted_events


class TestGetOrRaise:
    """Tests for get_or_raise method."""

    @pytest.mark.asyncio
    async def test_get_or_raise_returns_existing(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """get_or_raise() should return existing aggregate."""
        agg_id = uuid4()

        event = CounterIncremented(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=7,
        )
        await event_store.append_events(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            events=[event],
            expected_version=0,
        )

        aggregate = await counter_repository.get_or_raise(agg_id)

        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 1

    @pytest.mark.asyncio
    async def test_get_or_raise_raises_for_nonexistent(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """get_or_raise() should raise AggregateNotFoundError."""
        nonexistent_id = uuid4()

        with pytest.raises(AggregateNotFoundError):
            await counter_repository.get_or_raise(nonexistent_id)


class TestSaveAggregate:
    """Tests for saving aggregates."""

    @pytest.mark.asyncio
    async def test_save_new_aggregate(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """save() should persist uncommitted events."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        aggregate.increment(3)

        await counter_repository.save(aggregate)

        # Verify events were persisted
        stream = await event_store.get_events(agg_id, "Counter")
        assert len(stream.events) == 2
        assert stream.version == 2

    @pytest.mark.asyncio
    async def test_save_clears_uncommitted_events(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """save() should mark events as committed."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        assert aggregate.has_uncommitted_events

        await counter_repository.save(aggregate)

        assert not aggregate.has_uncommitted_events
        assert len(aggregate.uncommitted_events) == 0

    @pytest.mark.asyncio
    async def test_save_preserves_aggregate_state(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """save() should not affect aggregate state."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(10)

        state_before = aggregate.state
        version_before = aggregate.version

        await counter_repository.save(aggregate)

        assert aggregate.state == state_before
        assert aggregate.version == version_before

    @pytest.mark.asyncio
    async def test_save_no_op_when_no_uncommitted_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """save() should be no-op when there are no uncommitted events."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)

        # Save without any changes
        await counter_repository.save(aggregate)

        # Event store should have no events
        stream = await event_store.get_events(agg_id, "Counter")
        assert len(stream.events) == 0

    @pytest.mark.asyncio
    async def test_save_existing_aggregate_with_new_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """save() should append new events to existing aggregate."""
        agg_id = uuid4()

        # Create and save initial aggregate
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        # Load, modify, and save again
        loaded = await counter_repository.load(agg_id)
        loaded.increment(3)
        await counter_repository.save(loaded)

        # Verify all events are persisted
        stream = await event_store.get_events(agg_id, "Counter")
        assert len(stream.events) == 2
        assert stream.version == 2


class TestOptimisticConcurrency:
    """Tests for optimistic concurrency control."""

    @pytest.mark.asyncio
    async def test_concurrent_modification_raises_error(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """Concurrent modification should raise OptimisticLockError."""
        agg_id = uuid4()

        # Create initial aggregate
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        # Load aggregate twice (simulating two concurrent users)
        loaded1 = await counter_repository.load(agg_id)
        loaded2 = await counter_repository.load(agg_id)

        # Both modify the aggregate
        loaded1.increment(10)
        loaded2.increment(20)

        # First save succeeds
        await counter_repository.save(loaded1)

        # Second save should fail
        with pytest.raises(OptimisticLockError) as exc_info:
            await counter_repository.save(loaded2)

        assert exc_info.value.aggregate_id == agg_id
        assert exc_info.value.expected_version == 1  # loaded2 expects version 1
        assert exc_info.value.actual_version == 2  # But actual version is 2

    @pytest.mark.asyncio
    async def test_expected_version_calculated_correctly(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """Expected version should be current version minus uncommitted events."""
        agg_id = uuid4()

        # Create aggregate with some events
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(1)
        aggregate.increment(2)
        aggregate.increment(3)
        await counter_repository.save(aggregate)

        # Load and add more events
        loaded = await counter_repository.load(agg_id)
        loaded.increment(4)
        loaded.increment(5)

        # Before save:
        # - loaded.version = 5 (3 from history + 2 new)
        # - uncommitted_events = 2
        # - expected_version should be 5 - 2 = 3
        assert loaded.version == 5
        assert len(loaded.uncommitted_events) == 2

        # Save should succeed
        await counter_repository.save(loaded)

        # Verify final state
        final = await counter_repository.load(agg_id)
        assert final.version == 5


class TestEventPublishing:
    """Tests for event publishing after save."""

    @pytest.mark.asyncio
    async def test_events_published_after_save(
        self,
        counter_repository_with_publisher: AggregateRepository[CounterAggregate],
        mock_publisher: MockEventPublisher,
    ) -> None:
        """Events should be published after successful save."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        await counter_repository_with_publisher.save(aggregate)

        assert len(mock_publisher.published_events) == 2
        assert all(isinstance(e, CounterIncremented) for e in mock_publisher.published_events)

    @pytest.mark.asyncio
    async def test_no_publishing_when_no_events(
        self,
        counter_repository_with_publisher: AggregateRepository[CounterAggregate],
        mock_publisher: MockEventPublisher,
    ) -> None:
        """No events should be published when there are no uncommitted events."""
        aggregate = CounterAggregate(uuid4())

        await counter_repository_with_publisher.save(aggregate)

        assert len(mock_publisher.published_events) == 0

    @pytest.mark.asyncio
    async def test_no_publishing_without_publisher(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Save should work without publisher configured."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        # Should not raise
        await counter_repository.save(aggregate)

        assert not aggregate.has_uncommitted_events


class TestExistsMethod:
    """Tests for exists method."""

    @pytest.mark.asyncio
    async def test_exists_returns_true_for_existing(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
    ) -> None:
        """exists() should return True for existing aggregate."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        result = await counter_repository.exists(agg_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_exists_returns_false_for_nonexistent(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """exists() should return False for nonexistent aggregate."""
        nonexistent_id = uuid4()

        result = await counter_repository.exists(nonexistent_id)

        assert result is False


class TestGetVersionMethod:
    """Tests for get_version method."""

    @pytest.mark.asyncio
    async def test_get_version_returns_correct_version(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
    ) -> None:
        """get_version() should return current version."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(1)
        aggregate.increment(2)
        aggregate.increment(3)
        await counter_repository.save(aggregate)

        version = await counter_repository.get_version(agg_id)

        assert version == 3

    @pytest.mark.asyncio
    async def test_get_version_returns_zero_for_nonexistent(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """get_version() should return 0 for nonexistent aggregate."""
        nonexistent_id = uuid4()

        version = await counter_repository.get_version(nonexistent_id)

        assert version == 0


class TestCreateNewMethod:
    """Tests for create_new method."""

    def test_create_new_returns_empty_aggregate(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """create_new() should return new aggregate with version 0."""
        new_id = uuid4()

        aggregate = counter_repository.create_new(new_id)

        assert aggregate.aggregate_id == new_id
        assert aggregate.version == 0
        assert aggregate.state is None
        assert not aggregate.has_uncommitted_events

    def test_create_new_can_be_modified_and_saved(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Aggregate from create_new() should be modifiable."""
        aggregate = counter_repository.create_new(uuid4())
        aggregate.increment(5)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 5
        assert aggregate.has_uncommitted_events


class TestComplexScenarios:
    """Tests for complex usage scenarios."""

    @pytest.mark.asyncio
    async def test_full_order_lifecycle(
        self,
        order_repository: AggregateRepository[OrderAggregate],
    ) -> None:
        """Test complete order lifecycle through repository."""
        order_id = uuid4()
        customer_id = uuid4()

        # Create order
        order = order_repository.create_new(order_id)
        order.create(customer_id)
        await order_repository.save(order)

        # Add items
        order = await order_repository.load(order_id)
        order.add_item("Widget A", 10.0)
        order.add_item("Widget B", 15.0)
        await order_repository.save(order)

        # Ship order
        order = await order_repository.load(order_id)
        order.ship("TRACK123")
        await order_repository.save(order)

        # Verify final state
        final_order = await order_repository.load(order_id)
        assert final_order.version == 4
        assert final_order.state is not None
        assert final_order.state.status == "shipped"
        assert final_order.state.customer_id == customer_id
        assert final_order.state.items == ["Widget A", "Widget B"]
        assert final_order.state.total == 25.0

    @pytest.mark.asyncio
    async def test_multiple_aggregates_same_repository(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Repository should handle multiple aggregates correctly."""
        id1 = uuid4()
        id2 = uuid4()

        # Create first counter
        counter1 = counter_repository.create_new(id1)
        counter1.increment(10)
        await counter_repository.save(counter1)

        # Create second counter
        counter2 = counter_repository.create_new(id2)
        counter2.increment(20)
        await counter_repository.save(counter2)

        # Load and verify both
        loaded1 = await counter_repository.load(id1)
        loaded2 = await counter_repository.load(id2)

        assert loaded1.state.value == 10
        assert loaded2.state.value == 20

    @pytest.mark.asyncio
    async def test_save_multiple_times(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Multiple saves should work correctly."""
        agg_id = uuid4()
        aggregate = counter_repository.create_new(agg_id)

        for i in range(10):
            aggregate.increment(i + 1)
            await counter_repository.save(aggregate)

        loaded = await counter_repository.load(agg_id)
        assert loaded.version == 10
        assert loaded.state.value == 55  # Sum of 1 to 10


class TestGenericTyping:
    """Tests to verify generic typing works correctly."""

    @pytest.mark.asyncio
    async def test_different_aggregate_types_in_different_repos(
        self, event_store: InMemoryEventStore
    ) -> None:
        """Different repositories should handle different aggregate types."""
        counter_repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=CounterAggregate,
            aggregate_type="Counter",
        )

        order_repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create and save counter
        counter = counter_repo.create_new(uuid4())
        counter.increment(5)
        await counter_repo.save(counter)

        # Create and save order
        order = order_repo.create_new(uuid4())
        order.create(uuid4())
        await order_repo.save(order)

        # Load and verify types
        loaded_counter = await counter_repo.load(counter.aggregate_id)
        loaded_order = await order_repo.load(order.aggregate_id)

        assert isinstance(loaded_counter, CounterAggregate)
        assert isinstance(loaded_order, OrderAggregate)


class TestImports:
    """Tests for verifying correct module exports."""

    def test_import_from_aggregates_module(self) -> None:
        """Test importing AggregateRepository from aggregates module."""
        from eventsource.aggregates import AggregateRepository

        assert AggregateRepository is not None

    def test_import_from_main_module(self) -> None:
        """Test importing AggregateRepository from main eventsource module."""
        from eventsource import AggregateRepository

        assert AggregateRepository is not None

    def test_import_aggregate_not_found_error(self) -> None:
        """Test importing AggregateNotFoundError."""
        from eventsource import AggregateNotFoundError

        assert AggregateNotFoundError is not None


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_large_number_of_events(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Repository should handle aggregates with many events."""
        aggregate = counter_repository.create_new(uuid4())

        for _ in range(100):
            aggregate.increment(1)

        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(aggregate.aggregate_id)
        assert loaded.version == 100
        assert loaded.state.value == 100

    @pytest.mark.asyncio
    async def test_save_batch_of_events(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """save() should handle multiple uncommitted events correctly."""
        aggregate = counter_repository.create_new(uuid4())

        # Add many events before saving
        for _ in range(50):
            aggregate.increment(1)

        # Single save with all events
        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(aggregate.aggregate_id)
        assert loaded.version == 50
        assert loaded.state.value == 50

    @pytest.mark.asyncio
    async def test_aggregate_id_type_preserved(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Aggregate ID should maintain its UUID type through save/load cycle."""
        original_id = uuid4()
        aggregate = counter_repository.create_new(original_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(original_id)

        assert isinstance(loaded.aggregate_id, UUID)
        assert loaded.aggregate_id == original_id
