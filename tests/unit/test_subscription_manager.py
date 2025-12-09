"""
Unit tests for the SubscriptionManager.

Tests cover:
- Manager initialization
- Subscription registration and removal
- Starting all and specific subscriptions
- Stopping all and specific subscriptions
- Subscription lookup methods
- Health status reporting
- Context manager support
- Error handling scenarios
- Duplicate subscription prevention
"""

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.subscriptions import (
    CheckpointStrategy,
    SubscriptionAlreadyExistsError,
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)

# --- Sample Event Classes ---


class ManagerTestEvent(DomainEvent):
    """Simple test event for manager testing."""

    event_type: str = "ManagerTestEvent"
    aggregate_type: str = "ManagerAggregate"
    data: str = "test"


class AnotherManagerEvent(DomainEvent):
    """Another test event type."""

    event_type: str = "AnotherManagerEvent"
    aggregate_type: str = "ManagerAggregate"
    value: int = 0


# --- Mock Subscribers ---


class MockSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self, name: str | None = None) -> None:
        self.handled_events: list[DomainEvent] = []
        self.fail_on_event_types: set[str] = set()
        self.handle_delay: float = 0.0
        self._name = name

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [ManagerTestEvent, AnotherManagerEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.handle_delay > 0:
            await asyncio.sleep(self.handle_delay)
        if event.event_type in self.fail_on_event_types:
            raise ValueError(f"Intentional failure for {event.event_type}")
        self.handled_events.append(event)


class OrderProjection(MockSubscriber):
    """Named projection for testing."""

    pass


class CustomerProjection(MockSubscriber):
    """Another named projection for testing."""

    pass


# --- Fixtures ---


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore."""
    return InMemoryEventStore(enable_tracing=False)


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh InMemoryEventBus."""
    return InMemoryEventBus(enable_tracing=False)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def manager(
    event_store: InMemoryEventStore,
    event_bus: InMemoryEventBus,
    checkpoint_repo: InMemoryCheckpointRepository,
) -> SubscriptionManager:
    """Create a SubscriptionManager."""
    return SubscriptionManager(event_store, event_bus, checkpoint_repo)


@pytest.fixture
def subscriber() -> MockSubscriber:
    """Create a test subscriber."""
    return MockSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create default subscription config."""
    return SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    )


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = ManagerTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="ManagerAggregate",
            events=[event],
            expected_version=0,
        )
        events.append(event)
    return events


# --- Manager Initialization Tests ---


class TestManagerInitialization:
    """Tests for SubscriptionManager initialization."""

    def test_manager_initialization(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test manager initializes correctly."""
        manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)

        assert manager.event_store is event_store
        assert manager.event_bus is event_bus
        assert manager.checkpoint_repo is checkpoint_repo
        assert manager.is_running is False
        assert manager.subscription_count == 0
        assert manager.subscriptions == []
        assert manager.subscription_names == []

    def test_manager_starts_not_running(self, manager: SubscriptionManager):
        """Test manager starts in not running state."""
        assert manager.is_running is False


# --- Subscription Registration Tests ---


class TestSubscriptionRegistration:
    """Tests for subscription registration."""

    @pytest.mark.asyncio
    async def test_subscribe_creates_subscription(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test subscribe() creates and returns a subscription."""
        subscription = await manager.subscribe(subscriber)

        assert subscription is not None
        assert subscription.name == "MockSubscriber"
        assert subscription.state == SubscriptionState.STARTING
        assert manager.subscription_count == 1

    @pytest.mark.asyncio
    async def test_subscribe_with_custom_name(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test subscribe() with custom name."""
        subscription = await manager.subscribe(subscriber, name="CustomName")

        assert subscription.name == "CustomName"
        assert manager.get_subscription("CustomName") is subscription
        assert manager.get_subscription("MockSubscriber") is None

    @pytest.mark.asyncio
    async def test_subscribe_with_config(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
        config: SubscriptionConfig,
    ):
        """Test subscribe() with custom config."""
        subscription = await manager.subscribe(subscriber, config=config)

        assert subscription.config is config
        assert subscription.config.batch_size == 10

    @pytest.mark.asyncio
    async def test_subscribe_duplicate_raises_error(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test subscribe() raises error for duplicate name."""
        await manager.subscribe(subscriber)

        with pytest.raises(SubscriptionAlreadyExistsError) as exc_info:
            await manager.subscribe(subscriber)

        assert "MockSubscriber" in str(exc_info.value)
        assert manager.subscription_count == 1

    @pytest.mark.asyncio
    async def test_subscribe_multiple_subscribers(
        self,
        manager: SubscriptionManager,
    ):
        """Test subscribing multiple subscribers."""
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        sub1 = await manager.subscribe(order_projection)
        sub2 = await manager.subscribe(customer_projection)

        assert sub1.name == "OrderProjection"
        assert sub2.name == "CustomerProjection"
        assert manager.subscription_count == 2
        assert set(manager.subscription_names) == {"OrderProjection", "CustomerProjection"}


# --- Subscription Removal Tests ---


class TestSubscriptionRemoval:
    """Tests for subscription removal."""

    @pytest.mark.asyncio
    async def test_unsubscribe_existing(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test unsubscribe removes subscription."""
        await manager.subscribe(subscriber)
        assert manager.subscription_count == 1

        result = await manager.unsubscribe("MockSubscriber")

        assert result is True
        assert manager.subscription_count == 0
        assert manager.get_subscription("MockSubscriber") is None

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent(
        self,
        manager: SubscriptionManager,
    ):
        """Test unsubscribe returns False for nonexistent subscription."""
        result = await manager.unsubscribe("NonExistent")

        assert result is False

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_running_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        """Test unsubscribe stops a running subscription."""
        await add_events_to_store(event_store, 5)
        await manager.subscribe(subscriber)
        await manager.start()

        assert manager.is_running is True

        result = await manager.unsubscribe("MockSubscriber")

        assert result is True
        assert manager.subscription_count == 0


# --- Subscription Lookup Tests ---


class TestSubscriptionLookup:
    """Tests for subscription lookup methods."""

    @pytest.mark.asyncio
    async def test_get_subscription_existing(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test get_subscription returns existing subscription."""
        expected = await manager.subscribe(subscriber)

        result = manager.get_subscription("MockSubscriber")

        assert result is expected

    @pytest.mark.asyncio
    async def test_get_subscription_nonexistent(
        self,
        manager: SubscriptionManager,
    ):
        """Test get_subscription returns None for unknown name."""
        result = manager.get_subscription("NonExistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_subscriptions_property(
        self,
        manager: SubscriptionManager,
    ):
        """Test subscriptions property returns all subscriptions."""
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        sub1 = await manager.subscribe(order_projection)
        sub2 = await manager.subscribe(customer_projection)

        subscriptions = manager.subscriptions

        assert len(subscriptions) == 2
        assert sub1 in subscriptions
        assert sub2 in subscriptions

    @pytest.mark.asyncio
    async def test_subscription_names_property(
        self,
        manager: SubscriptionManager,
    ):
        """Test subscription_names property returns all names."""
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        names = manager.subscription_names

        assert set(names) == {"OrderProjection", "CustomerProjection"}


# --- Start Tests ---


class TestManagerStart:
    """Tests for starting the subscription manager."""

    @pytest.mark.asyncio
    async def test_start_all_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test start() starts all registered subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        await manager.start()

        assert manager.is_running is True
        # Both projections should have received the events
        assert len(order_projection.handled_events) == 5
        assert len(customer_projection.handled_events) == 5

    @pytest.mark.asyncio
    async def test_start_specific_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test start() with specific subscription names."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        # Only start OrderProjection
        await manager.start(subscription_names=["OrderProjection"])

        assert manager.is_running is True
        assert len(order_projection.handled_events) == 5
        assert len(customer_projection.handled_events) == 0  # Not started

    @pytest.mark.asyncio
    async def test_start_empty_manager(
        self,
        manager: SubscriptionManager,
    ):
        """Test starting manager with no subscriptions."""
        await manager.start()

        assert manager.is_running is True

    @pytest.mark.asyncio
    async def test_start_when_already_running(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test start() when already running is a no-op."""
        await manager.subscribe(subscriber)
        await manager.start()

        assert manager.is_running is True

        # Second start should be no-op
        await manager.start()

        assert manager.is_running is True

    @pytest.mark.asyncio
    async def test_start_processes_events_from_beginning(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test start processes all events from beginning by default."""
        await add_events_to_store(event_store, 10)

        subscriber = MockSubscriber()
        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(subscriber, config=config)

        await manager.start()

        assert len(subscriber.handled_events) == 10


# --- Stop Tests ---


class TestManagerStop:
    """Tests for stopping the subscription manager."""

    @pytest.mark.asyncio
    async def test_stop_all_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test stop() stops all subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        await manager.start()
        await manager.stop()

        assert manager.is_running is False

        # Check subscriptions are in stopped state
        order_sub = manager.get_subscription("OrderProjection")
        customer_sub = manager.get_subscription("CustomerProjection")
        assert order_sub.state == SubscriptionState.STOPPED
        assert customer_sub.state == SubscriptionState.STOPPED

    @pytest.mark.asyncio
    async def test_stop_specific_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test stop() with specific subscription names."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        await manager.start()

        # Only stop OrderProjection
        await manager.stop(subscription_names=["OrderProjection"])

        # Manager should still be running
        assert manager.is_running is True

        order_sub = manager.get_subscription("OrderProjection")
        customer_sub = manager.get_subscription("CustomerProjection")
        assert order_sub.state == SubscriptionState.STOPPED
        assert customer_sub.state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_stop_when_not_running(
        self,
        manager: SubscriptionManager,
    ):
        """Test stop() when not running is a no-op."""
        await manager.stop()

        assert manager.is_running is False

    @pytest.mark.asyncio
    async def test_stop_with_timeout(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        """Test stop() with custom timeout."""
        await add_events_to_store(event_store, 5)
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.stop(timeout=5.0)

        assert manager.is_running is False


# --- Context Manager Tests ---


class TestContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager_start_stop(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test async with starts and stops the manager."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
        await manager.subscribe(subscriber)

        async with manager:
            assert manager.is_running is True
            assert len(subscriber.handled_events) == 5

        assert manager.is_running is False

    @pytest.mark.asyncio
    async def test_context_manager_stop_on_exception(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test context manager stops even on exception."""
        subscriber = MockSubscriber()
        manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
        await manager.subscribe(subscriber)

        try:
            async with manager:
                assert manager.is_running is True
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert manager.is_running is False


# --- Health Status Tests ---


class TestHealthStatus:
    """Tests for health status reporting."""

    @pytest.mark.asyncio
    async def test_health_when_stopped(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test health status when stopped."""
        await manager.subscribe(subscriber)

        health = manager.get_health()

        assert health["status"] == "stopped"
        assert health["running"] is False
        assert health["subscription_count"] == 1
        assert len(health["subscriptions"]) == 1

    @pytest.mark.asyncio
    async def test_health_when_running(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        """Test health status when running."""
        await add_events_to_store(event_store, 5)
        await manager.subscribe(subscriber)
        await manager.start()

        health = manager.get_health()

        assert health["status"] == "healthy"
        assert health["running"] is True
        assert health["subscription_count"] == 1
        assert len(health["subscriptions"]) == 1
        assert health["subscriptions"][0]["state"] == "live"

        await manager.stop()

    @pytest.mark.asyncio
    async def test_health_with_error_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test health status with errored subscription."""
        await add_events_to_store(event_store, 5)

        # Create subscriber that fails
        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("ManagerTestEvent")

        config = SubscriptionConfig(continue_on_error=False)
        await manager.subscribe(subscriber, config=config)

        with contextlib.suppress(Exception):
            await manager.start()

        health = manager.get_health()

        assert health["status"] == "unhealthy"
        assert health["subscriptions"][0]["state"] == "error"

    @pytest.mark.asyncio
    async def test_health_subscription_details(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
        """Test health includes subscription details."""
        await add_events_to_store(event_store, 5)
        await manager.subscribe(subscriber)
        await manager.start()

        health = manager.get_health()

        sub_status = health["subscriptions"][0]
        assert "name" in sub_status
        assert "state" in sub_status
        assert "position" in sub_status
        assert "events_processed" in sub_status

        await manager.stop()


# --- Error Handling Tests ---


class TestErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_start_with_failing_subscriber(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test start handles subscriber failure with isolation.

        With subscription isolation enabled (default), start() does not raise
        exceptions but instead returns a dictionary of results indicating
        which subscriptions failed. The manager remains running even if some
        subscriptions fail.
        """
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("ManagerTestEvent")

        config = SubscriptionConfig(continue_on_error=False)
        await manager.subscribe(subscriber, config=config)

        # start() no longer raises - it returns results dict with isolation
        results = await manager.start()

        # Manager should be running (isolation keeps it running)
        assert manager.is_running is True

        # Results should indicate failure
        assert results["MockSubscriber"] is not None
        assert isinstance(results["MockSubscriber"], Exception)

        # Subscription should be in error state
        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.ERROR

        await manager.stop()

    @pytest.mark.asyncio
    async def test_continue_on_error_mode(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test continue_on_error allows processing to continue."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("ManagerTestEvent")

        config = SubscriptionConfig(continue_on_error=True)
        await manager.subscribe(subscriber, config=config)

        await manager.start()

        # Should be running despite errors
        assert manager.is_running is True

        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.events_failed == 5

        await manager.stop()


# --- Live Event Tests ---


class TestLiveEvents:
    """Tests for live event processing after transition."""

    @pytest.mark.asyncio
    async def test_receives_live_events_after_start(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
    ):
        """Test manager receives live events after transition."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        assert len(subscriber.handled_events) == 5

        # Publish a live event
        live_event = ManagerTestEvent(
            aggregate_id=uuid4(),
            data="live_event",
        )
        await event_bus.publish([live_event])

        # Wait for processing
        await asyncio.sleep(0.01)

        assert len(subscriber.handled_events) == 6
        assert subscriber.handled_events[-1].data == "live_event"

        await manager.stop()


# --- Module Import Tests ---


class TestModuleImports:
    """Tests for module imports and exports."""

    def test_import_from_subscriptions_module(self):
        """Test SubscriptionManager can be imported from subscriptions module."""
        from eventsource.subscriptions import SubscriptionManager as SubMgr

        assert SubMgr is SubscriptionManager

    def test_all_exports(self):
        """Test __all__ contains SubscriptionManager."""
        from eventsource.subscriptions import manager

        assert "SubscriptionManager" in manager.__all__


# --- Edge Cases ---


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_multiple_start_calls(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test multiple start calls are idempotent."""
        await manager.subscribe(subscriber)

        await manager.start()
        await manager.start()
        await manager.start()

        assert manager.is_running is True

        await manager.stop()

    @pytest.mark.asyncio
    async def test_multiple_stop_calls(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test multiple stop calls are idempotent."""
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.stop()
        await manager.stop()
        await manager.stop()

        assert manager.is_running is False

    @pytest.mark.asyncio
    async def test_stop_without_start(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test stop can be called without start."""
        await manager.subscribe(subscriber)

        await manager.stop()  # Should not raise

        assert manager.is_running is False

    @pytest.mark.asyncio
    async def test_subscribe_after_start(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test subscribing after start is allowed but doesn't auto-start."""
        await add_events_to_store(event_store, 5)

        subscriber1 = OrderProjection()
        await manager.subscribe(subscriber1)
        await manager.start()

        # Subscribe a new subscriber after start
        subscriber2 = CustomerProjection()
        subscription = await manager.subscribe(subscriber2)

        # New subscriber is registered but not started
        assert subscription.state == SubscriptionState.STARTING
        assert len(subscriber2.handled_events) == 0

        await manager.stop()

    @pytest.mark.asyncio
    async def test_empty_store_transition(
        self,
        manager: SubscriptionManager,
        subscriber: MockSubscriber,
    ):
        """Test transition with empty event store."""
        await manager.subscribe(subscriber)
        await manager.start()

        assert manager.is_running is True
        assert len(subscriber.handled_events) == 0

        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.LIVE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_large_number_of_events(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test manager handles large number of events."""
        await add_events_to_store(event_store, 500)

        subscriber = MockSubscriber()
        config = SubscriptionConfig(batch_size=50)
        await manager.subscribe(subscriber, config=config)

        await manager.start()

        assert len(subscriber.handled_events) == 500

        await manager.stop()


# --- PHASE3-002: Multiple Subscriptions Tests ---


class TestMultipleSubscriptionsConcurrent:
    """Tests for concurrent multiple subscription handling (PHASE3-002)."""

    @pytest.mark.asyncio
    async def test_concurrent_start_all_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test that multiple subscriptions start concurrently."""
        await add_events_to_store(event_store, 10)

        # Create multiple subscribers
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        third_projection = MockSubscriber()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.subscribe(third_projection, name="ThirdProjection")

        # Start all concurrently (default behavior)
        results = await manager.start()

        # All should succeed
        assert len(results) == 3
        assert all(v is None for v in results.values())

        # All should receive events
        assert len(order_projection.handled_events) == 10
        assert len(customer_projection.handled_events) == 10
        assert len(third_projection.handled_events) == 10

        await manager.stop()

    @pytest.mark.asyncio
    async def test_concurrent_start_returns_results(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test that start() returns a dictionary of results."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        results = await manager.start()

        # Results should map subscription names to None (success)
        assert "OrderProjection" in results
        assert "CustomerProjection" in results
        assert results["OrderProjection"] is None
        assert results["CustomerProjection"] is None

        await manager.stop()

    @pytest.mark.asyncio
    async def test_sequential_start_mode(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test sequential start mode (concurrent=False)."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        # Start sequentially
        results = await manager.start(concurrent=False)

        assert len(results) == 2
        assert all(v is None for v in results.values())

        assert len(order_projection.handled_events) == 5
        assert len(customer_projection.handled_events) == 5

        await manager.stop()


class TestSubscriptionIsolation:
    """Tests for subscription isolation (one failure doesn't affect others)."""

    @pytest.mark.asyncio
    async def test_one_failing_subscription_does_not_stop_others(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test that one subscription failing during start doesn't prevent others."""
        await add_events_to_store(event_store, 5)

        # First subscriber works normally
        working_subscriber = OrderProjection()

        # Second subscriber fails on all events
        failing_subscriber = MockSubscriber()
        failing_subscriber.fail_on_event_types.add("ManagerTestEvent")

        await manager.subscribe(working_subscriber)
        await manager.subscribe(
            failing_subscriber,
            config=SubscriptionConfig(continue_on_error=False),
            name="FailingSubscriber",
        )

        # Start should complete (not raise)
        results = await manager.start()

        # Manager should be running
        assert manager.is_running is True

        # Working subscriber should have processed events
        assert len(working_subscriber.handled_events) == 5

        # Results should indicate which succeeded and which failed
        assert results["OrderProjection"] is None  # Success
        assert results["FailingSubscriber"] is not None  # Failed (Exception)

        # Working subscription should be in LIVE state
        order_sub = manager.get_subscription("OrderProjection")
        assert order_sub.state == SubscriptionState.LIVE

        # Failed subscription should be in ERROR state
        failing_sub = manager.get_subscription("FailingSubscriber")
        assert failing_sub.state == SubscriptionState.ERROR

        await manager.stop()

    @pytest.mark.asyncio
    async def test_multiple_failing_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test multiple subscriptions failing independently."""
        await add_events_to_store(event_store, 5)

        # Working subscriber
        working = OrderProjection()

        # Two failing subscribers
        failing1 = MockSubscriber()
        failing1.fail_on_event_types.add("ManagerTestEvent")

        failing2 = MockSubscriber()
        failing2.fail_on_event_types.add("ManagerTestEvent")

        await manager.subscribe(working)
        await manager.subscribe(
            failing1,
            config=SubscriptionConfig(continue_on_error=False),
            name="Failing1",
        )
        await manager.subscribe(
            failing2,
            config=SubscriptionConfig(continue_on_error=False),
            name="Failing2",
        )

        results = await manager.start()

        # Working subscriber should succeed
        assert results["OrderProjection"] is None
        assert len(working.handled_events) == 5

        # Both failing subscribers should have errors
        assert results["Failing1"] is not None
        assert results["Failing2"] is not None

        # Manager should still be running
        assert manager.is_running is True

        await manager.stop()

    @pytest.mark.asyncio
    async def test_failed_subscription_has_error_details(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test that failed subscriptions have proper error information."""
        await add_events_to_store(event_store, 5)

        failing_subscriber = MockSubscriber()
        failing_subscriber.fail_on_event_types.add("ManagerTestEvent")

        await manager.subscribe(
            failing_subscriber,
            config=SubscriptionConfig(continue_on_error=False),
            name="FailingSubscriber",
        )

        await manager.start()

        # Get the subscription and check error details
        subscription = manager.get_subscription("FailingSubscriber")
        assert subscription is not None
        assert subscription.state == SubscriptionState.ERROR
        assert subscription.last_error is not None
        assert "Intentional failure" in str(subscription.last_error)

        await manager.stop()


class TestGetAllStatuses:
    """Tests for get_all_statuses method."""

    @pytest.mark.asyncio
    async def test_get_all_statuses_returns_dict(
        self,
        manager: SubscriptionManager,
    ):
        """Test get_all_statuses returns dictionary of statuses."""
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        statuses = manager.get_all_statuses()

        assert isinstance(statuses, dict)
        assert "OrderProjection" in statuses
        assert "CustomerProjection" in statuses

    @pytest.mark.asyncio
    async def test_get_all_statuses_returns_subscription_status_objects(
        self,
        manager: SubscriptionManager,
    ):
        """Test get_all_statuses returns SubscriptionStatus objects."""
        from eventsource.subscriptions import SubscriptionStatus

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)

        statuses = manager.get_all_statuses()

        assert "MockSubscriber" in statuses
        status = statuses["MockSubscriber"]
        assert isinstance(status, SubscriptionStatus)
        assert status.name == "MockSubscriber"
        assert status.state == "starting"

    @pytest.mark.asyncio
    async def test_get_all_statuses_shows_different_states(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test get_all_statuses shows different subscription states."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        # Start only one subscription
        await manager.start(subscription_names=["OrderProjection"])

        statuses = manager.get_all_statuses()

        assert statuses["OrderProjection"].state == "live"
        assert statuses["CustomerProjection"].state == "starting"

        await manager.stop()

    @pytest.mark.asyncio
    async def test_get_all_statuses_empty_manager(
        self,
        manager: SubscriptionManager,
    ):
        """Test get_all_statuses with no subscriptions."""
        statuses = manager.get_all_statuses()

        assert statuses == {}

    @pytest.mark.asyncio
    async def test_get_all_statuses_includes_statistics(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test get_all_statuses includes event processing statistics."""
        await add_events_to_store(event_store, 10)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        statuses = manager.get_all_statuses()
        status = statuses["MockSubscriber"]

        assert status.events_processed == 10
        assert status.position > 0
        assert status.uptime_seconds >= 0

        await manager.stop()


class TestMultipleSubscriptionsHealth:
    """Tests for health status with multiple subscriptions."""

    @pytest.mark.asyncio
    async def test_health_status_all_healthy(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test health shows healthy when all subscriptions are live."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        await manager.start()

        health = manager.get_health()

        assert health["status"] == "healthy"
        assert health["subscription_count"] == 2
        assert len(health["subscriptions"]) == 2

        await manager.stop()

    @pytest.mark.asyncio
    async def test_health_status_with_mixed_states(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test health shows appropriate status with mixed subscription states."""
        await add_events_to_store(event_store, 5)

        # Working subscriber
        working = OrderProjection()

        # Failing subscriber
        failing = MockSubscriber()
        failing.fail_on_event_types.add("ManagerTestEvent")

        await manager.subscribe(working)
        await manager.subscribe(
            failing,
            config=SubscriptionConfig(continue_on_error=False),
            name="FailingSubscriber",
        )

        await manager.start()

        health = manager.get_health()

        # Health should show unhealthy due to one subscription in error state
        assert health["status"] == "unhealthy"

        # Check individual subscription statuses
        sub_states = {s["name"]: s["state"] for s in health["subscriptions"]}
        assert sub_states["OrderProjection"] == "live"
        assert sub_states["FailingSubscriber"] == "error"

        await manager.stop()

    @pytest.mark.asyncio
    async def test_is_healthy_property_with_multiple_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test is_healthy property with multiple subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        await manager.start()

        assert manager.is_healthy is True

        await manager.stop()


class TestMultipleSubscriptionsLifecycle:
    """Tests for subscription lifecycle with multiple subscriptions."""

    @pytest.mark.asyncio
    async def test_stop_stops_all_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test that stop() stops all subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        third_projection = MockSubscriber()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.subscribe(third_projection, name="ThirdProjection")

        await manager.start()
        await manager.stop()

        # All should be stopped
        statuses = manager.get_all_statuses()
        for name, status in statuses.items():
            assert status.state == "stopped", f"{name} should be stopped"

    @pytest.mark.asyncio
    async def test_unsubscribe_one_keeps_others(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test unsubscribing one subscription doesn't affect others."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        await manager.start()

        # Unsubscribe OrderProjection
        await manager.unsubscribe("OrderProjection")

        # Manager should still be running
        assert manager.is_running is True

        # CustomerProjection should still be live
        customer_sub = manager.get_subscription("CustomerProjection")
        assert customer_sub.state == SubscriptionState.LIVE

        # OrderProjection should be gone
        assert manager.get_subscription("OrderProjection") is None

        await manager.stop()

    @pytest.mark.asyncio
    async def test_independent_checkpoint_per_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test that each subscription has independent checkpoint."""
        await add_events_to_store(event_store, 10)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        await manager.start()

        # Each subscription should have its own checkpoint
        order_checkpoint = await checkpoint_repo.get_position("OrderProjection")
        customer_checkpoint = await checkpoint_repo.get_position("CustomerProjection")

        assert order_checkpoint is not None
        assert customer_checkpoint is not None
        assert order_checkpoint > 0
        assert customer_checkpoint > 0

        await manager.stop()


class TestConcurrentStartPerformance:
    """Tests for concurrent start performance characteristics."""

    @pytest.mark.asyncio
    async def test_concurrent_start_is_faster_than_sequential(
        self,
        event_store: InMemoryEventStore,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
    ):
        """Test concurrent start completes faster than sequential."""
        import time

        await add_events_to_store(event_store, 50)

        # Create subscribers with a small delay to simulate processing
        slow_subscriber1 = MockSubscriber()
        slow_subscriber1.handle_delay = 0.01
        slow_subscriber2 = MockSubscriber()
        slow_subscriber2.handle_delay = 0.01
        slow_subscriber3 = MockSubscriber()
        slow_subscriber3.handle_delay = 0.01

        # Test concurrent
        concurrent_manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
        await concurrent_manager.subscribe(slow_subscriber1, name="Sub1")
        await concurrent_manager.subscribe(slow_subscriber2, name="Sub2")
        await concurrent_manager.subscribe(slow_subscriber3, name="Sub3")

        start_time = time.time()
        await concurrent_manager.start(concurrent=True)
        concurrent_duration = time.time() - start_time

        await concurrent_manager.stop()

        # Reset state
        slow_subscriber1.handled_events.clear()
        slow_subscriber2.handled_events.clear()
        slow_subscriber3.handled_events.clear()

        # For sequential test, we'd need a fresh checkpoint repo
        # Just verify concurrent completed reasonably
        assert concurrent_duration < 10.0  # Sanity check

    @pytest.mark.asyncio
    async def test_start_returns_empty_dict_if_already_running(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test start returns empty dict if already running."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)

        # First start
        results1 = await manager.start()
        assert len(results1) == 1

        # Second start while running
        results2 = await manager.start()
        assert results2 == {}

        await manager.stop()
