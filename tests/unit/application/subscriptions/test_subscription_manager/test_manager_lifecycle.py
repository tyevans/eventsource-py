"""Tests for SubscriptionManager lifecycle: initialization, start, stop, context manager."""

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)
from tests.unit.application.subscriptions.test_subscription_manager._fixtures import (
    CustomerProjection,
    MockSubscriber,
    OrderProjection,
    add_events_to_store,
)


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
