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
        manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
        assert manager.event_store is event_store
        assert manager.event_bus is event_bus
        assert manager.checkpoint_repo is checkpoint_repo
        assert manager.is_running is False
        assert manager.subscription_count == 0
        assert manager.subscriptions == []
        assert manager.subscription_names == []

    def test_manager_starts_not_running(self, manager: SubscriptionManager):
        assert manager.is_running is False


class TestManagerStart:
    """Tests for starting the subscription manager."""

    @pytest.mark.asyncio
    async def test_start_all_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        assert manager.is_running is True
        assert len(order_projection.handled_events) == 5
        assert len(customer_projection.handled_events) == 5

    @pytest.mark.asyncio
    async def test_start_specific_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start(subscription_names=["OrderProjection"])

        assert manager.is_running is True
        assert len(order_projection.handled_events) == 5
        assert len(customer_projection.handled_events) == 0

    @pytest.mark.asyncio
    async def test_start_empty_manager(self, manager: SubscriptionManager):
        await manager.start()
        assert manager.is_running is True

    @pytest.mark.asyncio
    async def test_start_when_already_running(
        self, manager: SubscriptionManager, subscriber: MockSubscriber
    ):
        await manager.subscribe(subscriber)
        await manager.start()
        assert manager.is_running is True
        await manager.start()
        assert manager.is_running is True

    @pytest.mark.asyncio
    async def test_start_processes_events_from_beginning(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 10)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber, config=SubscriptionConfig(start_from="beginning"))
        await manager.start()
        assert len(subscriber.handled_events) == 10


class TestManagerStop:
    """Tests for stopping the subscription manager."""

    @pytest.mark.asyncio
    async def test_stop_all_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start()
        await manager.stop()

        assert manager.is_running is False
        assert manager.get_subscription("OrderProjection").state == SubscriptionState.STOPPED
        assert manager.get_subscription("CustomerProjection").state == SubscriptionState.STOPPED

    @pytest.mark.asyncio
    async def test_stop_specific_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start()
        await manager.stop(subscription_names=["OrderProjection"])

        assert manager.is_running is True
        assert manager.get_subscription("OrderProjection").state == SubscriptionState.STOPPED
        assert manager.get_subscription("CustomerProjection").state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, manager: SubscriptionManager):
        await manager.stop()
        assert manager.is_running is False

    @pytest.mark.asyncio
    async def test_stop_with_timeout(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
        subscriber: MockSubscriber,
    ):
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
