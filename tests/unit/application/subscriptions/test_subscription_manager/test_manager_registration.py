"""Tests for subscription registration, lookup, and unregistration in SubscriptionManager."""

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    SubscriptionAlreadyExistsError,
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
