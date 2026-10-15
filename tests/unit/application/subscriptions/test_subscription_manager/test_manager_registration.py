"""Tests for subscription registration, lookup, and unregistration in SubscriptionManager."""

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionAlreadyExistsError,
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)
from eventsource.application.subscriptions.registry import SubscriptionRegistry
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
        self, manager: SubscriptionManager, subscriber: MockSubscriber
    ):
        """Test subscribe() creates and returns a subscription."""
        subscription = await manager.subscribe(subscriber)

        assert subscription is not None
        assert subscription.name == "MockSubscriber"
        assert subscription.state == SubscriptionState.STARTING
        assert manager.subscription_count == 1

    @pytest.mark.asyncio
    async def test_subscribe_with_custom_name(
        self, manager: SubscriptionManager, subscriber: MockSubscriber
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
        self, manager: SubscriptionManager, subscriber: MockSubscriber
    ):
        """Test subscribe() raises error for duplicate name."""
        await manager.subscribe(subscriber)

        with pytest.raises(SubscriptionAlreadyExistsError) as exc_info:
            await manager.subscribe(subscriber)

        assert "MockSubscriber" in str(exc_info.value)
        assert manager.subscription_count == 1

    @pytest.mark.asyncio
    async def test_subscribe_multiple_subscribers(self, manager: SubscriptionManager):
        """Test subscribing multiple subscribers."""
        sub1 = await manager.subscribe(OrderProjection())
        sub2 = await manager.subscribe(CustomerProjection())

        assert sub1.name == "OrderProjection"
        assert sub2.name == "CustomerProjection"
        assert manager.subscription_count == 2
        assert set(manager.subscription_names) == {"OrderProjection", "CustomerProjection"}


class TestSubscriptionRemoval:
    """Tests for subscription removal."""

    @pytest.mark.asyncio
    async def test_unsubscribe_existing(
        self, manager: SubscriptionManager, subscriber: MockSubscriber
    ):
        """Test unsubscribe removes subscription."""
        await manager.subscribe(subscriber)
        assert manager.subscription_count == 1

        result = await manager.unsubscribe("MockSubscriber")

        assert result is True
        assert manager.subscription_count == 0
        assert manager.get_subscription("MockSubscriber") is None

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent(self, manager: SubscriptionManager):
        """Test unsubscribe returns False for nonexistent subscription."""
        assert await manager.unsubscribe("NonExistent") is False

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
        assert await manager.unsubscribe("MockSubscriber") is True
        assert manager.subscription_count == 0


class TestSubscriptionLookup:
    """Tests for subscription lookup methods."""

    @pytest.mark.asyncio
    async def test_get_subscription_existing(
        self, manager: SubscriptionManager, subscriber: MockSubscriber
    ):
        """Test get_subscription returns existing subscription."""
        expected = await manager.subscribe(subscriber)
        assert manager.get_subscription("MockSubscriber") is expected

    @pytest.mark.asyncio
    async def test_get_subscription_nonexistent(self, manager: SubscriptionManager):
        """Test get_subscription returns None for unknown name."""
        assert manager.get_subscription("NonExistent") is None

    @pytest.mark.asyncio
    async def test_subscriptions_property(self, manager: SubscriptionManager):
        """Test subscriptions property returns all subscriptions."""
        sub1 = await manager.subscribe(OrderProjection())
        sub2 = await manager.subscribe(CustomerProjection())

        subscriptions = manager.subscriptions
        assert len(subscriptions) == 2
        assert sub1 in subscriptions and sub2 in subscriptions

    @pytest.mark.asyncio
    async def test_subscription_names_property(self, manager: SubscriptionManager):
        """Test subscription_names property returns all names."""
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())

        assert set(manager.subscription_names) == {"OrderProjection", "CustomerProjection"}

    @pytest.mark.asyncio
    async def test_registry_bool_and_contains_dunders(self, subscriber: MockSubscriber):
        """Test SubscriptionRegistry __bool__, contains, and __contains__."""
        registry = SubscriptionRegistry()
        assert bool(registry) is True
        assert len(registry) == 0

        sub = await registry.register(subscriber)
        other_sub = Subscription(
            name="MockSubscriber", config=SubscriptionConfig(), subscriber=OrderProjection()
        )
        assert "MockSubscriber" in registry and "Unknown" not in registry
        assert sub in registry and other_sub not in registry
        assert subscriber in registry and OrderProjection() not in registry
        assert registry.contains("MockSubscriber") is True
        assert registry.contains(sub) is True
        assert registry.contains(other_sub) is False
        assert registry.contains(subscriber) is True
