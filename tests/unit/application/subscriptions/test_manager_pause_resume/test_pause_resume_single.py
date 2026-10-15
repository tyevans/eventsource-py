"""Tests for individual subscription pause/resume operations and properties."""

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    PauseReason,
    SubscriptionManager,
    SubscriptionState,
)
from tests.unit.application.subscriptions.test_manager_pause_resume._fixtures import (
    CustomerProjection,
    MockSubscriber,
    OrderProjection,
    add_events_to_store,
)


class TestManagerPauseSubscription:
    """Tests for SubscriptionManager.pause_subscription()."""

    @pytest.mark.asyncio
    async def test_pause_running_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.LIVE

        result = await manager.pause_subscription("MockSubscriber")
        assert result is True
        assert subscription.state == SubscriptionState.PAUSED
        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_with_reason(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        result = await manager.pause_subscription(
            "MockSubscriber",
            reason=PauseReason.MAINTENANCE,
        )
        subscription = manager.get_subscription("MockSubscriber")
        assert result is True
        assert subscription.pause_reason == PauseReason.MAINTENANCE
        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_nonexistent_subscription_returns_false(
        self,
        manager: SubscriptionManager,
    ):
        result = await manager.pause_subscription("NonExistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_pause_not_running_subscription_returns_false(
        self,
        manager: SubscriptionManager,
    ):
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        result = await manager.pause_subscription("MockSubscriber")
        assert result is False

    @pytest.mark.asyncio
    async def test_pause_already_paused_returns_false(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_subscription("MockSubscriber")
        result = await manager.pause_subscription("MockSubscriber")
        assert result is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_maintains_position(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 10)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        subscription = manager.get_subscription("MockSubscriber")
        position_before = subscription.last_processed_position
        await manager.pause_subscription("MockSubscriber")
        assert subscription.last_processed_position == position_before
        await manager.stop()


class TestManagerResumeSubscription:
    """Tests for SubscriptionManager.resume_subscription()."""

    @pytest.mark.asyncio
    async def test_resume_paused_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_subscription("MockSubscriber")
        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.PAUSED

        result = await manager.resume_subscription("MockSubscriber")
        assert result is True
        assert subscription.state == SubscriptionState.LIVE
        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_nonexistent_subscription_returns_false(
        self,
        manager: SubscriptionManager,
    ):
        result = await manager.resume_subscription("NonExistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_resume_not_paused_subscription_returns_false(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        result = await manager.resume_subscription("MockSubscriber")
        assert result is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_continues_from_position(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 10)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        subscription = manager.get_subscription("MockSubscriber")
        position_before = subscription.last_processed_position

        await manager.pause_subscription("MockSubscriber")
        await manager.resume_subscription("MockSubscriber")
        assert subscription.last_processed_position >= position_before
        await manager.stop()


class TestPausedSubscriptionsProperties:
    """Tests for paused_subscriptions and paused_subscription_names properties."""

    @pytest.mark.asyncio
    async def test_paused_subscriptions_property(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start()

        await manager.pause_subscription("OrderProjection")
        paused = manager.paused_subscriptions
        assert len(paused) == 1
        assert paused[0].name == "OrderProjection"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_paused_subscriptions_empty_when_none_paused(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(MockSubscriber())
        await manager.start()

        assert len(manager.paused_subscriptions) == 0
        await manager.stop()

    @pytest.mark.asyncio
    async def test_paused_subscription_names_property(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        await add_events_to_store(event_store, 5)
        await manager.subscribe(OrderProjection())
        await manager.subscribe(CustomerProjection())
        await manager.start()

        await manager.pause_all()
        assert set(manager.paused_subscription_names) == {"OrderProjection", "CustomerProjection"}
        await manager.stop()
