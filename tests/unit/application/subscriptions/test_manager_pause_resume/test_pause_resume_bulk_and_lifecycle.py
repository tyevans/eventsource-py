"""Tests for bulk pause/resume operations, lifecycle, and health status."""

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


class TestManagerPauseAll:
    """Tests for SubscriptionManager.pause_all()."""

    @pytest.mark.asyncio
    async def test_pause_all_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        results = await manager.pause_all()
        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is True
        assert manager.get_subscription("OrderProjection").state == SubscriptionState.PAUSED
        assert manager.get_subscription("CustomerProjection").state == SubscriptionState.PAUSED
        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_all_with_reason(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_all(reason=PauseReason.BACKPRESSURE)
        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.pause_reason == PauseReason.BACKPRESSURE
        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_all_skips_non_running(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start(subscription_names=["OrderProjection"])

        results = await manager.pause_all()
        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_all_empty_manager(self, manager: SubscriptionManager):
        results = await manager.pause_all()
        assert results == {}


class TestManagerResumeAll:
    """Tests for SubscriptionManager.resume_all()."""

    @pytest.mark.asyncio
    async def test_resume_all_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()
        await manager.pause_all()

        results = await manager.resume_all()
        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is True
        assert manager.get_subscription("OrderProjection").state == SubscriptionState.LIVE
        assert manager.get_subscription("CustomerProjection").state == SubscriptionState.LIVE
        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_all_skips_non_paused(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()
        await manager.pause_subscription("OrderProjection")

        results = await manager.resume_all()
        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_all_empty_manager(self, manager: SubscriptionManager):
        results = await manager.resume_all()
        assert results == {}


class TestPauseResumeLifecycle:
    """Tests for pause/resume lifecycle scenarios."""

    @pytest.mark.asyncio
    async def test_multiple_pause_resume_cycles(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        for _ in range(5):
            await manager.pause_subscription("MockSubscriber")
            subscription = manager.get_subscription("MockSubscriber")
            assert subscription.state == SubscriptionState.PAUSED
            await manager.resume_subscription("MockSubscriber")
            assert subscription.state == SubscriptionState.LIVE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_does_not_affect_other_subscriptions(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        order_projection = OrderProjection()
        customer_projection = CustomerProjection()
        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()
        await manager.pause_subscription("OrderProjection")

        assert manager.get_subscription("OrderProjection").state == SubscriptionState.PAUSED
        assert manager.get_subscription("CustomerProjection").state == SubscriptionState.LIVE
        await manager.stop()

    @pytest.mark.asyncio
    async def test_stop_while_paused(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()
        await manager.pause_subscription("MockSubscriber")
        await manager.stop()

        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.STOPPED


class TestHealthStatusWithPaused:
    """Tests for health status when subscriptions are paused."""

    @pytest.mark.asyncio
    async def test_health_shows_paused_state(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()
        await manager.pause_subscription("MockSubscriber")

        health = manager.get_health()
        assert health["subscriptions"][0]["state"] == "paused"
        await manager.stop()

    @pytest.mark.asyncio
    async def test_subscription_status_shows_paused(
        self, manager: SubscriptionManager, event_store: InMemoryEventStore
    ):
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()
        await manager.pause_subscription("MockSubscriber")

        statuses = manager.get_all_statuses()
        assert statuses["MockSubscriber"].state == "paused"
        await manager.stop()
