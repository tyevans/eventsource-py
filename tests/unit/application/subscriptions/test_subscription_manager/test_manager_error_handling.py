"""Tests for error handling, subscription isolation, and edge cases in SubscriptionManager."""

import pytest

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


class TestErrorHandling:
    """Tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_start_with_failing_subscriber(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test start handles subscriber failure with isolation."""
        await add_events_to_store(event_store, 5)
        subscriber = MockSubscriber()
        subscriber.fail_on_event_types.add("ManagerTestEvent")
        config = SubscriptionConfig(continue_on_error=False)
        await manager.subscribe(subscriber, config=config)

        results = await manager.start()

        assert manager.is_running is True
        assert results["MockSubscriber"] is not None
        assert isinstance(results["MockSubscriber"], Exception)
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

        assert manager.is_running is True
        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.events_failed == 5
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
        working_subscriber = OrderProjection()
        failing_subscriber = MockSubscriber()
        failing_subscriber.fail_on_event_types.add("ManagerTestEvent")

        await manager.subscribe(working_subscriber)
        await manager.subscribe(
            failing_subscriber,
            config=SubscriptionConfig(continue_on_error=False),
            name="FailingSubscriber",
        )

        results = await manager.start()

        assert manager.is_running is True
        assert len(working_subscriber.handled_events) == 5
        assert results["OrderProjection"] is None
        assert results["FailingSubscriber"] is not None
        order_sub = manager.get_subscription("OrderProjection")
        assert order_sub.state == SubscriptionState.LIVE
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
        working = OrderProjection()
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

        assert results["OrderProjection"] is None
        assert len(working.handled_events) == 5
        assert results["Failing1"] is not None
        assert results["Failing2"] is not None
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

        subscription = manager.get_subscription("FailingSubscriber")
        assert subscription is not None
        assert subscription.state == SubscriptionState.ERROR
        assert subscription.last_error is not None
        assert "Intentional failure" in str(subscription.last_error)
        await manager.stop()


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
        await manager.stop()
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

        subscriber2 = CustomerProjection()
        subscription = await manager.subscribe(subscriber2)
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
