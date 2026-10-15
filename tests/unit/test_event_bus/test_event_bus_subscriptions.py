"""Unit tests for event bus subscriptions, management, and lifecycle."""

from __future__ import annotations

import threading
import time
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from tests.unit.test_event_bus._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    RecordingHandler,
    SelectiveSubscriber,
    SlowHandler,
)


class TestInMemoryEventBusWildcardSubscriptions:
    """Tests for wildcard subscription functionality."""

    @pytest.mark.asyncio
    async def test_wildcard_subscription_receives_all_events(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        """Test subscribing to all event types."""
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        await event_bus.publish([sample_order_created, sample_order_shipped])
        assert handler.handle_count == 2

    @pytest.mark.asyncio
    async def test_wildcard_and_specific_subscription(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that wildcard and specific subscriptions both receive events."""
        wildcard_handler, specific_handler = RecordingHandler(), RecordingHandler()
        event_bus.subscribe_to_all_events(wildcard_handler)
        event_bus.subscribe(OrderCreated, specific_handler)
        await event_bus.publish([sample_order_created])
        # Both should receive the event
        assert wildcard_handler.handle_count == 1
        assert specific_handler.handle_count == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_from_all_events(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test unsubscribing from wildcard subscription."""
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        # Unsubscribe
        result = event_bus.unsubscribe_from_all_events(handler)
        assert result is True
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 0

    def test_unsubscribe_from_all_events_not_found(self, event_bus: InMemoryEventBus) -> None:
        """Test unsubscribing handler that isn't registered."""
        handler = RecordingHandler()
        result = event_bus.unsubscribe_from_all_events(handler)
        assert result is False

    def test_get_wildcard_subscriber_count(self, event_bus: InMemoryEventBus) -> None:
        """Test getting wildcard subscriber count."""
        handler1, handler2 = RecordingHandler(), RecordingHandler()
        assert event_bus.get_wildcard_subscriber_count() == 0
        event_bus.subscribe_to_all_events(handler1)
        assert event_bus.get_wildcard_subscriber_count() == 1
        event_bus.subscribe_to_all_events(handler2)
        assert event_bus.get_wildcard_subscriber_count() == 2


class TestInMemoryEventBusSubscribeAll:
    """Tests for subscribe_all with EventSubscriber."""

    @pytest.mark.asyncio
    async def test_subscribe_all_with_subscriber(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        """Test subscribe_all registers for all declared event types."""
        subscriber = SelectiveSubscriber([OrderCreated, OrderShipped])
        event_bus.subscribe_all(subscriber)
        await event_bus.publish([sample_order_created])
        assert len(subscriber.handled_events) == 1
        await event_bus.publish([sample_order_shipped])
        assert len(subscriber.handled_events) == 2

    @pytest.mark.asyncio
    async def test_subscribe_all_ignores_undeclared_events(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_cancelled: OrderCancelled,
    ) -> None:
        """Test subscribe_all doesn't receive undeclared event types."""
        subscriber = SelectiveSubscriber([OrderCreated])
        event_bus.subscribe_all(subscriber)
        await event_bus.publish([sample_order_created, sample_order_cancelled])
        # Should only have received OrderCreated
        assert len(subscriber.handled_events) == 1
        assert isinstance(subscriber.handled_events[0], OrderCreated)


class TestInMemoryEventBusUnsubscribe:
    """Tests for unsubscribe functionality."""

    @pytest.mark.asyncio
    async def test_unsubscribe_handler(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test unsubscribing a handler."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        # Verify subscribed
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1
        # Unsubscribe
        result = event_bus.unsubscribe(OrderCreated, handler)
        assert result is True
        # Verify unsubscribed
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1  # Should not have increased

    def test_unsubscribe_handler_not_found(self, event_bus: InMemoryEventBus) -> None:
        """Test unsubscribing a handler that isn't registered."""
        handler = RecordingHandler()
        result = event_bus.unsubscribe(OrderCreated, handler)
        assert result is False

    def test_unsubscribe_wrong_event_type(self, event_bus: InMemoryEventBus) -> None:
        """Test unsubscribing from wrong event type."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        result = event_bus.unsubscribe(OrderShipped, handler)
        assert result is False


class TestInMemoryEventBusSubscriberManagement:
    """Tests for subscriber management utilities."""

    def test_clear_subscribers(self, event_bus: InMemoryEventBus) -> None:
        """Test clearing all subscribers."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        event_bus.subscribe_to_all_events(handler)
        assert event_bus.get_subscriber_count(OrderCreated) == 1
        assert event_bus.get_wildcard_subscriber_count() == 1
        event_bus.clear_subscribers()
        assert event_bus.get_subscriber_count(OrderCreated) == 0
        assert event_bus.get_wildcard_subscriber_count() == 0

    def test_get_subscriber_count_specific_type(self, event_bus: InMemoryEventBus) -> None:
        """Test getting subscriber count for specific event type."""
        handler1, handler2 = RecordingHandler(), RecordingHandler()
        assert event_bus.get_subscriber_count(OrderCreated) == 0
        event_bus.subscribe(OrderCreated, handler1)
        assert event_bus.get_subscriber_count(OrderCreated) == 1
        event_bus.subscribe(OrderCreated, handler2)
        assert event_bus.get_subscriber_count(OrderCreated) == 2
        event_bus.subscribe(OrderShipped, handler1)
        assert event_bus.get_subscriber_count(OrderCreated) == 2
        assert event_bus.get_subscriber_count(OrderShipped) == 1

    def test_get_subscriber_count_total(self, event_bus: InMemoryEventBus) -> None:
        """Test getting total subscriber count."""
        handler = RecordingHandler()
        assert event_bus.get_subscriber_count() == 0
        event_bus.subscribe(OrderCreated, handler)
        event_bus.subscribe(OrderShipped, handler)
        event_bus.subscribe(OrderCancelled, handler)
        assert event_bus.get_subscriber_count() == 3


class TestInMemoryEventBusShutdown:
    """Tests for event bus shutdown functionality."""

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_background_tasks(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that shutdown waits for background tasks."""
        slow_handler = SlowHandler(delay=0.2)
        event_bus.subscribe(OrderCreated, slow_handler)
        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_background_task_count() == 1
        await event_bus.shutdown(timeout=1.0)
        assert len(slow_handler.handled_events) == 1
        assert event_bus.get_background_task_count() == 0

    @pytest.mark.asyncio
    async def test_shutdown_with_no_tasks(self, event_bus: InMemoryEventBus) -> None:
        """Test shutdown with no background tasks."""
        # Should complete immediately without error
        await event_bus.shutdown(timeout=1.0)

    @pytest.mark.asyncio
    async def test_shutdown_timeout(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that shutdown times out for long-running tasks."""
        very_slow_handler = SlowHandler(delay=5.0)
        event_bus.subscribe(OrderCreated, very_slow_handler)
        await event_bus.publish([sample_order_created], background=True)
        start = time.time()
        await event_bus.shutdown(timeout=0.1)
        elapsed = time.time() - start
        # Should timeout around 0.1s, not wait 5s
        assert elapsed < 0.5


class TestInMemoryEventBusStatistics:
    """Tests for event bus statistics."""

    @pytest.mark.asyncio
    async def test_events_published_counter(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        """Test that events published counter is accurate."""
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        await event_bus.publish([sample_order_created])
        await event_bus.publish([sample_order_shipped])
        stats = event_bus.get_stats()
        assert stats["events_published"] == 2

    @pytest.mark.asyncio
    async def test_get_stats_returns_copy(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that get_stats returns a copy of statistics."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        await event_bus.publish([sample_order_created])
        stats1 = event_bus.get_stats()
        stats1["events_published"] = 999  # Modify the returned dict
        stats2 = event_bus.get_stats()
        assert stats2["events_published"] == 1  # Original unchanged


class TestInMemoryEventBusThreadSafety:
    """Tests for thread safety of subscription operations."""

    def test_subscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        """Test that subscribe is thread-safe."""
        handlers: list[RecordingHandler] = []

        def subscribe_handler() -> None:
            handler = RecordingHandler()
            handlers.append(handler)
            event_bus.subscribe(OrderCreated, handler)

        threads = [threading.Thread(target=subscribe_handler) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert event_bus.get_subscriber_count(OrderCreated) == 10

    def test_unsubscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        """Test that unsubscribe is thread-safe."""
        handlers = [RecordingHandler() for _ in range(10)]
        for handler in handlers:
            event_bus.subscribe(OrderCreated, handler)
        results: list[bool] = []

        def unsubscribe_handler(h: RecordingHandler) -> None:
            results.append(event_bus.unsubscribe(OrderCreated, h))

        threads = [threading.Thread(target=unsubscribe_handler, args=(h,)) for h in handlers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(results)
        assert event_bus.get_subscriber_count(OrderCreated) == 0


class TestEventOrdering:
    """Tests for event ordering guarantees."""

    @pytest.mark.asyncio
    async def test_events_processed_in_order(self, event_bus: InMemoryEventBus) -> None:
        """Test that events are processed in order."""
        order: list[str] = []

        class OrderTrackingHandler:
            async def handle(self, event: DomainEvent) -> None:
                order.append(event.event_type)

        event_bus.subscribe_to_all_events(OrderTrackingHandler())
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", customer_id="cust-1"),
            OrderShipped(aggregate_id=uuid4(), order_number="ORD-001", tracking_number="TRK-1"),
            OrderCancelled(aggregate_id=uuid4(), order_number="ORD-001", reason="Test"),
        ]
        await event_bus.publish(events)
        assert order == ["OrderCreated", "OrderShipped", "OrderCancelled"]


__all__ = [
    "TestEventOrdering",
    "TestInMemoryEventBusShutdown",  # pragma: allowlist secret
    "TestInMemoryEventBusStatistics",  # pragma: allowlist secret
    "TestInMemoryEventBusSubscribeAll",  # pragma: allowlist secret
    "TestInMemoryEventBusSubscriberManagement",  # pragma: allowlist secret
    "TestInMemoryEventBusThreadSafety",  # pragma: allowlist secret
    "TestInMemoryEventBusUnsubscribe",  # pragma: allowlist secret
    "TestInMemoryEventBusWildcardSubscriptions",  # pragma: allowlist secret
]
