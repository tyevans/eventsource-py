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
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        await event_bus.publish([sample_order_created, sample_order_shipped])
        assert handler.handle_count == 2

    @pytest.mark.asyncio
    async def test_wildcard_and_specific_subscription(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        wildcard_handler, specific_handler = RecordingHandler(), RecordingHandler()
        event_bus.subscribe_to_all_events(wildcard_handler)
        event_bus.subscribe(OrderCreated, specific_handler)
        await event_bus.publish([sample_order_created])
        assert wildcard_handler.handle_count == 1
        assert specific_handler.handle_count == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_from_all_events(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        assert event_bus.unsubscribe_from_all_events(handler) is True
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 0

    def test_unsubscribe_from_all_events_not_found(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.unsubscribe_from_all_events(RecordingHandler()) is False

    def test_get_wildcard_subscriber_count(self, event_bus: InMemoryEventBus) -> None:
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
        subscriber = SelectiveSubscriber([OrderCreated])
        event_bus.subscribe_all(subscriber)
        await event_bus.publish([sample_order_created, sample_order_cancelled])
        assert len(subscriber.handled_events) == 1
        assert isinstance(subscriber.handled_events[0], OrderCreated)


class TestInMemoryEventBusUnsubscribe:
    """Tests for unsubscribe functionality."""

    @pytest.mark.asyncio
    async def test_unsubscribe_handler(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1
        assert event_bus.unsubscribe(OrderCreated, handler) is True
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1

    def test_unsubscribe_handler_not_found(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.unsubscribe(OrderCreated, RecordingHandler()) is False

    def test_unsubscribe_wrong_event_type(self, event_bus: InMemoryEventBus) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        assert event_bus.unsubscribe(OrderShipped, handler) is False


class TestInMemoryEventBusSubscriberManagement:
    """Tests for subscriber management utilities."""

    def test_clear_subscribers(self, event_bus: InMemoryEventBus) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        event_bus.subscribe_to_all_events(handler)
        assert event_bus.get_subscriber_count(OrderCreated) == 1
        assert event_bus.get_wildcard_subscriber_count() == 1
        event_bus.clear_subscribers()
        assert event_bus.get_subscriber_count(OrderCreated) == 0
        assert event_bus.get_wildcard_subscriber_count() == 0

    def test_get_subscriber_count_specific_type(self, event_bus: InMemoryEventBus) -> None:
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
        slow_handler = SlowHandler(delay=0.2)
        event_bus.subscribe(OrderCreated, slow_handler)
        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_background_task_count() == 1
        await event_bus.shutdown(timeout=1.0)
        assert len(slow_handler.handled_events) == 1
        assert event_bus.get_background_task_count() == 0

    @pytest.mark.asyncio
    async def test_shutdown_with_no_tasks(self, event_bus: InMemoryEventBus) -> None:
        await event_bus.shutdown(timeout=1.0)

    @pytest.mark.asyncio
    async def test_shutdown_timeout(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        event_bus.subscribe(OrderCreated, SlowHandler(delay=5.0))
        await event_bus.publish([sample_order_created], background=True)
        start = time.time()
        await event_bus.shutdown(timeout=0.1)
        assert time.time() - start < 0.5


class TestInMemoryEventBusStatistics:
    """Tests for event bus statistics."""

    @pytest.mark.asyncio
    async def test_events_published_counter(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        event_bus.subscribe_to_all_events(RecordingHandler())
        await event_bus.publish([sample_order_created])
        await event_bus.publish([sample_order_shipped])
        assert event_bus.get_stats()["events_published"] == 2

    @pytest.mark.asyncio
    async def test_get_stats_returns_copy(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        event_bus.subscribe(OrderCreated, RecordingHandler())
        await event_bus.publish([sample_order_created])
        stats1 = event_bus.get_stats()
        stats1["events_published"] = 999
        assert event_bus.get_stats()["events_published"] == 1


class TestInMemoryEventBusThreadSafety:
    """Tests for thread safety of subscription operations."""

    def test_subscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
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
