"""Unit tests for event bus subscriptions, management, and lifecycle."""

from __future__ import annotations

import threading
import time
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from tests.unit.test_event_bus._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    RecordingHandler,
    SelectiveSubscriber,
    SlowHandler,
)


class TestInMemoryEventBusWildcardSubscriptions:
    @pytest.mark.asyncio
    async def test_wildcard_subscription_receives_all_events(
        self, event_bus, sample_order_created, sample_order_shipped
    ) -> None:
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        await event_bus.publish([sample_order_created, sample_order_shipped])
        assert handler.handle_count == 2

    @pytest.mark.asyncio
    async def test_wildcard_and_specific_subscription(
        self, event_bus, sample_order_created
    ) -> None:
        w_handler, s_handler = RecordingHandler(), RecordingHandler()
        event_bus.subscribe_to_all_events(w_handler)
        event_bus.subscribe(OrderCreated, s_handler)
        await event_bus.publish([sample_order_created])
        assert (w_handler.handle_count, s_handler.handle_count) == (1, 1)

    @pytest.mark.asyncio
    async def test_unsubscribe_from_all_events(self, event_bus, sample_order_created) -> None:
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        assert event_bus.unsubscribe_from_all_events(handler) is True
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 0

    def test_unsubscribe_from_all_events_not_found(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.unsubscribe_from_all_events(RecordingHandler()) is False

    def test_get_wildcard_subscriber_count(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.get_wildcard_subscriber_count() == 0
        event_bus.subscribe_to_all_events(RecordingHandler())
        assert event_bus.get_wildcard_subscriber_count() == 1
        event_bus.subscribe_to_all_events(RecordingHandler())
        assert event_bus.get_wildcard_subscriber_count() == 2


class TestInMemoryEventBusSubscribeAll:
    @pytest.mark.asyncio
    async def test_subscribe_all_with_subscriber(
        self, event_bus, sample_order_created, sample_order_shipped
    ) -> None:
        subscriber = SelectiveSubscriber([OrderCreated, OrderShipped])
        event_bus.subscribe_all(subscriber)
        await event_bus.publish([sample_order_created])
        assert len(subscriber.handled_events) == 1
        await event_bus.publish([sample_order_shipped])
        assert len(subscriber.handled_events) == 2

    @pytest.mark.asyncio
    async def test_subscribe_all_ignores_undeclared_events(
        self, event_bus, sample_order_created, sample_order_cancelled
    ) -> None:
        subscriber = SelectiveSubscriber([OrderCreated])
        event_bus.subscribe_all(subscriber)
        await event_bus.publish([sample_order_created, sample_order_cancelled])
        assert len(subscriber.handled_events) == 1
        assert isinstance(subscriber.handled_events[0], OrderCreated)


class TestInMemoryEventBusUnsubscribe:
    @pytest.mark.asyncio
    async def test_unsubscribe_handler(self, event_bus, sample_order_created) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1 and event_bus.unsubscribe(OrderCreated, handler) is True
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1

    def test_unsubscribe_handler_not_found(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.unsubscribe(OrderCreated, RecordingHandler()) is False

    def test_unsubscribe_wrong_event_type(self, event_bus: InMemoryEventBus) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        assert event_bus.unsubscribe(OrderShipped, handler) is False


class TestInMemoryEventBusSubscriberManagement:
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
        h1, h2 = RecordingHandler(), RecordingHandler()
        assert event_bus.get_subscriber_count(OrderCreated) == 0
        event_bus.subscribe(OrderCreated, h1)
        assert event_bus.get_subscriber_count(OrderCreated) == 1
        event_bus.subscribe(OrderCreated, h2)
        event_bus.subscribe(OrderShipped, h1)
        assert event_bus.get_subscriber_count(OrderCreated) == 2
        assert event_bus.get_subscriber_count(OrderShipped) == 1

    def test_get_subscriber_count_total(self, event_bus: InMemoryEventBus) -> None:
        handler = RecordingHandler()
        assert event_bus.get_subscriber_count() == 0
        for evt in (OrderCreated, OrderShipped, OrderCancelled):
            event_bus.subscribe(evt, handler)
        assert event_bus.get_subscriber_count() == 3


class TestInMemoryEventBusShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_waits_for_background_tasks(
        self, event_bus, sample_order_created
    ) -> None:
        slow_handler = SlowHandler(delay=0.2)
        event_bus.subscribe(OrderCreated, slow_handler)
        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_background_task_count() == 1
        await event_bus.shutdown(timeout=1.0)
        assert (len(slow_handler.handled_events), event_bus.get_background_task_count()) == (1, 0)

    @pytest.mark.asyncio
    async def test_shutdown_with_no_tasks(self, event_bus: InMemoryEventBus) -> None:
        await event_bus.shutdown(timeout=1.0)

    @pytest.mark.asyncio
    async def test_shutdown_timeout(self, event_bus, sample_order_created) -> None:
        event_bus.subscribe(OrderCreated, SlowHandler(delay=5.0))
        await event_bus.publish([sample_order_created], background=True)
        start = time.time()
        await event_bus.shutdown(timeout=0.1)
        assert time.time() - start < 0.5


class TestInMemoryEventBusStatistics:
    @pytest.mark.asyncio
    async def test_events_published_counter(
        self, event_bus, sample_order_created, sample_order_shipped
    ) -> None:
        event_bus.subscribe_to_all_events(RecordingHandler())
        await event_bus.publish([sample_order_created])
        await event_bus.publish([sample_order_shipped])
        assert event_bus.get_stats()["events_published"] == 2

    @pytest.mark.asyncio
    async def test_get_stats_returns_copy(self, event_bus, sample_order_created) -> None:
        event_bus.subscribe(OrderCreated, RecordingHandler())
        await event_bus.publish([sample_order_created])
        stats1 = event_bus.get_stats()
        stats1["events_published"] = 999
        assert event_bus.get_stats()["events_published"] == 1

    @pytest.mark.asyncio
    async def test_len_bool_and_clear(self, event_bus, sample_order_created) -> None:
        assert len(event_bus) == 0 and bool(event_bus) is True
        await event_bus.publish([sample_order_created])
        assert len(event_bus) == 1 and bool(event_bus) is True
        event_bus.clear()
        assert len(event_bus) == 0 and event_bus.published_events == []


class TestInMemoryEventBusThreadSafety:
    def test_subscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        fn = lambda: event_bus.subscribe(OrderCreated, RecordingHandler())  # noqa: E731
        threads = [threading.Thread(target=fn) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert event_bus.get_subscriber_count(OrderCreated) == 10

    def test_unsubscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        handlers = [RecordingHandler() for _ in range(10)]
        for h in handlers:
            event_bus.subscribe(OrderCreated, h)
        results: list[bool] = []
        fn = lambda h: results.append(event_bus.unsubscribe(OrderCreated, h))  # noqa: E731
        threads = [threading.Thread(target=fn, args=(h,)) for h in handlers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(results) and event_bus.get_subscriber_count(OrderCreated) == 0


class TestEventOrdering:
    @pytest.mark.asyncio
    async def test_events_processed_in_order(self, event_bus: InMemoryEventBus) -> None:
        order: list[str] = []
        event_bus.subscribe_to_all_events(lambda e: order.append(e.event_type))
        e1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", customer_id="cust-1")
        e2 = OrderShipped(aggregate_id=uuid4(), order_number="ORD-001", tracking_number="TRK-1")
        e3 = OrderCancelled(aggregate_id=uuid4(), order_number="ORD-001", reason="Test")
        await event_bus.publish([e1, e2, e3])
        assert order == ["OrderCreated", "OrderShipped", "OrderCancelled"]
