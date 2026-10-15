"""Unit tests for event bus subscriptions, management, and lifecycle."""

from __future__ import annotations

import threading
import time

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


def _run_threads(fn, items) -> None:
    threads = [threading.Thread(target=fn, args=(x,)) for x in items]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


class TestInMemoryEventBusWildcardSubscriptions:
    @pytest.mark.asyncio
    async def test_wildcard_subscription_receives_all_events(
        self, event_bus, sample_order_created, sample_order_shipped
    ):
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        await event_bus.publish([sample_order_created, sample_order_shipped])
        assert handler.handle_count == 2

    @pytest.mark.asyncio
    async def test_wildcard_and_specific_subscription(self, event_bus, sample_order_created):
        w_handler, s_handler = RecordingHandler(), RecordingHandler()
        event_bus.subscribe_to_all_events(w_handler)
        event_bus.subscribe(OrderCreated, s_handler)
        await event_bus.publish([sample_order_created])
        assert (w_handler.handle_count, s_handler.handle_count) == (1, 1)

    @pytest.mark.asyncio
    async def test_unsubscribe_from_all_events(self, event_bus, sample_order_created):
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        assert event_bus.unsubscribe_from_all_events(handler) is True
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 0

    def test_unsubscribe_from_all_events_not_found(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.unsubscribe_from_all_events(RecordingHandler()) is False

    def test_get_wildcard_subscriber_count(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.get_wildcard_subscriber_count() == 0
        for expected in (1, 2):
            event_bus.subscribe_to_all_events(RecordingHandler())
            assert event_bus.get_wildcard_subscriber_count() == expected


@pytest.mark.asyncio
class TestInMemoryEventBusSubscribeAll:
    async def test_subscribe_all_with_subscriber(
        self, event_bus, sample_order_created, sample_order_shipped
    ):
        subscriber = SelectiveSubscriber([OrderCreated, OrderShipped])
        event_bus.subscribe_all(subscriber)
        for count, evt in enumerate((sample_order_created, sample_order_shipped), 1):
            await event_bus.publish([evt])
            assert len(subscriber.handled_events) == count

    async def test_subscribe_all_ignores_undeclared_events(
        self, event_bus, sample_order_created, sample_order_cancelled
    ):
        subscriber = SelectiveSubscriber([OrderCreated])
        event_bus.subscribe_all(subscriber)
        await event_bus.publish([sample_order_created, sample_order_cancelled])
        assert [type(e) for e in subscriber.handled_events] == [OrderCreated]


class TestInMemoryEventBusUnsubscribe:
    @pytest.mark.asyncio
    async def test_unsubscribe_handler(self, event_bus, sample_order_created):
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
        bus, handler = event_bus, RecordingHandler()
        bus.subscribe(OrderCreated, handler)
        bus.subscribe_to_all_events(handler)
        assert bus.get_subscriber_count(OrderCreated) == 1 == bus.get_wildcard_subscriber_count()
        bus.clear_subscribers()
        assert bus.get_subscriber_count(OrderCreated) == 0 == bus.get_wildcard_subscriber_count()

    def test_get_subscriber_count_specific_type(self, event_bus: InMemoryEventBus) -> None:
        cnt = event_bus.get_subscriber_count
        assert cnt(OrderCreated) == 0
        event_bus.subscribe(OrderCreated, RecordingHandler())
        assert cnt(OrderCreated) == 1
        event_bus.subscribe(OrderCreated, RecordingHandler())
        event_bus.subscribe(OrderShipped, RecordingHandler())
        assert (cnt(OrderCreated), cnt(OrderShipped)) == (2, 1)

    def test_get_subscriber_count_total(self, event_bus: InMemoryEventBus) -> None:
        assert event_bus.get_subscriber_count() == 0
        for evt in (OrderCreated, OrderShipped, OrderCancelled):
            event_bus.subscribe(evt, RecordingHandler())
        assert event_bus.get_subscriber_count() == 3


@pytest.mark.asyncio
class TestInMemoryEventBusShutdown:
    async def test_shutdown_waits_for_background_tasks(self, event_bus, sample_order_created):
        slow_handler = SlowHandler(delay=0.2)
        event_bus.subscribe(OrderCreated, slow_handler)
        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_background_task_count() == 1
        await event_bus.shutdown(timeout=1.0)
        assert (len(slow_handler.handled_events), event_bus.get_background_task_count()) == (1, 0)

    async def test_shutdown_with_no_tasks(self, event_bus: InMemoryEventBus) -> None:
        await event_bus.shutdown(timeout=1.0)

    async def test_shutdown_timeout(self, event_bus, sample_order_created):
        event_bus.subscribe(OrderCreated, SlowHandler(delay=5.0))
        await event_bus.publish([sample_order_created], background=True)
        start = time.time()
        await event_bus.shutdown(timeout=0.1)
        assert time.time() - start < 0.5


@pytest.mark.asyncio
class TestInMemoryEventBusStatistics:
    async def test_events_published_counter(
        self, event_bus, sample_order_created, sample_order_shipped
    ):
        event_bus.subscribe_to_all_events(RecordingHandler())
        await event_bus.publish([sample_order_created])
        await event_bus.publish([sample_order_shipped])
        assert event_bus.get_stats()["events_published"] == 2

    async def test_get_stats_returns_copy(self, event_bus, sample_order_created):
        event_bus.subscribe(OrderCreated, RecordingHandler())
        await event_bus.publish([sample_order_created])
        event_bus.get_stats()["events_published"] = 999
        assert event_bus.get_stats()["events_published"] == 1

    async def test_len_bool_and_clear(self, event_bus, sample_order_created):
        assert len(event_bus) == 0 and bool(event_bus) is True
        await event_bus.publish([sample_order_created])
        assert len(event_bus) == 1 and bool(event_bus) is True
        event_bus.clear()
        assert len(event_bus) == 0 and event_bus.published_events == []


class TestInMemoryEventBusThreadSafety:
    def test_subscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        handlers = [RecordingHandler() for _ in range(10)]
        _run_threads(lambda h: event_bus.subscribe(OrderCreated, h), handlers)
        assert event_bus.get_subscriber_count(OrderCreated) == 10

    def test_unsubscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        handlers = [RecordingHandler() for _ in range(10)]
        for h in handlers:
            event_bus.subscribe(OrderCreated, h)
        results: list[bool] = []
        _run_threads(lambda h: results.append(event_bus.unsubscribe(OrderCreated, h)), handlers)
        assert all(results) and event_bus.get_subscriber_count(OrderCreated) == 0


class TestEventOrdering:
    @pytest.mark.asyncio
    async def test_events_processed_in_order(
        self, event_bus, sample_order_created, sample_order_shipped, sample_order_cancelled
    ) -> None:
        order: list[str] = []
        event_bus.subscribe_to_all_events(lambda e: order.append(e.event_type))
        events = [sample_order_created, sample_order_shipped, sample_order_cancelled]
        await event_bus.publish(events)
        assert order == ["OrderCreated", "OrderShipped", "OrderCancelled"]
