"""Unit tests for event bus publishing functionality."""

from __future__ import annotations

import asyncio
import time

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from tests.unit.test_event_bus._fixtures import (
    FailingHandler,
    OrderCreated,
    OrderShipped,
    RecordingHandler,
    SlowHandler,
    SyncRecordingHandler,
)


class TestInMemoryEventBusBasicPublishing:
    @pytest.mark.asyncio
    async def test_publish_to_subscribed_handler(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1
        assert handler.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_publish_multiple_events(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        event_bus.subscribe(OrderShipped, handler)
        await event_bus.publish([sample_order_created, sample_order_shipped])
        assert handler.handle_count == 2
        assert handler.handled_events[0] == sample_order_created
        assert handler.handled_events[1] == sample_order_shipped

    @pytest.mark.asyncio
    async def test_publish_empty_list(self, event_bus: InMemoryEventBus) -> None:
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)
        await event_bus.publish([])
        assert handler.handle_count == 0

    @pytest.mark.asyncio
    async def test_publish_to_no_handlers(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        await event_bus.publish([sample_order_created])


class TestInMemoryEventBusMultipleHandlers:
    @pytest.mark.asyncio
    async def test_multiple_handlers_for_same_event(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handler1, handler2 = RecordingHandler(), RecordingHandler()
        event_bus.subscribe(OrderCreated, handler1)
        event_bus.subscribe(OrderCreated, handler2)
        await event_bus.publish([sample_order_created])
        assert handler1.handle_count == 1
        assert handler2.handle_count == 1
        assert handler1.handled_events[0] == sample_order_created
        assert handler2.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_handlers_execute_concurrently(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handler1, handler2 = SlowHandler(delay=0.1), SlowHandler(delay=0.1)
        event_bus.subscribe(OrderCreated, handler1)
        event_bus.subscribe(OrderCreated, handler2)
        start = time.time()
        await event_bus.publish([sample_order_created])
        assert time.time() - start < 0.15
        assert len(handler1.handled_events) == 1
        assert len(handler2.handled_events) == 1


class TestInMemoryEventBusErrorHandling:
    @pytest.mark.asyncio
    async def test_handler_failure_doesnt_affect_others(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        failing_handler, recording_handler = FailingHandler(), RecordingHandler()
        event_bus.subscribe(OrderCreated, failing_handler)
        event_bus.subscribe(OrderCreated, recording_handler)
        await event_bus.publish([sample_order_created])
        assert failing_handler.call_count == 1
        assert recording_handler.handle_count == 1

    @pytest.mark.asyncio
    async def test_error_statistics_tracked(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        event_bus.subscribe(OrderCreated, FailingHandler())
        await event_bus.publish([sample_order_created])
        assert event_bus.get_stats()["handler_errors"] == 1

    @pytest.mark.asyncio
    async def test_successful_handlers_tracked(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        event_bus.subscribe(OrderCreated, RecordingHandler())
        await event_bus.publish([sample_order_created])
        assert event_bus.get_stats()["handlers_invoked"] == 1


class TestInMemoryEventBusSyncAsyncSupport:
    @pytest.mark.asyncio
    async def test_sync_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handler = SyncRecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1
        assert handler.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_async_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1
        assert handler.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_mixed_sync_async_handlers(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        sync_handler, async_handler = SyncRecordingHandler(), RecordingHandler()
        event_bus.subscribe(OrderCreated, sync_handler)
        event_bus.subscribe(OrderCreated, async_handler)
        await event_bus.publish([sample_order_created])
        assert sync_handler.handle_count == 1
        assert async_handler.handle_count == 1

    @pytest.mark.asyncio
    async def test_callable_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handled_events: list[DomainEvent] = []

        async def handler_func(event: DomainEvent) -> None:
            handled_events.append(event)

        event_bus.subscribe(OrderCreated, handler_func)
        await event_bus.publish([sample_order_created])
        assert handled_events == [sample_order_created]

    @pytest.mark.asyncio
    async def test_sync_callable_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        handled_events: list[DomainEvent] = []

        def handler_func(event: DomainEvent) -> None:
            handled_events.append(event)

        event_bus.subscribe(OrderCreated, handler_func)
        await event_bus.publish([sample_order_created])
        assert handled_events == [sample_order_created]


class TestInMemoryEventBusBackgroundPublishing:
    @pytest.mark.asyncio
    async def test_background_publish_returns_immediately(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        slow_handler = SlowHandler(delay=0.5)
        event_bus.subscribe(OrderCreated, slow_handler)
        start = time.time()
        await event_bus.publish([sample_order_created], background=True)
        assert time.time() - start < 0.1
        await asyncio.sleep(0.6)
        assert len(slow_handler.handled_events) == 1

    @pytest.mark.asyncio
    async def test_background_tasks_tracked(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        event_bus.subscribe(OrderCreated, SlowHandler(delay=0.2))
        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_stats()["background_tasks_created"] == 1
        await asyncio.sleep(0.3)
        assert event_bus.get_stats()["background_tasks_completed"] == 1

    @pytest.mark.asyncio
    async def test_get_background_task_count(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        event_bus.subscribe(OrderCreated, SlowHandler(delay=0.3))
        assert event_bus.get_background_task_count() == 0
        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_background_task_count() == 1
        await asyncio.sleep(0.4)
        assert event_bus.get_background_task_count() == 0


__all__ = [
    "TestInMemoryEventBusBackgroundPublishing",  # pragma: allowlist secret
    "TestInMemoryEventBusBasicPublishing",  # pragma: allowlist secret
    "TestInMemoryEventBusErrorHandling",  # pragma: allowlist secret
    "TestInMemoryEventBusMultipleHandlers",  # pragma: allowlist secret
    "TestInMemoryEventBusSyncAsyncSupport",  # pragma: allowlist secret
]
