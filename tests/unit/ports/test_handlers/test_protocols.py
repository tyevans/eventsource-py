"""Unit tests for EventHandler, SyncEventHandler, FlexibleEventHandler, and EventSubscriber."""

from collections.abc import Awaitable
from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import (
    EventHandler,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
    SyncEventHandler,
)

from ._fixtures import OrderCreated, OrderShipped


class TestEventHandlerProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MyHandler(), EventHandler)

    def test_non_handler_fails_check(self) -> None:
        class NotAHandler:
            pass

        assert not isinstance(NotAHandler(), EventHandler)

    def test_sync_handler_still_matches_protocol(self) -> None:
        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(SyncHandler(), EventHandler)

    @pytest.mark.asyncio
    async def test_async_handler_works(self) -> None:
        events_handled: list[DomainEvent] = []

        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await MyHandler().handle(event)
        assert events_handled == [event]


class TestSyncEventHandlerProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        class MySyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySyncHandler(), SyncEventHandler)

    def test_sync_handler_works(self) -> None:
        events_handled: list[DomainEvent] = []

        class MySyncHandler:
            def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        MySyncHandler().handle(event)
        assert len(events_handled) == 1


class TestFlexibleEventHandlerProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        class MyHandler:
            def handle(self, event: DomainEvent) -> Awaitable[None] | None:
                return None

        assert isinstance(MyHandler(), FlexibleEventHandler)

    def test_async_handler_matches(self) -> None:
        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(AsyncHandler(), FlexibleEventHandler)

    def test_sync_handler_matches(self) -> None:
        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(SyncHandler(), FlexibleEventHandler)


class TestEventSubscriber:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            EventSubscriber()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_complete_subscriber_works(self) -> None:
        events_handled: list[DomainEvent] = []

        class MySubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        subscriber = MySubscriber()
        assert subscriber.subscribed_to() == [OrderCreated, OrderShipped]
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await subscriber.handle(event)
        assert len(events_handled) == 1

    def test_missing_subscribed_to_raises_error(self) -> None:
        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(EventSubscriber):
                async def handle(self, event: DomainEvent) -> None:
                    pass

            IncompleteSubscriber()  # type: ignore[abstract]

    def test_missing_handle_raises_error(self) -> None:
        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(EventSubscriber):
                def subscribed_to(self) -> list[type[DomainEvent]]:
                    return [OrderCreated]

            IncompleteSubscriber()  # type: ignore[abstract]


class TestFlexibleEventSubscriberProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> Awaitable[None] | None:
                return None

        assert isinstance(MySubscriber(), FlexibleEventSubscriber)

    def test_abc_subscriber_matches(self) -> None:
        class MySubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySubscriber(), FlexibleEventSubscriber)


class TestProtocolDuckTyping:
    @pytest.mark.asyncio
    async def test_function_accepting_event_handler(self) -> None:
        async def process_event(handler: EventHandler, event: DomainEvent) -> None:
            await handler.handle(event)

        class CustomHandler:
            def __init__(self) -> None:
                self.events: list[DomainEvent] = []

            async def handle(self, event: DomainEvent) -> None:
                self.events.append(event)

        handler = CustomHandler()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await process_event(handler, event)
        assert len(handler.events) == 1

    def test_function_accepting_flexible_handler(self) -> None:
        def get_handler_type(handler: FlexibleEventHandler) -> str:
            return handler.__class__.__name__

        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert get_handler_type(SyncHandler()) == "SyncHandler"
        assert get_handler_type(AsyncHandler()) == "AsyncHandler"
