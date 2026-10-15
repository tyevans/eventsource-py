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
    """Tests for EventHandler protocol (async-only)."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """EventHandler is runtime checkable."""

        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = MyHandler()
        assert isinstance(handler, EventHandler)

    def test_non_handler_fails_check(self) -> None:
        """Object without handle method fails check."""

        class NotAHandler:
            pass

        obj = NotAHandler()
        assert not isinstance(obj, EventHandler)

    def test_sync_handler_still_matches_protocol(self) -> None:
        """Sync handler matches protocol (runtime check only checks method existence)."""

        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = SyncHandler()
        assert isinstance(handler, EventHandler)

    @pytest.mark.asyncio
    async def test_async_handler_works(self) -> None:
        """Async handler works correctly."""
        events_handled: list[DomainEvent] = []

        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        handler = MyHandler()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await handler.handle(event)

        assert len(events_handled) == 1
        assert events_handled[0] == event


class TestSyncEventHandlerProtocol:
    """Tests for SyncEventHandler protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """SyncEventHandler is runtime checkable."""

        class MySyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = MySyncHandler()
        assert isinstance(handler, SyncEventHandler)

    def test_sync_handler_works(self) -> None:
        """Sync handler works correctly."""
        events_handled: list[DomainEvent] = []

        class MySyncHandler:
            def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        handler = MySyncHandler()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        handler.handle(event)

        assert len(events_handled) == 1


class TestFlexibleEventHandlerProtocol:
    """Tests for FlexibleEventHandler protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """FlexibleEventHandler is runtime checkable."""

        class MyHandler:
            def handle(self, event: DomainEvent) -> Awaitable[None] | None:
                return None

        handler = MyHandler()
        assert isinstance(handler, FlexibleEventHandler)

    def test_async_handler_matches(self) -> None:
        """Async handler matches FlexibleEventHandler."""

        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = AsyncHandler()
        assert isinstance(handler, FlexibleEventHandler)

    def test_sync_handler_matches(self) -> None:
        """Sync handler matches FlexibleEventHandler."""

        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = SyncHandler()
        assert isinstance(handler, FlexibleEventHandler)


class TestEventSubscriber:
    """Tests for EventSubscriber abstract class."""

    def test_cannot_instantiate_directly(self) -> None:
        """EventSubscriber cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            EventSubscriber()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_complete_subscriber_works(self) -> None:
        """Complete EventSubscriber subclass works."""
        events_handled: list[DomainEvent] = []

        class MySubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        subscriber = MySubscriber()

        assert OrderCreated in subscriber.subscribed_to()
        assert OrderShipped in subscriber.subscribed_to()
        assert len(subscriber.subscribed_to()) == 2

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await subscriber.handle(event)

        assert len(events_handled) == 1

    def test_missing_subscribed_to_raises_error(self) -> None:
        """Subclass missing subscribed_to raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(EventSubscriber):
                async def handle(self, event: DomainEvent) -> None:
                    pass

            IncompleteSubscriber()  # type: ignore[abstract]

    def test_missing_handle_raises_error(self) -> None:
        """Subclass missing handle raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(EventSubscriber):
                def subscribed_to(self) -> list[type[DomainEvent]]:
                    return [OrderCreated]

            IncompleteSubscriber()  # type: ignore[abstract]


class TestFlexibleEventSubscriberProtocol:
    """Tests for FlexibleEventSubscriber protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """FlexibleEventSubscriber is runtime checkable."""

        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> Awaitable[None] | None:
                return None

        subscriber = MySubscriber()
        assert isinstance(subscriber, FlexibleEventSubscriber)

    def test_abc_subscriber_matches(self) -> None:
        """ABC-based EventSubscriber also matches FlexibleEventSubscriber."""

        class MySubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        assert isinstance(subscriber, FlexibleEventSubscriber)


class TestProtocolDuckTyping:
    """Tests for duck typing with protocols."""

    @pytest.mark.asyncio
    async def test_function_accepting_event_handler(self) -> None:
        """Function can accept any EventHandler implementation."""

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
        """Function can accept any FlexibleEventHandler implementation."""

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
