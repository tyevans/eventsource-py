"""Unit tests for projection protocols."""

from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
    SyncEventHandler,
)


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class TestEventHandlerProtocol:
    """Tests for EventHandler protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MyHandler(), EventHandler)

    def test_non_handler_fails_check(self) -> None:
        class NotAHandler:
            pass

        assert not isinstance(NotAHandler(), EventHandler)

    def test_sync_handler_fails_check(self) -> None:
        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(SyncHandler(), EventHandler)


class TestSyncEventHandlerProtocol:
    """Tests for SyncEventHandler protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        class MySyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySyncHandler(), SyncEventHandler)


class TestEventSubscriber:
    """Tests for EventSubscriber abstract class."""

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
        assert OrderCreated in subscriber.subscribed_to()
        assert OrderShipped in subscriber.subscribed_to()
        assert len(subscriber.subscribed_to()) == 2

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


class TestAsyncEventHandler:
    """Tests for AsyncEventHandler abstract class."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            AsyncEventHandler()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_complete_handler_works(self) -> None:
        events_handled: list[DomainEvent] = []

        class MyHandler(AsyncEventHandler):
            def event_types(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        handler = MyHandler()
        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        assert handler.can_handle(created) is True
        assert handler.can_handle(shipped) is False
        await handler.handle(created)
        assert len(events_handled) == 1

    def test_can_handle_default_implementation(self) -> None:
        class OrderHandler(AsyncEventHandler):
            def event_types(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = OrderHandler()
        assert handler.can_handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))

    @pytest.mark.asyncio
    async def test_can_handle_can_be_overridden(self) -> None:
        class CustomHandler(AsyncEventHandler):
            def event_types(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def can_handle(self, event: DomainEvent) -> bool:
                return isinstance(event, OrderCreated)

            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = CustomHandler()
        assert handler.can_handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))


class TestProtocolDuckTyping:
    """Tests for duck typing with protocols."""

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
        await process_event(handler, OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(handler.events) == 1
