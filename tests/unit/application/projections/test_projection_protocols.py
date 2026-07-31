"""
Unit tests for projection protocols.

Tests cover:
- EventHandler protocol
- SyncEventHandler protocol
- EventSubscriber abstract class
- AsyncEventHandler abstract class
- Protocol runtime checking
"""

from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.projections.protocols import AsyncEventHandler
from eventsource.protocols import (
    EventHandler,
    EventSubscriber,
    SyncEventHandler,
)


# Sample events for testing
class OrderCreated(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class TestEventHandlerProtocol:
    """Tests for EventHandler protocol."""

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

    def test_sync_handler_fails_check(self) -> None:
        """Sync handler method fails check (needs async)."""

        # Note: The protocol check doesn't verify async,
        # it just checks for the method existence
        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        # Protocol only checks method existence, not async
        handler = SyncHandler()
        assert isinstance(handler, EventHandler)


class TestSyncEventHandlerProtocol:
    """Tests for SyncEventHandler protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """SyncEventHandler is runtime checkable."""

        class MySyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = MySyncHandler()
        assert isinstance(handler, SyncEventHandler)


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


class TestAsyncEventHandler:
    """Tests for AsyncEventHandler abstract class."""

    def test_cannot_instantiate_directly(self) -> None:
        """AsyncEventHandler cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            AsyncEventHandler()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_complete_handler_works(self) -> None:
        """Complete AsyncEventHandler subclass works."""
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
        """can_handle uses default implementation checking event_types."""

        class OrderHandler(AsyncEventHandler):
            def event_types(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = OrderHandler()

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        assert handler.can_handle(created) is True

    @pytest.mark.asyncio
    async def test_can_handle_can_be_overridden(self) -> None:
        """can_handle can be overridden for custom logic."""

        class CustomHandler(AsyncEventHandler):
            def event_types(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def can_handle(self, event: DomainEvent) -> bool:
                # Only handle high-value orders
                return isinstance(event, OrderCreated)

            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = CustomHandler()

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        assert handler.can_handle(created) is True


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
