"""
Unit tests for canonical protocol definitions.

Tests cover:
- EventHandler protocol (async-only)
- SyncEventHandler protocol
- FlexibleEventHandler protocol
- EventSubscriber abstract class
- FlexibleEventSubscriber protocol
- AsyncEventHandler abstract class (ABF-01)
- Deprecation warnings from old import locations
- Protocol runtime checking
"""

import warnings
from collections.abc import Awaitable
from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.protocols import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
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

        # Note: The protocol runtime check doesn't verify async,
        # it just checks for the method existence. Type checkers
        # will catch the signature mismatch.
        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = SyncHandler()
        # Protocol only checks method existence at runtime, not async
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


class TestDeprecationWarnings:
    """Tests for deprecation warnings when importing from old locations."""

    def test_canonical_import_no_warning(self) -> None:
        """Importing from canonical location does not warn."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            from eventsource.protocols import (  # noqa: F401
                EventHandler,
                EventSubscriber,
                FlexibleEventHandler,
                SyncEventHandler,
            )
            # Should not raise


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


class TestCanonicalExports:
    """Tests for canonical module exports."""

    def test_eventsource_root_exports_protocols(self) -> None:
        """Root eventsource module exports protocols."""
        import eventsource

        assert hasattr(eventsource, "EventHandler")
        assert hasattr(eventsource, "SyncEventHandler")
        assert hasattr(eventsource, "FlexibleEventHandler")
        assert hasattr(eventsource, "EventSubscriber")
        assert hasattr(eventsource, "FlexibleEventSubscriber")

    def test_eventsource_bus_exports_protocols(self) -> None:
        """Bus module exports protocols."""
        from eventsource import bus

        assert hasattr(bus, "EventHandler")
        assert hasattr(bus, "EventSubscriber")
        assert hasattr(bus, "FlexibleEventHandler")
        assert hasattr(bus, "FlexibleEventSubscriber")

    def test_eventsource_projections_exports_protocols(self) -> None:
        """Projections module exports protocols."""
        from eventsource import projections

        assert hasattr(projections, "EventHandler")
        assert hasattr(projections, "SyncEventHandler")
        assert hasattr(projections, "EventSubscriber")

    def test_eventsource_root_exports_async_event_handler(self) -> None:
        """Root eventsource module exports AsyncEventHandler."""
        import eventsource

        assert hasattr(eventsource, "AsyncEventHandler")


class TestAsyncEventHandler:
    """Tests for AsyncEventHandler abstract base class."""

    def test_canonical_import_works(self) -> None:
        """Importing AsyncEventHandler from protocols works."""

        assert AsyncEventHandler is not None

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
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        handler = MyHandler()

        assert OrderCreated in handler.event_types()
        assert OrderShipped in handler.event_types()
        assert len(handler.event_types()) == 2

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await handler.handle(event)

        assert len(events_handled) == 1

    def test_can_handle_default_implementation(self) -> None:
        """can_handle method correctly checks event types."""

        class MyHandler(AsyncEventHandler):
            def event_types(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = MyHandler()

        created_event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped_event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        assert handler.can_handle(created_event) is True
        assert handler.can_handle(shipped_event) is False

    def test_missing_event_types_raises_error(self) -> None:
        """Subclass missing event_types raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteHandler(AsyncEventHandler):
                async def handle(self, event: DomainEvent) -> None:
                    pass

            IncompleteHandler()  # type: ignore[abstract]

    def test_missing_handle_raises_error(self) -> None:
        """Subclass missing handle raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteHandler(AsyncEventHandler):
                def event_types(self) -> list[type[DomainEvent]]:
                    return [OrderCreated]

            IncompleteHandler()  # type: ignore[abstract]


class TestAsyncEventHandlerImports:
    """Tests for AsyncEventHandler imports from canonical location."""

    def test_top_level_import_works(self) -> None:
        """Top-level eventsource import works."""
        from eventsource import AsyncEventHandler
        from eventsource.protocols import AsyncEventHandler as Canonical

        assert AsyncEventHandler is Canonical

    def test_handlers_adapter_import_works(self) -> None:
        """Import from handlers.adapter works."""
        from eventsource.handlers.adapter import AsyncEventHandler
        from eventsource.protocols import AsyncEventHandler as Canonical

        assert AsyncEventHandler is Canonical

    def test_projections_protocols_import_works(self) -> None:
        """Import from projections.protocols works (now direct, no deprecation)."""
        from eventsource.projections.protocols import AsyncEventHandler
        from eventsource.protocols import AsyncEventHandler as Canonical

        assert AsyncEventHandler is Canonical

    def test_bus_import_works(self) -> None:
        """Import from bus module works."""
        from eventsource.bus import AsyncEventHandler
        from eventsource.protocols import AsyncEventHandler as Canonical

        assert AsyncEventHandler is Canonical
