"""Unit tests for AsyncEventHandler and canonical handler exports."""

import warnings
from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
    SyncEventHandler,
)

from ._fixtures import OrderCreated, OrderShipped


class TestDeprecationWarnings:
    """Tests for canonical imports without warnings."""

    def test_canonical_import_no_warning(self) -> None:
        """Importing from canonical location does not warn."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            from eventsource.ports.handlers import (  # noqa: F401
                EventHandler,
                EventSubscriber,
                FlexibleEventHandler,
                SyncEventHandler,
            )


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

    def test_eventsource_bus_package_removed(self) -> None:
        """The eventsource.bus package no longer exists (ADR-0031)."""
        import importlib

        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("eventsource.bus")

    def test_eventsource_root_exports_async_event_handler(self) -> None:
        """Root eventsource module exports AsyncEventHandler."""
        import eventsource

        assert hasattr(eventsource, "AsyncEventHandler")

    def test_all_six_names_importable_from_ports_handlers(self) -> None:
        """All six names are importable directly from eventsource.ports.handlers."""
        for obj in (
            EventHandler,
            SyncEventHandler,
            FlexibleEventHandler,
            EventSubscriber,
            FlexibleEventSubscriber,
            AsyncEventHandler,
        ):
            assert obj is not None

    def test_all_six_names_reexported_from_ports_package(self) -> None:
        """All six names are re-exported from eventsource.ports."""
        from eventsource import ports
        from eventsource.ports import handlers as ports_handlers

        for name in (
            "EventHandler",
            "SyncEventHandler",
            "FlexibleEventHandler",
            "EventSubscriber",
            "FlexibleEventSubscriber",
            "AsyncEventHandler",
        ):
            assert hasattr(ports, name)
            assert getattr(ports, name) is getattr(ports_handlers, name)

    def test_all_six_names_reexported_from_top_level(self) -> None:
        """All six names are re-exported from the top-level eventsource package."""
        import eventsource
        from eventsource.ports import handlers as ports_handlers

        for name in (
            "EventHandler",
            "SyncEventHandler",
            "FlexibleEventHandler",
            "EventSubscriber",
            "FlexibleEventSubscriber",
            "AsyncEventHandler",
        ):
            assert hasattr(eventsource, name)
            assert getattr(eventsource, name) is getattr(ports_handlers, name)


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
        from eventsource.ports.handlers import AsyncEventHandler as Canonical

        assert AsyncEventHandler is Canonical

    def test_handlers_adapter_import_works(self) -> None:
        """Import from ports.handlers works."""
        from eventsource.ports.handlers import AsyncEventHandler
        from eventsource.ports.handlers import AsyncEventHandler as Canonical

        assert AsyncEventHandler is Canonical
