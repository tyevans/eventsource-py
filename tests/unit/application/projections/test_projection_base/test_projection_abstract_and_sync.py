"""Unit tests for Projection, SyncProjection, and EventHandlerBase."""

from uuid import uuid4

import pytest

from eventsource.application.projections.base import (
    EventHandlerBase,
    Projection,
    SyncProjection,
)
from eventsource.domain.event import DomainEvent
from tests.unit.application.projections.test_projection_base._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
)


class TestProjectionAbstract:
    """Tests for Projection abstract base class."""

    def test_projection_cannot_be_instantiated_directly(self) -> None:
        """Projection cannot be instantiated without implementing abstract methods."""
        with pytest.raises(TypeError, match="abstract"):
            Projection()  # type: ignore[abstract]

    def test_projection_subclass_must_implement_handle(self) -> None:
        """Projection subclass must implement handle method."""

        class IncompleteProjection(Projection):
            async def reset(self) -> None:
                pass

        with pytest.raises(TypeError, match="abstract"):
            IncompleteProjection()  # type: ignore[abstract]

    def test_projection_subclass_must_implement_reset(self) -> None:
        """Projection subclass must implement reset method."""

        class IncompleteProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

        with pytest.raises(TypeError, match="abstract"):
            IncompleteProjection()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_complete_projection_subclass_works(self) -> None:
        """Complete Projection subclass can be instantiated and used."""
        events_handled: list[DomainEvent] = []

        class CompleteProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

            async def reset(self) -> None:
                events_handled.clear()

        projection = CompleteProjection()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(events_handled) == 1
        assert events_handled[0] == event


class TestSyncProjection:
    """Tests for SyncProjection abstract base class."""

    def test_sync_projection_cannot_be_instantiated(self) -> None:
        """SyncProjection cannot be instantiated without implementing abstract methods."""
        with pytest.raises(TypeError, match="abstract"):
            SyncProjection()  # type: ignore[abstract]

    def test_complete_sync_projection_works(self) -> None:
        """Complete SyncProjection subclass works."""
        events_handled: list[DomainEvent] = []

        class CompleteSyncProjection(SyncProjection):
            def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

            def reset(self) -> None:
                events_handled.clear()

        projection = CompleteSyncProjection()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        projection.handle(event)

        assert len(events_handled) == 1


class TestEventHandlerBase:
    """Tests for EventHandlerBase class."""

    def test_event_handler_cannot_be_instantiated(self) -> None:
        """EventHandlerBase cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            EventHandlerBase()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_complete_event_handler_works(self) -> None:
        """Complete EventHandlerBase subclass works."""
        events_handled: list[DomainEvent] = []

        class OrderHandler(EventHandlerBase):
            def can_handle(self, event: DomainEvent) -> bool:
                return isinstance(event, OrderCreated | OrderShipped)

            async def handle(self, event: DomainEvent) -> None:
                events_handled.append(event)

        handler = OrderHandler()

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        cancelled = OrderCancelled(aggregate_id=uuid4(), reason="Customer request")

        assert handler.can_handle(created) is True
        assert handler.can_handle(shipped) is True
        assert handler.can_handle(cancelled) is False

        await handler.handle(created)
        assert len(events_handled) == 1
