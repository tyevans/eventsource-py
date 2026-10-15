"""Tests for projection tracing: declarative, database, registry, and coordinator."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    Projection,
)
from eventsource.application.projections.coordinator import (
    ProjectionCoordinator,
    ProjectionRegistry,
)
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
)


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


def _make_span_cm(span: MagicMock | None = None) -> MagicMock:
    return MagicMock(
        __enter__=Mock(return_value=span or MagicMock()),
        __exit__=Mock(return_value=None),
    )


def _order_event() -> OrderCreated:
    return OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")


class _RecordingProjection(Projection):
    def __init__(self, events: list[DomainEvent] | None = None) -> None:
        self.events = events if events is not None else []

    async def handle(self, event: DomainEvent) -> None:
        self.events.append(event)

    async def reset(self) -> None:
        pass


class _DeclarativeOrderProjection(DeclarativeProjection):
    def __init__(self, events: list[DomainEvent] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.events = events if events is not None else []

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.events.append(event)


class TestDeclarativeProjectionTracing:
    def test_enable_tracing_passed_through(self):
        projection = _DeclarativeOrderProjection(enable_tracing=True)
        assert projection._enable_tracing is True
        assert projection._tracer is not None

    @pytest.mark.asyncio
    async def test_handler_dispatch_creates_span(self):
        mock_tracer = Mock(enabled=True)
        mock_tracer.span.side_effect = [_make_span_cm(), _make_span_cm()]

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer
        await projection.handle(_order_event())

        assert mock_tracer.span.call_count == 2
        handler_call = mock_tracer.span.call_args_list[1]
        assert handler_call[0][0] == "eventsource.projection.handler"
        attrs = handler_call[0][1]
        assert attrs[ATTR_PROJECTION_NAME] == "TestProjection"
        assert attrs[ATTR_EVENT_TYPE] == "OrderCreated"
        assert attrs[ATTR_HANDLER_NAME] == "_handle_order_created"


class TestDatabaseProjectionTracing:
    @pytest.fixture
    def mock_session_factory(self):
        mock_conn = AsyncMock(execute=AsyncMock())
        mock_session = AsyncMock(connection=AsyncMock(return_value=mock_conn))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_begin = MagicMock(
            __aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=None)
        )
        mock_session.begin = MagicMock(return_value=mock_begin)
        mock_factory = MagicMock(return_value=mock_session)
        mock_factory.__call__ = MagicMock(return_value=mock_session)
        return mock_factory, mock_session, mock_conn

    def test_enable_tracing_passed_through(self, mock_session_factory):
        factory, _, _ = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = TestProjection(session_factory=factory, enable_tracing=True)
        assert projection._enable_tracing is True
        assert projection._tracer is not None


class TestProjectionRegistryTracing:
    def test_enable_tracing_parameter_default_false(self):
        registry = ProjectionRegistry()
        assert registry._enable_tracing is False
        assert registry._tracer is not None and registry._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        registry = ProjectionRegistry(enable_tracing=True)
        assert registry._enable_tracing is True and registry._tracer is not None

    @pytest.mark.asyncio
    async def test_dispatch_creates_span_when_enabled(self):
        mock_tracer = Mock(enabled=True)
        mock_tracer.span.return_value = _make_span_cm()
        registry = ProjectionRegistry(enable_tracing=True)
        registry._tracer = mock_tracer
        registry.register_projection(_RecordingProjection())
        await registry.dispatch(_order_event())

        mock_tracer.span.assert_called()
        assert mock_tracer.span.call_args[0][0] == "eventsource.projection.registry.dispatch"

    @pytest.mark.asyncio
    async def test_dispatch_no_span_when_disabled(self):
        handled_events: list[DomainEvent] = []
        registry = ProjectionRegistry(enable_tracing=False)
        registry.register_projection(_RecordingProjection(handled_events))
        await registry.dispatch(_order_event())

        assert len(handled_events) == 1
        assert registry._tracer is not None and registry._tracer.enabled is False


class TestProjectionCoordinatorTracing:
    def test_enable_tracing_parameter_default_false(self):
        coordinator = ProjectionCoordinator(registry=ProjectionRegistry())
        assert coordinator._enable_tracing is False
        assert coordinator._tracer is not None and coordinator._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        coordinator = ProjectionCoordinator(registry=ProjectionRegistry(), enable_tracing=True)
        assert coordinator._enable_tracing is True and coordinator._tracer is not None

    @pytest.mark.asyncio
    async def test_dispatch_events_creates_span_when_enabled(self):
        mock_tracer = Mock(enabled=True)
        mock_tracer.span.return_value = _make_span_cm()
        coordinator = ProjectionCoordinator(registry=ProjectionRegistry(), enable_tracing=True)
        coordinator._tracer = mock_tracer
        await coordinator.dispatch_events([_order_event()])

        mock_tracer.span.assert_called()
        assert mock_tracer.span.call_args[0][0] == "eventsource.projection.coordinate"


class TestBackwardCompatibility:
    @pytest.mark.asyncio
    async def test_checkpoint_projection_without_tracing_arg_works(self):
        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(checkpoint_repo=InMemoryCheckpointRepository())
        await projection.handle(_order_event())
        assert await projection.get_checkpoint() is not None

    @pytest.mark.asyncio
    async def test_declarative_projection_without_tracing_arg_works(self):
        handled_events: list[DomainEvent] = []
        projection = _DeclarativeOrderProjection(handled_events)
        await projection.handle(_order_event())
        assert len(handled_events) == 1

    @pytest.mark.asyncio
    async def test_registry_without_tracing_arg_works(self):
        handled_events: list[DomainEvent] = []
        registry = ProjectionRegistry()
        registry.register_projection(_RecordingProjection(handled_events))
        await registry.dispatch(_order_event())
        assert len(handled_events) == 1

    @pytest.mark.asyncio
    async def test_coordinator_without_tracing_arg_works(self):
        coordinator = ProjectionCoordinator(registry=ProjectionRegistry())
        assert await coordinator.dispatch_events([_order_event()]) == 1
