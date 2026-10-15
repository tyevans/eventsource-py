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
    """Sample event for testing."""

    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


def _make_span_cm(span: MagicMock | None = None) -> MagicMock:
    cm = MagicMock()
    cm.__enter__ = Mock(return_value=span or MagicMock())
    cm.__exit__ = Mock(return_value=None)
    return cm


class TestDeclarativeProjectionTracing:
    """Tests for DeclarativeProjection tracing support."""

    def test_enable_tracing_passed_through(self):
        """enable_tracing is passed to parent class."""

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        assert projection._enable_tracing is True
        assert projection._tracer is not None

    @pytest.mark.asyncio
    async def test_handler_dispatch_creates_span(self):
        """Handler dispatch creates span when tracing enabled."""
        mock_tracer = Mock(enabled=True)
        mock_tracer.span.side_effect = [_make_span_cm(), _make_span_cm()]

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer

        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))

        assert mock_tracer.span.call_count == 2
        handler_call = mock_tracer.span.call_args_list[1]
        assert handler_call[0][0] == "eventsource.projection.handler"
        attrs = handler_call[0][1]
        assert attrs[ATTR_PROJECTION_NAME] == "TestProjection"
        assert attrs[ATTR_EVENT_TYPE] == "OrderCreated"
        assert attrs[ATTR_HANDLER_NAME] == "_handle_order_created"


class TestDatabaseProjectionTracing:
    """Tests for DatabaseProjection tracing support."""

    @pytest.fixture
    def mock_session_factory(self):
        """Create a mock async session factory."""
        mock_conn = AsyncMock(execute=AsyncMock())
        mock_session = AsyncMock(connection=AsyncMock(return_value=mock_conn))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_begin = MagicMock(
            __aenter__=AsyncMock(return_value=None),
            __aexit__=AsyncMock(return_value=None),
        )
        mock_session.begin = MagicMock(return_value=mock_begin)
        mock_factory = MagicMock(return_value=mock_session)
        mock_factory.__call__ = MagicMock(return_value=mock_session)
        return mock_factory, mock_session, mock_conn

    def test_enable_tracing_passed_through(self, mock_session_factory):
        """enable_tracing is passed to parent class."""
        factory, _, _ = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = TestProjection(session_factory=factory, enable_tracing=True)
        assert projection._enable_tracing is True
        assert projection._tracer is not None


class TestProjectionRegistryTracing:
    """Tests for ProjectionRegistry tracing support."""

    def test_enable_tracing_parameter_default_false(self):
        """enable_tracing defaults to False."""
        registry = ProjectionRegistry()
        assert registry._enable_tracing is False
        assert registry._tracer is not None
        assert registry._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        """enable_tracing=True enables tracing."""
        registry = ProjectionRegistry(enable_tracing=True)
        assert registry._enable_tracing is True
        assert registry._tracer is not None

    @pytest.mark.asyncio
    async def test_dispatch_creates_span_when_enabled(self):
        """dispatch() creates span when tracing enabled."""
        mock_tracer = Mock(enabled=True)
        mock_tracer.span.return_value = _make_span_cm()

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry(enable_tracing=True)
        registry._tracer = mock_tracer
        registry.register_projection(TestProjection())

        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))

        mock_tracer.span.assert_called()
        assert mock_tracer.span.call_args[0][0] == "eventsource.projection.registry.dispatch"

    @pytest.mark.asyncio
    async def test_dispatch_no_span_when_disabled(self):
        """dispatch() does not create span when tracing disabled."""
        handled_events = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry(enable_tracing=False)
        registry.register_projection(TestProjection())

        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))

        assert len(handled_events) == 1
        assert registry._tracer is not None
        assert registry._tracer.enabled is False


class TestProjectionCoordinatorTracing:
    """Tests for ProjectionCoordinator tracing support."""

    def test_enable_tracing_parameter_default_false(self):
        """enable_tracing defaults to False."""
        coordinator = ProjectionCoordinator(registry=ProjectionRegistry())
        assert coordinator._enable_tracing is False
        assert coordinator._tracer is not None
        assert coordinator._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        """enable_tracing=True enables tracing."""
        coordinator = ProjectionCoordinator(registry=ProjectionRegistry(), enable_tracing=True)
        assert coordinator._enable_tracing is True
        assert coordinator._tracer is not None

    @pytest.mark.asyncio
    async def test_dispatch_events_creates_span_when_enabled(self):
        """dispatch_events() creates span when tracing enabled."""
        mock_tracer = Mock(enabled=True)
        mock_tracer.span.return_value = _make_span_cm()

        coordinator = ProjectionCoordinator(registry=ProjectionRegistry(), enable_tracing=True)
        coordinator._tracer = mock_tracer

        await coordinator.dispatch_events(
            [OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")]
        )

        mock_tracer.span.assert_called()
        assert mock_tracer.span.call_args[0][0] == "eventsource.projection.coordinate"


class TestBackwardCompatibility:
    """Tests ensuring backward compatibility with existing code."""

    @pytest.mark.asyncio
    async def test_checkpoint_projection_without_tracing_arg_works(self):
        """CheckpointTrackingProjection works without enable_tracing arg."""

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(checkpoint_repo=InMemoryCheckpointRepository())
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert await projection.get_checkpoint() is not None

    @pytest.mark.asyncio
    async def test_declarative_projection_without_tracing_arg_works(self):
        """DeclarativeProjection works without enable_tracing arg."""
        handled_events = []

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                handled_events.append(event)

        projection = TestProjection()
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(handled_events) == 1

    @pytest.mark.asyncio
    async def test_registry_without_tracing_arg_works(self):
        """ProjectionRegistry works without enable_tracing arg."""
        handled_events = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry()
        registry.register_projection(TestProjection())
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(handled_events) == 1

    @pytest.mark.asyncio
    async def test_coordinator_without_tracing_arg_works(self):
        """ProjectionCoordinator works without enable_tracing arg."""
        coordinator = ProjectionCoordinator(registry=ProjectionRegistry())
        count = await coordinator.dispatch_events(
            [OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")]
        )
        assert count == 1
