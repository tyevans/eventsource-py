"""
Unit tests for projection tracing support (O11Y-013).

Tests for:
- CheckpointTrackingProjection tracing
- DeclarativeProjection handler dispatch tracing
- ProjectionRegistry dispatch tracing
- ProjectionCoordinator tracing
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
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


class TestCheckpointTrackingProjectionTracing:
    """Tests for CheckpointTrackingProjection tracing support."""

    def test_enable_tracing_parameter_default_false(self):
        """enable_tracing defaults to False for projections."""
        from eventsource.projections.base import CheckpointTrackingProjection

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection()

        # Tracing should be off by default
        assert projection._enable_tracing is False
        # With composition-based tracing, _tracer is always set
        # but it will be a NullTracer when disabled
        assert projection._tracer is not None
        assert projection._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        """enable_tracing=True enables tracing."""
        from eventsource.projections.base import CheckpointTrackingProjection

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=True)

        # Tracing should be enabled
        assert projection._enable_tracing is True
        assert projection._tracer is not None

    @pytest.mark.asyncio
    async def test_handle_creates_span_when_enabled(self):
        """handle() creates span when tracing enabled."""
        from eventsource.projections.base import CheckpointTrackingProjection

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # Verify span was created with correct name and attributes
        mock_tracer.span.assert_called_once()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "eventsource.projection.handle"
        attrs = call_args[0][1]
        assert attrs[ATTR_PROJECTION_NAME] == "TestProjection"
        assert attrs[ATTR_EVENT_TYPE] == "OrderCreated"
        assert ATTR_EVENT_ID in attrs

    @pytest.mark.asyncio
    async def test_handle_no_span_when_disabled(self):
        """handle() does not create span when tracing disabled."""
        from eventsource.projections.base import CheckpointTrackingProjection

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=False)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # With composition-based tracing, _tracer is always set but disabled
        assert projection._tracer is not None
        assert projection._tracer.enabled is False

    @pytest.mark.asyncio
    async def test_span_sets_checkpoint_updated_on_success(self):
        """Successful event processing sets checkpoint.updated attribute."""
        from eventsource.projections.base import CheckpointTrackingProjection

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # Verify checkpoint.updated attribute was set
        mock_span.set_attribute.assert_called_with("checkpoint.updated", True)

    @pytest.mark.asyncio
    async def test_span_sets_retry_count_on_failure(self):
        """Failed event processing sets retry count attribute."""
        from eventsource.projections.base import CheckpointTrackingProjection
        from eventsource.projections.retry import ExponentialBackoffRetryPolicy
        from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
        from eventsource.repositories.dlq import InMemoryDLQRepository
        from eventsource.subscriptions.retry import RetryConfig

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class FailingProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                raise ValueError("Test error")

        # Create a retry policy with 2 retries (3 total attempts) and minimal backoff
        retry_policy = ExponentialBackoffRetryPolicy(
            config=RetryConfig(
                max_retries=2,
                initial_delay=0.001,  # Minimal delay
                exponential_base=1.01,  # Must be > 1.0
                jitter=0.0,
            )
        )

        projection = FailingProjection(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
            retry_policy=retry_policy,
            enable_tracing=True,
        )
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        # The event will go to DLQ after exhausting retries, then raise
        with pytest.raises(ValueError):
            await projection.handle(event)

        # Verify retry count attribute was set (3 attempts = max_retries + 1)
        mock_span.set_attribute.assert_called_with(ATTR_RETRY_COUNT, 3)


class TestDeclarativeProjectionTracing:
    """Tests for DeclarativeProjection tracing support."""

    def test_enable_tracing_passed_through(self):
        """enable_tracing is passed to parent class."""
        from eventsource.handlers import handles
        from eventsource.projections.base import DeclarativeProjection

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
        from eventsource.handlers import handles
        from eventsource.projections.base import DeclarativeProjection

        mock_tracer = Mock()
        mock_span_handle = MagicMock()
        mock_span_handler = MagicMock()
        # Create separate context managers for each span call
        mock_span_cm_handle = MagicMock()
        mock_span_cm_handle.__enter__ = Mock(return_value=mock_span_handle)
        mock_span_cm_handle.__exit__ = Mock(return_value=None)
        mock_span_cm_handler = MagicMock()
        mock_span_cm_handler.__enter__ = Mock(return_value=mock_span_handler)
        mock_span_cm_handler.__exit__ = Mock(return_value=None)
        # First call for handle(), second call for handler dispatch
        mock_tracer.span.side_effect = [mock_span_cm_handle, mock_span_cm_handler]
        mock_tracer.enabled = True

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = TestProjection(enable_tracing=True)
        projection._tracer = mock_tracer

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # Verify at least two spans were created (handle + handler dispatch)
        assert mock_tracer.span.call_count == 2

        # Check handler dispatch span
        calls = mock_tracer.span.call_args_list
        handler_call = calls[1]
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
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()

        mock_session = AsyncMock()
        mock_session.connection = AsyncMock(return_value=mock_conn)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_begin = MagicMock()
        mock_begin.__aenter__ = AsyncMock(return_value=None)
        mock_begin.__aexit__ = AsyncMock(return_value=None)
        mock_session.begin = MagicMock(return_value=mock_begin)

        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_factory.__call__ = MagicMock(return_value=mock_session)

        return mock_factory, mock_session, mock_conn

    def test_enable_tracing_passed_through(self, mock_session_factory):
        """enable_tracing is passed to parent class."""
        from eventsource.handlers import handles
        from eventsource.projections.base import DatabaseProjection

        factory, _, _ = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            enable_tracing=True,
        )

        assert projection._enable_tracing is True
        assert projection._tracer is not None


class TestProjectionRegistryTracing:
    """Tests for ProjectionRegistry tracing support."""

    def test_enable_tracing_parameter_default_false(self):
        """enable_tracing defaults to False."""
        from eventsource.projections.coordinator import ProjectionRegistry

        registry = ProjectionRegistry()

        assert registry._enable_tracing is False
        # With composition-based tracing, _tracer is always set but disabled
        assert registry._tracer is not None
        assert registry._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        """enable_tracing=True enables tracing."""
        from eventsource.projections.coordinator import ProjectionRegistry

        registry = ProjectionRegistry(enable_tracing=True)

        assert registry._enable_tracing is True
        assert registry._tracer is not None

    @pytest.mark.asyncio
    async def test_dispatch_creates_span_when_enabled(self):
        """dispatch() creates span when tracing enabled."""
        from eventsource.projections.base import Projection
        from eventsource.projections.coordinator import ProjectionRegistry

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry(enable_tracing=True)
        registry._tracer = mock_tracer
        registry.register_projection(TestProjection())

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

        # Verify span was created
        mock_tracer.span.assert_called()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "eventsource.projection.registry.dispatch"

    @pytest.mark.asyncio
    async def test_dispatch_no_span_when_disabled(self):
        """dispatch() does not create span when tracing disabled."""
        from eventsource.projections.base import Projection
        from eventsource.projections.coordinator import ProjectionRegistry

        handled_events = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry(enable_tracing=False)
        registry.register_projection(TestProjection())

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

        # Event should still be dispatched
        assert len(handled_events) == 1
        # With composition-based tracing, _tracer is always set but disabled
        assert registry._tracer is not None
        assert registry._tracer.enabled is False


class TestProjectionCoordinatorTracing:
    """Tests for ProjectionCoordinator tracing support."""

    def test_enable_tracing_parameter_default_false(self):
        """enable_tracing defaults to False."""
        from eventsource.projections.coordinator import (
            ProjectionCoordinator,
            ProjectionRegistry,
        )

        registry = ProjectionRegistry()
        coordinator = ProjectionCoordinator(registry=registry)

        assert coordinator._enable_tracing is False
        # With composition-based tracing, _tracer is always set but disabled
        assert coordinator._tracer is not None
        assert coordinator._tracer.enabled is False

    def test_enable_tracing_parameter_true(self):
        """enable_tracing=True enables tracing."""
        from eventsource.projections.coordinator import (
            ProjectionCoordinator,
            ProjectionRegistry,
        )

        registry = ProjectionRegistry()
        coordinator = ProjectionCoordinator(registry=registry, enable_tracing=True)

        assert coordinator._enable_tracing is True
        assert coordinator._tracer is not None

    @pytest.mark.asyncio
    async def test_dispatch_events_creates_span_when_enabled(self):
        """dispatch_events() creates span when tracing enabled."""
        from eventsource.projections.coordinator import (
            ProjectionCoordinator,
            ProjectionRegistry,
        )

        mock_tracer = Mock()
        mock_span = MagicMock()
        mock_span_cm = MagicMock()
        mock_span_cm.__enter__ = Mock(return_value=mock_span)
        mock_span_cm.__exit__ = Mock(return_value=None)
        mock_tracer.span.return_value = mock_span_cm
        mock_tracer.enabled = True

        registry = ProjectionRegistry()
        coordinator = ProjectionCoordinator(registry=registry, enable_tracing=True)
        coordinator._tracer = mock_tracer

        events = [OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")]
        await coordinator.dispatch_events(events)

        # Verify span was created
        mock_tracer.span.assert_called()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "eventsource.projection.coordinate"


class TestBackwardCompatibility:
    """Tests ensuring backward compatibility with existing code."""

    @pytest.mark.asyncio
    async def test_checkpoint_projection_without_tracing_arg_works(self):
        """CheckpointTrackingProjection works without enable_tracing arg."""
        from eventsource.projections.base import CheckpointTrackingProjection

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        # Should work without enable_tracing argument
        projection = TestProjection()

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # Checkpoint should be updated
        checkpoint = await projection.get_checkpoint()
        assert checkpoint is not None

    @pytest.mark.asyncio
    async def test_declarative_projection_without_tracing_arg_works(self):
        """DeclarativeProjection works without enable_tracing arg."""
        from eventsource.handlers import handles
        from eventsource.projections.base import DeclarativeProjection

        handled_events = []

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                handled_events.append(event)

        # Should work without enable_tracing argument
        projection = TestProjection()

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(handled_events) == 1

    @pytest.mark.asyncio
    async def test_registry_without_tracing_arg_works(self):
        """ProjectionRegistry works without enable_tracing arg."""
        from eventsource.projections.base import Projection
        from eventsource.projections.coordinator import ProjectionRegistry

        handled_events = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

            async def reset(self) -> None:
                pass

        # Should work without enable_tracing argument
        registry = ProjectionRegistry()
        registry.register_projection(TestProjection())

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

        assert len(handled_events) == 1

    @pytest.mark.asyncio
    async def test_coordinator_without_tracing_arg_works(self):
        """ProjectionCoordinator works without enable_tracing arg."""
        from eventsource.projections.coordinator import (
            ProjectionCoordinator,
            ProjectionRegistry,
        )

        # Should work without enable_tracing argument
        registry = ProjectionRegistry()
        coordinator = ProjectionCoordinator(registry=registry)

        events = [OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")]
        count = await coordinator.dispatch_events(events)

        assert count == 1
