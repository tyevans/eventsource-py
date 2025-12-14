"""
Comprehensive unit tests for the projection base classes.

Tests cover:
- Projection abstract base class
- SyncProjection abstract base class
- EventHandlerBase class
- CheckpointTrackingProjection with retry and DLQ
- DeclarativeProjection with @handles decorator
- Handler discovery and validation
"""

from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.exceptions import UnhandledEventError
from eventsource.handlers import handles
from eventsource.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    EventHandlerBase,
    Projection,
    SyncProjection,
)
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository


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


class OrderCancelled(DomainEvent):
    """Sample event for testing - not handled by any projection."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"
    reason: str = Field(..., description="Cancellation reason")


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
        events_handled = []

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
        events_handled = []

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
        events_handled = []

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


class TestCheckpointTrackingProjection:
    """Tests for CheckpointTrackingProjection."""

    @pytest.fixture
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        """Create an in-memory checkpoint repository."""
        return InMemoryCheckpointRepository()

    @pytest.fixture
    def dlq_repo(self) -> InMemoryDLQRepository:
        """Create an in-memory DLQ repository."""
        return InMemoryDLQRepository()

    def test_cannot_instantiate_without_subscribed_to(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """CheckpointTrackingProjection requires subscribed_to implementation."""

        class IncompleteProjection(CheckpointTrackingProjection):
            async def _process_event(self, event: DomainEvent) -> None:
                pass

        with pytest.raises(TypeError, match="abstract"):
            IncompleteProjection(checkpoint_repo=checkpoint_repo)  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_successful_event_processing_updates_checkpoint(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """Successful event processing updates checkpoint."""
        events_processed = []

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                events_processed.append(event)

        projection = TestProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(events_processed) == 1
        checkpoint = await checkpoint_repo.get_checkpoint("TestProjection")
        assert checkpoint == event.event_id

    @pytest.mark.asyncio
    async def test_failed_event_retries_with_backoff(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """Failed event processing retries with exponential backoff."""
        attempt_count = 0

        class FailingProjection(CheckpointTrackingProjection):
            MAX_RETRIES = 3
            RETRY_BACKOFF_BASE = 0  # No backoff for testing

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                nonlocal attempt_count
                attempt_count += 1
                raise ValueError("Processing failed")

        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with pytest.raises(ValueError, match="Processing failed"):
            await projection.handle(event)

        assert attempt_count == 3  # 3 attempts total

    @pytest.mark.asyncio
    async def test_failed_event_goes_to_dlq(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """Event goes to DLQ after all retries exhausted."""

        class FailingProjection(CheckpointTrackingProjection):
            MAX_RETRIES = 2
            RETRY_BACKOFF_BASE = 0

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                raise ValueError("Processing failed")

        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with pytest.raises(ValueError):
            await projection.handle(event)

        # Check DLQ
        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id
        assert failed_events[0].projection_name == "FailingProjection"

    @pytest.mark.asyncio
    async def test_get_checkpoint_returns_last_processed(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """get_checkpoint returns last processed event ID."""

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        # Initially no checkpoint
        assert await projection.get_checkpoint() is None

        # Process event
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        checkpoint = await projection.get_checkpoint()
        assert checkpoint == str(event.event_id)

    @pytest.mark.asyncio
    async def test_reset_clears_checkpoint(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """reset clears checkpoint."""

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        # Process event
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert await projection.get_checkpoint() is not None

        # Reset
        await projection.reset()

        assert await projection.get_checkpoint() is None

    @pytest.mark.asyncio
    async def test_projection_name_defaults_to_class_name(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """projection_name defaults to class name."""

        class MyCustomProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = MyCustomProjection(checkpoint_repo=checkpoint_repo)
        assert projection.projection_name == "MyCustomProjection"

    @pytest.mark.asyncio
    async def test_uses_default_in_memory_repos_when_none_provided(self) -> None:
        """Uses in-memory repositories by default."""

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection()  # No repos provided

        # Should work without error
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert await projection.get_checkpoint() is not None


class TestDeclarativeProjection:
    """Tests for DeclarativeProjection with @handles decorator."""

    @pytest.fixture
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        """Create an in-memory checkpoint repository."""
        return InMemoryCheckpointRepository()

    @pytest.fixture
    def dlq_repo(self) -> InMemoryDLQRepository:
        """Create an in-memory DLQ repository."""
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_handles_decorator_registers_handler(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """@handles decorator registers handler method."""
        events_handled = []

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = TestProjection(checkpoint_repo=checkpoint_repo)

        assert OrderCreated in projection.subscribed_to()

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(events_handled) == 1

    @pytest.mark.asyncio
    async def test_multiple_handlers(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Multiple @handles decorators work correctly."""
        created_events = []
        shipped_events = []

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                created_events.append(event)

            @handles(OrderShipped)
            async def _handle_order_shipped(self, event: OrderShipped) -> None:
                shipped_events.append(event)

        projection = TestProjection(checkpoint_repo=checkpoint_repo)

        assert len(projection.subscribed_to()) == 2
        assert OrderCreated in projection.subscribed_to()
        assert OrderShipped in projection.subscribed_to()

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        await projection.handle(created)
        await projection.handle(shipped)

        assert len(created_events) == 1
        assert len(shipped_events) == 1

    @pytest.mark.asyncio
    async def test_handler_with_two_params(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Handler with conn and event parameters works."""
        events_handled = []
        conn_values = []

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                conn_values.append(conn)
                events_handled.append(event)

        projection = TestProjection(checkpoint_repo=checkpoint_repo)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(events_handled) == 1
        # conn is None by default in base class
        assert conn_values[0] is None

    def test_non_async_handler_raises_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Non-async handler raises ValueError."""

        with pytest.raises(ValueError, match="must be async"):

            class BadProjection(DeclarativeProjection):
                @handles(OrderCreated)
                def _handle_order_created(self, event: OrderCreated) -> None:
                    pass

            BadProjection(checkpoint_repo=checkpoint_repo)

    def test_handler_with_wrong_param_count_raises_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Handler with wrong parameter count raises ValueError."""

        with pytest.raises(ValueError, match="1 or 2 parameters"):

            class BadProjection(DeclarativeProjection):
                @handles(OrderCreated)
                async def _handle_order_created(self, a, b, c: OrderCreated) -> None:
                    pass

            BadProjection(checkpoint_repo=checkpoint_repo)

    @pytest.mark.asyncio
    async def test_unhandled_event_ignored_by_default(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Unhandled event type is silently ignored by default (backwards compatible)."""
        import logging

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = TestProjection(checkpoint_repo=checkpoint_repo)

        # Try to handle an event type with no handler
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with caplog.at_level(logging.WARNING):
            await projection.handle(event)

        # Default mode is "ignore" - no warning should be logged
        assert not any("No handler registered" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_public_handler_method_discovered(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Public handler methods (without underscore) are discovered."""
        events_handled = []

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = TestProjection(checkpoint_repo=checkpoint_repo)

        assert OrderCreated in projection.subscribed_to()

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(events_handled) == 1


class TestDeclarativeProjectionWithDLQ:
    """Tests for DeclarativeProjection error handling and DLQ."""

    @pytest.fixture
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.fixture
    def dlq_repo(self) -> InMemoryDLQRepository:
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_failing_handler_goes_to_dlq(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """Failing handler sends event to DLQ."""

        class FailingProjection(DeclarativeProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                raise ValueError("Handler error")

        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with pytest.raises(ValueError):
            await projection.handle(event)

        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert "Handler error" in failed_events[0].error_message


class TestLagMetrics:
    """Tests for projection lag metrics."""

    @pytest.fixture
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_none_initially(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """get_lag_metrics returns None when no checkpoint exists."""

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(checkpoint_repo=checkpoint_repo)

        metrics = await projection.get_lag_metrics()
        assert metrics is None

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_data_after_processing(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """get_lag_metrics returns data after processing events."""

        class TestProjection(CheckpointTrackingProjection):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                pass

        projection = TestProjection(checkpoint_repo=checkpoint_repo)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        metrics = await projection.get_lag_metrics()
        assert metrics is not None
        assert metrics["projection_name"] == "TestProjection"
        assert metrics["events_processed"] == 1


class TestDatabaseProjection:
    """Tests for DatabaseProjection with database connection support.

    These tests verify that DatabaseProjection:
    - Provides real database connections to handlers
    - Wraps handler operations in transactions
    - Rolls back on errors
    - Maintains checkpoint after successful processing
    """

    @pytest.fixture
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        """Create an in-memory checkpoint repository."""
        return InMemoryCheckpointRepository()

    @pytest.fixture
    def dlq_repo(self) -> InMemoryDLQRepository:
        """Create an in-memory DLQ repository."""
        return InMemoryDLQRepository()

    @pytest.fixture
    def mock_session_factory(self):
        """Create a mock async session factory."""
        from unittest.mock import AsyncMock, MagicMock

        # Create mock connection
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()

        # Create mock session
        mock_session = AsyncMock()
        mock_session.connection = AsyncMock(return_value=mock_conn)

        # Mock the context managers
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        # Create mock transaction context manager
        mock_begin = MagicMock()
        mock_begin.__aenter__ = AsyncMock(return_value=None)
        mock_begin.__aexit__ = AsyncMock(return_value=None)
        mock_session.begin = MagicMock(return_value=mock_begin)

        # Create factory that returns the mock session
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_factory.__call__ = MagicMock(return_value=mock_session)

        return mock_factory, mock_session, mock_conn

    @pytest.mark.asyncio
    async def test_handler_receives_valid_connection(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """Handler with (conn, event) signature gets real connection."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory
        received_conn = []
        events_handled = []

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                received_conn.append(conn)
                events_handled.append(event)

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(events_handled) == 1
        assert len(received_conn) == 1
        # Connection should be the mock connection, not None
        assert received_conn[0] is mock_conn

    @pytest.mark.asyncio
    async def test_single_param_handler_still_works(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """Handler with (event) signature works without connection."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory
        events_handled = []

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        assert len(events_handled) == 1
        assert events_handled[0] == event

    @pytest.mark.asyncio
    async def test_checkpoint_updated_on_success(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """Checkpoint is updated after successful processing."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass  # Just succeed

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # Checkpoint should be updated
        checkpoint = await checkpoint_repo.get_checkpoint("TestProjection")
        assert checkpoint == event.event_id

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        """Checkpoint is NOT updated when handler raises error."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                raise ValueError("Handler error")

        projection = FailingProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with pytest.raises(ValueError, match="Handler error"):
            await projection.handle(event)

        # Checkpoint should NOT be updated
        checkpoint = await checkpoint_repo.get_checkpoint("FailingProjection")
        assert checkpoint is None

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        """Transaction is rolled back when handler raises error.

        This test verifies that the transaction context manager's __aexit__
        is called, which indicates the context is being properly exited
        (and will trigger rollback on exception in a real session).
        """
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        # Track whether the transaction context manager's __aexit__ was called
        exit_called = []
        original_aexit = session.begin.return_value.__aexit__

        async def capture_aexit(*args):
            exit_called.append(True)
            # Re-raise the exception to let it propagate properly
            if args[0] is not None:
                # An exception was passed in
                return False  # Don't suppress the exception
            return await original_aexit(*args)

        session.begin.return_value.__aexit__ = capture_aexit

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                raise ValueError("Handler error")

        projection = FailingProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with pytest.raises(ValueError):
            await projection.handle(event)

        # __aexit__ should have been called at least once (once per retry)
        # This confirms the transaction context manager was properly exited
        assert len(exit_called) >= 1

    @pytest.mark.asyncio
    async def test_multiple_handlers_share_same_connection(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """Different handlers in same projection get same connection."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory
        received_conns = []

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                received_conns.append(conn)

            @handles(OrderShipped)
            async def _handle_order_shipped(self, conn, event: OrderShipped) -> None:
                received_conns.append(conn)

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        event1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        event2 = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        await projection.handle(event1)
        await projection.handle(event2)

        # Both handlers should receive the same mock connection
        assert len(received_conns) == 2
        assert received_conns[0] is mock_conn
        assert received_conns[1] is mock_conn

    @pytest.mark.asyncio
    async def test_runtime_error_if_process_event_called_directly(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """RuntimeError if _process_event is called without handle() context."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        # Calling _process_event directly should raise RuntimeError
        with pytest.raises(RuntimeError, match="requires database connection"):
            await projection._process_event(event)

    @pytest.mark.asyncio
    async def test_unhandled_event_ignored_by_default(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
        caplog: "pytest.LogCaptureFixture",
    ) -> None:
        """Unhandled event type is silently ignored by default (backwards compatible)."""
        import logging

        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        # Try to handle an event type with no handler
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with caplog.at_level(logging.WARNING):
            await projection.handle(event)

        # Default mode is "ignore" - no warning should be logged
        assert not any("No handler registered" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_connection_is_none_after_handle_completes(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """Connection is cleared after handle() completes."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        # Before handle
        assert projection._current_connection is None

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)

        # After handle
        assert projection._current_connection is None

    @pytest.mark.asyncio
    async def test_connection_cleared_even_on_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        """Connection is cleared even when handler raises error."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                raise ValueError("Handler error")

        projection = FailingProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with pytest.raises(ValueError):
            await projection.handle(event)

        # Connection should still be cleared after error
        assert projection._current_connection is None

    @pytest.mark.asyncio
    async def test_mixed_handler_signatures(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """Projection can have both 1-param and 2-param handlers."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory
        one_param_events = []
        two_param_events = []
        received_conns = []

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                received_conns.append(conn)
                two_param_events.append(event)

            @handles(OrderShipped)
            async def _handle_order_shipped(self, event: OrderShipped) -> None:
                one_param_events.append(event)

        projection = TestProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        await projection.handle(created)
        await projection.handle(shipped)

        assert len(two_param_events) == 1
        assert len(one_param_events) == 1
        assert len(received_conns) == 1
        assert received_conns[0] is mock_conn

    def test_can_import_from_projections_module(self) -> None:
        """DatabaseProjection can be imported from projections module."""
        from eventsource.projections import DatabaseProjection as DBProjection

        assert DBProjection is not None

    def test_can_import_from_main_package(self) -> None:
        """DatabaseProjection can be imported from main eventsource package."""
        from eventsource import DatabaseProjection as DBProjection

        assert DBProjection is not None


# =============================================================================
# TD-012: Unregistered Event Handling Tests for Projections
# =============================================================================


class TestProjectionUnregisteredEventHandling:
    """Tests for configurable unregistered event handling in projections (TD-012).

    These tests verify:
    - AC1: Unhandled events raise UnhandledEventError when mode is "error"
    - AC2: Setting unregistered_event_handling="ignore" silently ignores events
    - AC3: Setting unregistered_event_handling="warn" logs warning
    - AC4: Error message lists available handlers
    - AC5: Default mode is "ignore" for backwards compatibility
    """

    @pytest.fixture
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        """Create an in-memory checkpoint repository."""
        return InMemoryCheckpointRepository()

    @pytest.fixture
    def dlq_repo(self) -> InMemoryDLQRepository:
        """Create an in-memory DLQ repository."""
        return InMemoryDLQRepository()

    def test_default_mode_is_ignore(self, checkpoint_repo: InMemoryCheckpointRepository) -> None:
        """AC5: Default mode should be 'ignore' for backwards compatibility."""

        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = TestProjection(checkpoint_repo=checkpoint_repo)
        assert projection.unregistered_event_handling == "ignore"

    @pytest.mark.asyncio
    async def test_ignore_mode_silently_accepts_unknown_events(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """AC2: ignore mode allows unknown events without raising."""

        class IgnoreProjection(DeclarativeProjection):
            unregistered_event_handling = "ignore"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = IgnoreProjection(checkpoint_repo=checkpoint_repo)

        # Handle an event with no handler - should not raise
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        await projection.handle(event)

        # Checkpoint should still be updated even for ignored events
        checkpoint = await projection.get_checkpoint()
        assert checkpoint == str(event.event_id)

    @pytest.mark.asyncio
    async def test_error_mode_raises_unhandled_event_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """AC1: error mode raises UnhandledEventError for unhandled events."""

        class StrictProjection(DeclarativeProjection):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = StrictProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        # Handle an event with no handler - should raise
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with pytest.raises(UnhandledEventError) as exc_info:
            await projection.handle(event)

        assert "OrderShipped" in str(exc_info.value)
        assert "StrictProjection" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_warn_mode_logs_warning(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """AC3: warn mode logs warning for unhandled events."""
        import logging

        class WarnProjection(DeclarativeProjection):
            unregistered_event_handling = "warn"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = WarnProjection(checkpoint_repo=checkpoint_repo)

        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with caplog.at_level(logging.WARNING):
            await projection.handle(event)

        # Check warning was logged
        assert "No handler registered" in caplog.text
        assert "OrderShipped" in caplog.text

        # Checkpoint should still be updated
        checkpoint = await projection.get_checkpoint()
        assert checkpoint == str(event.event_id)

    @pytest.mark.asyncio
    async def test_error_message_includes_available_handlers(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        """AC4: Error message lists handlers that ARE available."""

        class MultiHandlerProjection(DeclarativeProjection):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

            @handles(OrderCancelled)
            async def _handle_order_cancelled(self, event: OrderCancelled) -> None:
                pass

        projection = MultiHandlerProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with pytest.raises(UnhandledEventError) as exc_info:
            await projection.handle(event)

        error = exc_info.value
        assert "OrderCreated" in error.available_handlers
        assert "OrderCancelled" in error.available_handlers
        assert error.event_type == "OrderShipped"
        assert error.handler_class == "MultiHandlerProjection"

    @pytest.mark.asyncio
    async def test_handled_event_is_not_affected_by_mode(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """Events with handlers work regardless of handling mode."""
        events_handled = []

        class StrictProjection(DeclarativeProjection):
            unregistered_event_handling = "error"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = StrictProjection(checkpoint_repo=checkpoint_repo)
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        # Should work fine - handler exists
        await projection.handle(event)
        assert len(events_handled) == 1

    def test_subclass_can_override_handling_mode(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """Subclasses can override the handling mode."""

        class BaseProjection(DeclarativeProjection):
            unregistered_event_handling = "ignore"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        class StrictSubclass(BaseProjection):
            unregistered_event_handling = "error"

        base = BaseProjection(checkpoint_repo=checkpoint_repo)
        strict = StrictSubclass(checkpoint_repo=checkpoint_repo)

        assert base.unregistered_event_handling == "ignore"
        assert strict.unregistered_event_handling == "error"

    @pytest.mark.asyncio
    async def test_backwards_compatibility_existing_warning_behavior(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Existing code with default mode should work (no exception raised)."""
        import logging

        class LegacyProjection(DeclarativeProjection):
            # No explicit mode set - uses default "ignore"
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = LegacyProjection(checkpoint_repo=checkpoint_repo)

        # Handle an event with no handler
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with caplog.at_level(logging.WARNING):
            # Should not raise - backwards compatible
            await projection.handle(event)

        # No warning should be logged in ignore mode
        assert "No handler registered" not in caplog.text


class TestDatabaseProjectionUnregisteredEventHandling:
    """Tests for unregistered event handling in DatabaseProjection."""

    @pytest.fixture
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.fixture
    def dlq_repo(self) -> InMemoryDLQRepository:
        return InMemoryDLQRepository()

    @pytest.fixture
    def mock_session_factory(self):
        """Create a mock async session factory."""
        from unittest.mock import AsyncMock, MagicMock

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

    @pytest.mark.asyncio
    async def test_database_projection_error_mode(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        """DatabaseProjection respects unregistered_event_handling='error'."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class StrictDBProjection(DatabaseProjection):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = StrictDBProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with pytest.raises(UnhandledEventError) as exc_info:
            await projection.handle(event)

        assert "OrderShipped" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_database_projection_ignore_mode(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        """DatabaseProjection respects unregistered_event_handling='ignore'."""
        from eventsource.projections.base import DatabaseProjection

        factory, session, mock_conn = mock_session_factory

        class IgnoreDBProjection(DatabaseProjection):
            unregistered_event_handling = "ignore"

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = IgnoreDBProjection(
            session_factory=factory,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        # Should not raise
        await projection.handle(event)

        # Checkpoint should still be updated
        checkpoint = await projection.get_checkpoint()
        assert checkpoint == str(event.event_id)
