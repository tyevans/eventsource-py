"""
Comprehensive unit tests for ReadModelProjection.

These tests verify:
- Constructor properly initializes model class and inherits from DatabaseProjection
- Handle method creates repository and passes it to handlers
- Repository type is auto-detected from dialect (PostgreSQL/SQLite)
- Single-parameter handlers still work (event only)
- Two-parameter handlers receive (repo, event)
- Truncate uses repository's truncate method
- Compatible with existing @handles decorator
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.handlers import handles
from eventsource.readmodels import ReadModel, ReadModelProjection
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository

# =============================================================================
# Test Events
# =============================================================================


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


# =============================================================================
# Test Models
# =============================================================================


class OrderSummary(ReadModel):
    """Test model for projection tests."""

    order_number: str
    status: str
    total_amount: Decimal = Decimal("0.00")


class CustomTableModel(ReadModel):
    """Test model with custom table name."""

    __table_name__ = "custom_orders"
    name: str


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create an in-memory checkpoint repository."""
    return InMemoryCheckpointRepository()


@pytest.fixture
def dlq_repo() -> InMemoryDLQRepository:
    """Create an in-memory DLQ repository."""
    return InMemoryDLQRepository()


@pytest.fixture
def mock_postgresql_session_factory():
    """Create a mock async session factory for PostgreSQL."""
    # Create mock connection with PostgreSQL dialect
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_conn.dialect = MagicMock()
    mock_conn.dialect.name = "postgresql"

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


@pytest.fixture
def mock_sqlite_session_factory():
    """Create a mock async session factory for SQLite."""
    from aiosqlite import Connection as AiosqliteConnection

    # Create mock raw aiosqlite connection (spec it to pass isinstance check)
    mock_driver_conn = MagicMock(spec=AiosqliteConnection)
    mock_raw_conn = MagicMock()
    mock_raw_conn.driver_connection = mock_driver_conn

    # Create mock connection with SQLite dialect
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_conn.dialect = MagicMock()
    mock_conn.dialect.name = "sqlite"
    mock_conn.get_raw_connection = AsyncMock(return_value=mock_raw_conn)

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


# =============================================================================
# Test Classes
# =============================================================================


class TestReadModelProjectionConstruction:
    """Tests for ReadModelProjection construction and initialization."""

    def test_constructor_sets_model_class(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that constructor stores model class."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection._model_class == OrderSummary

    def test_model_class_property(self, mock_postgresql_session_factory, checkpoint_repo) -> None:
        """Test model_class property returns correct class."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection.model_class == OrderSummary

    def test_inherits_from_database_projection(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that ReadModelProjection inherits from DatabaseProjection."""
        from eventsource.projections.base import DatabaseProjection

        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert isinstance(projection, DatabaseProjection)

    def test_projection_name_defaults_to_class_name(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test projection_name defaults to class name."""
        factory, _, _ = mock_postgresql_session_factory

        class MyOrderProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = MyOrderProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection.projection_name == "MyOrderProjection"

    def test_repository_is_none_outside_handle_context(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test repository property is None outside of handle() context."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection.repository is None

    def test_repr_includes_model_info(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test __repr__ includes model class and table name."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        repr_str = repr(projection)
        assert "TestProjection" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str


class TestReadModelProjectionHandlerRouting:
    """Tests for handler routing and repository injection."""

    @pytest.mark.asyncio
    async def test_handler_receives_repository(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that handlers receive repository as context."""
        factory, _, _ = mock_postgresql_session_factory
        received_repos = []
        events_handled = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                received_repos.append(repo)
                events_handled.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance

            await projection.handle(event)

            assert len(events_handled) == 1
            assert len(received_repos) == 1
            assert received_repos[0] is mock_repo_instance

    @pytest.mark.asyncio
    async def test_single_param_handler_still_works(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that single-parameter handlers (event only) still work."""
        factory, _, _ = mock_postgresql_session_factory
        events_handled = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            await projection.handle(event)

            assert len(events_handled) == 1
            assert events_handled[0] == event

    @pytest.mark.asyncio
    async def test_multiple_handlers(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test multiple handlers work correctly."""
        factory, _, _ = mock_postgresql_session_factory
        created_events = []
        shipped_events = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                created_events.append(event)

            @handles(OrderShipped)
            async def _handle_order_shipped(self, repo, event: OrderShipped) -> None:
                shipped_events.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert len(projection.subscribed_to()) == 2
        assert OrderCreated in projection.subscribed_to()
        assert OrderShipped in projection.subscribed_to()

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

            await projection.handle(created)
            await projection.handle(shipped)

            assert len(created_events) == 1
            assert len(shipped_events) == 1

    @pytest.mark.asyncio
    async def test_mixed_handler_signatures(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test projection can have both 1-param and 2-param handlers."""
        factory, _, _ = mock_postgresql_session_factory
        one_param_events = []
        two_param_events = []
        received_repos = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                received_repos.append(repo)
                two_param_events.append(event)

            @handles(OrderShipped)
            async def _handle_order_shipped(self, event: OrderShipped) -> None:
                one_param_events.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance

            created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

            await projection.handle(created)
            await projection.handle(shipped)

            assert len(two_param_events) == 1
            assert len(one_param_events) == 1
            assert len(received_repos) == 1
            assert received_repos[0] is mock_repo_instance


class TestDialectDetection:
    """Tests for automatic repository type detection from dialect."""

    @pytest.mark.asyncio
    async def test_postgresql_dialect_creates_postgresql_repo(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test PostgreSQL dialect creates PostgreSQLReadModelRepository."""
        factory, _, mock_conn = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance

            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            await projection.handle(event)

            mock_repo.assert_called_once_with(
                mock_conn,
                OrderSummary,
                enable_tracing=False,
            )

    @pytest.mark.asyncio
    async def test_sqlite_dialect_creates_sqlite_repo(
        self, mock_sqlite_session_factory, checkpoint_repo
    ) -> None:
        """Test SQLite dialect creates SQLiteReadModelRepository."""
        factory, _, mock_conn = mock_sqlite_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.sqlite.SQLiteReadModelRepository") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance

            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            await projection.handle(event)

            # SQLite repo should receive the raw driver connection
            raw_conn = (await mock_conn.get_raw_connection()).driver_connection
            mock_repo.assert_called_once_with(
                raw_conn,
                OrderSummary,
                enable_tracing=False,
            )

    @pytest.mark.asyncio
    async def test_unknown_dialect_falls_back_to_postgresql(
        self, mock_postgresql_session_factory, checkpoint_repo, caplog
    ) -> None:
        """Test unknown dialect falls back to PostgreSQL repository with warning."""
        import logging

        factory, _, mock_conn = mock_postgresql_session_factory
        # Change dialect to something unknown
        mock_conn.dialect.name = "oracle"

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance

            with caplog.at_level(logging.WARNING):
                event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
                await projection.handle(event)

            # Should fall back to PostgreSQL
            mock_repo.assert_called()
            assert "Unknown dialect" in caplog.text or any(
                "oracle" in record.message.lower() for record in caplog.records
            )


class TestTruncateReadModels:
    """Tests for _truncate_read_models method."""

    @pytest.mark.asyncio
    async def test_truncate_calls_repository_truncate(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test _truncate_read_models uses repository's truncate method."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.truncate = AsyncMock(return_value=10)
            mock_repo.return_value = mock_repo_instance

            await projection._truncate_read_models()

            mock_repo_instance.truncate.assert_called_once()

    @pytest.mark.asyncio
    async def test_reset_calls_truncate_read_models(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test reset() calls _truncate_read_models."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository") as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.truncate = AsyncMock(return_value=5)
            mock_repo.return_value = mock_repo_instance

            await projection.reset()

            mock_repo_instance.truncate.assert_called_once()

            # Checkpoint should also be reset
            checkpoint = await checkpoint_repo.get_checkpoint("TestProjection")
            assert checkpoint is None


class TestRuntimeErrors:
    """Tests for runtime error handling."""

    @pytest.mark.asyncio
    async def test_runtime_error_if_process_event_called_directly(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test RuntimeError if _process_event is called without handle() context."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        # Calling _process_event directly should raise RuntimeError
        with pytest.raises(RuntimeError, match="requires repository"):
            await projection._process_event(event)


class TestCheckpointBehavior:
    """Tests for checkpoint tracking behavior."""

    @pytest.mark.asyncio
    async def test_checkpoint_updated_on_success(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test checkpoint is updated after successful processing."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            await projection.handle(event)

            # Checkpoint should be updated
            checkpoint = await checkpoint_repo.get_checkpoint("TestProjection")
            assert checkpoint == event.event_id

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_error(
        self, mock_postgresql_session_factory, checkpoint_repo, dlq_repo
    ) -> None:
        """Test checkpoint is NOT updated when handler raises error."""
        factory, _, _ = mock_postgresql_session_factory

        class FailingProjection(ReadModelProjection[OrderSummary]):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                raise ValueError("Handler error")

        projection = FailingProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

            with pytest.raises(ValueError, match="Handler error"):
                await projection.handle(event)

            # Checkpoint should NOT be updated
            checkpoint = await checkpoint_repo.get_checkpoint("FailingProjection")
            assert checkpoint is None


class TestRepositoryCleanup:
    """Tests for repository cleanup after handle() completes."""

    @pytest.mark.asyncio
    async def test_repository_is_none_after_handle_completes(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test repository is cleared after handle() completes."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        # Before handle
        assert projection._current_repository is None

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            await projection.handle(event)

        # After handle
        assert projection._current_repository is None

    @pytest.mark.asyncio
    async def test_repository_cleared_even_on_error(
        self, mock_postgresql_session_factory, checkpoint_repo, dlq_repo
    ) -> None:
        """Test repository is cleared even when handler raises error."""
        factory, _, _ = mock_postgresql_session_factory

        class FailingProjection(ReadModelProjection[OrderSummary]):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                raise ValueError("Handler error")

        projection = FailingProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

            with pytest.raises(ValueError):
                await projection.handle(event)

        # Repository should still be cleared after error
        assert projection._current_repository is None


class TestUnregisteredEventHandling:
    """Tests for unregistered event handling behavior."""

    @pytest.mark.asyncio
    async def test_unhandled_event_ignored_by_default(
        self, mock_postgresql_session_factory, checkpoint_repo, caplog
    ) -> None:
        """Test unhandled event type is silently ignored by default."""
        import logging

        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            # Try to handle an event type with no handler
            event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

            with caplog.at_level(logging.WARNING):
                await projection.handle(event)

            # Default mode is "ignore" - no warning should be logged
            assert not any("No handler registered" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_error_mode_raises_for_unhandled_event(
        self, mock_postgresql_session_factory, checkpoint_repo, dlq_repo
    ) -> None:
        """Test error mode raises UnhandledEventError for unhandled events."""
        from eventsource.exceptions import UnhandledEventError

        factory, _, _ = mock_postgresql_session_factory

        class StrictProjection(ReadModelProjection[OrderSummary]):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                pass

        projection = StrictProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        with patch("eventsource.readmodels.postgresql.PostgreSQLReadModelRepository"):
            event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

            with pytest.raises(UnhandledEventError) as exc_info:
                await projection.handle(event)

            assert "OrderShipped" in str(exc_info.value)


class TestTracingConfiguration:
    """Tests for tracing configuration."""

    def test_tracing_disabled_by_default(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test tracing is disabled by default for projections."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection._enable_tracing is False

    def test_tracing_can_be_enabled(self, mock_postgresql_session_factory, checkpoint_repo) -> None:
        """Test tracing can be explicitly enabled."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=True,
        )

        assert projection._enable_tracing is True


class TestImports:
    """Tests for module imports and exports."""

    def test_can_import_from_readmodels_module(self) -> None:
        """Test ReadModelProjection can be imported from readmodels module."""
        from eventsource.readmodels import ReadModelProjection as RMProjection

        assert RMProjection is not None

    def test_is_in_readmodels_all(self) -> None:
        """Test ReadModelProjection is in __all__."""
        from eventsource import readmodels

        assert "ReadModelProjection" in readmodels.__all__
