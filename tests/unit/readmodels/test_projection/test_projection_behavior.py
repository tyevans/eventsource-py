"""Unit tests for ReadModelProjection runtime errors, checkpoints, cleanup, events, tracing, and imports."""

import logging
from unittest.mock import patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.domain.decorators import handles
from eventsource.domain.exceptions import UnhandledEventError
from tests.unit.readmodels.test_projection._fixtures import (
    OrderCreated,
    OrderShipped,
    OrderSummary,
)


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

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
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

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
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

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
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

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
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

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
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

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
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
        """Test ReadModelProjection can be imported from its adapter module."""
        from eventsource.adapters.sql.readmodel_projection import (
            ReadModelProjection as RMProjection,
        )

        assert RMProjection is not None

    def test_is_in_readmodels_all(self) -> None:
        """Test ReadModelProjection is in top-level __all__."""
        import eventsource

        assert "ReadModelProjection" in eventsource.__all__
