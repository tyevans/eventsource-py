"""Unit tests for ReadModelProjection dialect detection and truncate/reset operations."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.domain.decorators import handles
from tests.unit.readmodels.test_projection._fixtures import (
    OrderCreated,
    OrderSummary,
)


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

        with patch(
            "eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"
        ) as mock_repo:
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

        with patch("eventsource.adapters.sqlite.readmodels.SQLiteReadModelRepository") as mock_repo:
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

        with patch(
            "eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"
        ) as mock_repo:
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

        with patch(
            "eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"
        ) as mock_repo:
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

        with patch(
            "eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"
        ) as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo_instance.truncate = AsyncMock(return_value=5)
            mock_repo.return_value = mock_repo_instance

            await projection.reset()

            mock_repo_instance.truncate.assert_called_once()

            # Checkpoint should also be reset
            checkpoint = await checkpoint_repo.get_checkpoint("TestProjection")
            assert checkpoint is None
