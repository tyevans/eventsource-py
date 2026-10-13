"""Tests for SQLite read model async operations, tracing, security, and exports."""

from unittest.mock import MagicMock

import pytest

from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from tests.unit.readmodels.test_sqlite._fixtures import (
    CustomTableModel,
    OrderSummary,
)


class TestAsyncOperations:
    """Tests for async operations - primarily testing early returns and edge cases.

    Note: Full database integration tests are in P2-004. These unit tests
    focus on early return conditions that don't require database access.
    """

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository with mock connection for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_many_empty_list_returns_empty(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_many with empty list returns empty list immediately."""
        result = await repo.get_many([])
        assert result == []

    @pytest.mark.asyncio
    async def test_save_many_empty_list_does_nothing(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test save_many with empty list returns immediately."""
        # Should not raise and should not call database
        await repo.save_many([])


class TestTracingConfiguration:
    """Tests for tracing configuration."""

    def test_tracing_disabled_by_flag(self) -> None:
        """Test tracing can be disabled via constructor flag."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        assert repo._enable_tracing is False

    def test_tracing_enabled_by_default(self) -> None:
        """Test tracing is enabled by default."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary)

        assert repo._enable_tracing is True

    def test_tracing_can_be_explicitly_enabled(self) -> None:
        """Test tracing can be explicitly enabled."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=True)

        assert repo._enable_tracing is True


class TestSQLSecurityAnnotations:
    """Tests to verify SQL security annotations are present."""

    def test_table_name_from_trusted_source(self) -> None:
        """Test that table names come from trusted class definitions."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

        # Table name should be derived from model class, not user input
        assert repo._table_name == OrderSummary.table_name()
        assert repo._table_name == "order_summaries"

    def test_custom_table_name_from_class(self) -> None:
        """Test custom table names come from class definition."""
        mock_conn = MagicMock()
        repo = SQLiteReadModelRepository(mock_conn, CustomTableModel, enable_tracing=False)

        assert repo._table_name == "custom_orders"
        assert repo._table_name == CustomTableModel.table_name()


class TestExportFromModule:
    """Tests that SQLiteReadModelRepository is properly exported."""

    def test_import_from_readmodels(self) -> None:
        """Test SQLiteReadModelRepository can be imported from its adapter module."""
        from eventsource.adapters.sqlite.readmodels import (
            SQLiteReadModelRepository as ImportedRepo,
        )

        assert ImportedRepo is SQLiteReadModelRepository

    def test_in_all_exports(self) -> None:
        """Test SQLiteReadModelRepository is in the sqlite adapter's __all__."""
        from eventsource.adapters import sqlite

        assert "SQLiteReadModelRepository" in sqlite.__all__


__all__ = [
    "TestAsyncOperations",  # pragma: allowlist secret
    "TestExportFromModule",  # pragma: allowlist secret
    "TestSQLSecurityAnnotations",  # pragma: allowlist secret
    "TestTracingConfiguration",  # pragma: allowlist secret
]
