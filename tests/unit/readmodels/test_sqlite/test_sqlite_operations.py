"""Tests for SQLite read model async operations, tracing, security, and exports."""

from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import aiosqlite
import pytest

from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_sqlite._fixtures import (
    CustomTableModel,
    OrderSummary,
)


def _order(num: str, status: str, amount: str) -> OrderSummary:
    return OrderSummary(id=uuid4(), order_number=num, status=status, total_amount=Decimal(amount))


class TestAsyncOperations:
    """Tests for async operations, find_one, eq_filters, and Iterable inputs."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository with mock connection for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_many_empty_list_returns_empty(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_many with empty list or generator returns empty list immediately."""
        assert await repo.get_many([]) == []
        assert await repo.get_many(x for x in ()) == []

    @pytest.mark.asyncio
    async def test_save_many_empty_list_does_nothing(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test save_many with empty list or generator returns immediately."""
        await repo.save_many([])
        await repo.save_many(x for x in ())

    @pytest.mark.asyncio
    async def test_sqlite_find_one_eq_filters_and_iterables(self) -> None:
        """Verify find_one, **eq_filters on find/count/find_one, and Iterable get_many/save_many."""
        async with aiosqlite.connect(":memory:") as db:
            await db.executescript(generate_schema(OrderSummary, dialect="sqlite"))
            live_repo = SQLiteReadModelRepository(db, OrderSummary, enable_tracing=False)

            orders = [
                _order("ORD-002", "shipped", "20"),
                _order("ORD-001", "shipped", "10"),
                _order("ORD-003", "pending", "30"),
            ]
            await live_repo.save_many(o for o in orders)

            by_set = await live_repo.get_many({orders[0].id, orders[1].id})
            assert {m.id for m in by_set} == {orders[0].id, orders[1].id}
            by_tuple = await live_repo.get_many((orders[2].id,))
            assert [m.id for m in by_tuple] == [orders[2].id]
            by_gen = await live_repo.get_many(o.id for o in orders)
            assert len(by_gen) == 3

            shipped = await live_repo.find(status="shipped")
            assert {m.order_number for m in shipped} == {"ORD-001", "ORD-002"}
            assert await live_repo.count(status="shipped") == 2
            assert (
                await live_repo.count(
                    Query(filters=[Filter.gt("order_number", "ORD-001")]), status="shipped"
                )
                == 1
            )

            first = await live_repo.find_one(
                Query(order_by="order_number", order_direction="asc"), status="shipped"
            )
            assert first is not None and first.order_number == "ORD-001"
            second = await live_repo.find_one(
                Query(order_by="order_number", order_direction="asc", offset=1), status="shipped"
            )
            assert second is not None and second.order_number == "ORD-002"
            assert await live_repo.find_one(status="cancelled") is None


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
