"""Tests for SQLite read model query building and SQL syntax."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_sqlite._fixtures import OrderSummary


class TestBuildSelectQuery:
    """Tests for SELECT query building with SQLite syntax."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_basic_select(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test basic SELECT query building."""
        query = Query()
        sql, params = repo._build_select_query(query)

        assert "SELECT" in sql
        assert "FROM order_summaries" in sql
        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert params == ()

    def test_select_with_filter(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with filter using positional parameters."""
        query = Query(filters=[Filter.eq("status", "active")])
        sql, params = repo._build_select_query(query)

        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert "status = ?" in sql
        assert params == ("active",)

    def test_select_with_multiple_filters(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test SELECT with multiple filters."""
        query = Query(
            filters=[
                Filter.eq("status", "active"),
                Filter.gt("total_amount", Decimal("100")),
            ]
        )
        sql, params = repo._build_select_query(query)

        assert "status = ?" in sql
        assert "total_amount > ?" in sql
        assert " AND " in sql
        assert "active" in params
        assert Decimal("100") in params

    def test_select_with_ordering(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with ordering."""
        query = Query(order_by="created_at", order_direction="desc")
        sql, params = repo._build_select_query(query)

        assert "ORDER BY created_at DESC" in sql

    def test_select_with_ordering_asc(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with ascending ordering."""
        query = Query(order_by="order_number", order_direction="asc")
        sql, params = repo._build_select_query(query)

        assert "ORDER BY order_number ASC" in sql

    def test_select_with_limit(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with limit."""
        query = Query(limit=10)
        sql, params = repo._build_select_query(query)

        assert "LIMIT 10" in sql

    def test_select_with_offset(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with offset."""
        query = Query(offset=5)
        sql, params = repo._build_select_query(query)

        assert "OFFSET 5" in sql

    def test_select_with_pagination(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with pagination (limit and offset)."""
        query = Query(limit=10, offset=20)
        sql, params = repo._build_select_query(query)

        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_select_include_deleted(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with include_deleted excludes soft-delete filter."""
        query = Query(include_deleted=True)
        sql, params = repo._build_select_query(query)

        # Should not have deleted_at filter since include_deleted=True
        assert "deleted_at IS NULL" not in sql

    def test_select_full_query(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test SELECT with all query options."""
        query = Query(
            filters=[Filter.eq("status", "active")],
            order_by="created_at",
            order_direction="desc",
            limit=10,
            offset=0,
        )
        sql, params = repo._build_select_query(query)

        assert "SELECT" in sql
        assert "FROM order_summaries" in sql
        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert "status = ?" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert "active" in params


class TestBuildCountQuery:
    """Tests for COUNT query building."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_basic_count(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test basic COUNT query building."""
        query = Query()
        sql, params = repo._build_count_query(query)

        assert "SELECT COUNT(*)" in sql
        assert "FROM order_summaries" in sql
        assert "deleted_at IS NULL" in sql
        assert params == ()

    def test_count_with_filter(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test COUNT with filter."""
        query = Query(filters=[Filter.eq("status", "shipped")])
        sql, params = repo._build_count_query(query)

        assert "SELECT COUNT(*)" in sql
        assert "status = ?" in sql
        assert "shipped" in params

    def test_count_include_deleted(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test COUNT with include_deleted."""
        query = Query(include_deleted=True)
        sql, params = repo._build_count_query(query)

        assert "deleted_at IS NULL" not in sql


class TestQueryBuilderIntegration:
    """Tests for Query builder integration."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_query_with_filter_method(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test Query with_filter method integration."""
        base_query = Query(limit=100)
        filtered = base_query.with_filter(Filter.eq("status", "active"))

        sql, params = repo._build_select_query(filtered)

        assert "status = ?" in sql
        assert "LIMIT 100" in sql
        assert "active" in params

    def test_query_with_order_method(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test Query with_order method integration."""
        query = Query().with_order("created_at", "desc")

        sql, params = repo._build_select_query(query)

        assert "ORDER BY created_at DESC" in sql

    def test_query_with_pagination_method(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test Query with_pagination method integration."""
        query = Query().with_pagination(limit=20, offset=40)

        sql, params = repo._build_select_query(query)

        assert "LIMIT 20" in sql
        assert "OFFSET 40" in sql

    def test_query_with_deleted_method(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test Query with_deleted method integration."""
        query = Query().with_deleted(True)

        sql, params = repo._build_select_query(query)

        assert "deleted_at IS NULL" not in sql

    def test_chained_query_methods(self, repo: SQLiteReadModelRepository[OrderSummary]) -> None:
        """Test chained Query methods integration."""
        query = (
            Query()
            .with_filter(Filter.eq("status", "shipped"))
            .with_filter(Filter.gt("total_amount", Decimal("50")))
            .with_order("created_at", "desc")
            .with_pagination(limit=10, offset=0)
        )

        sql, params = repo._build_select_query(query)

        assert "status = ?" in sql
        assert "total_amount > ?" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert "shipped" in params
        assert Decimal("50") in params


class TestSQLiteSyntaxDifferences:
    """Tests specific to SQLite syntax differences from PostgreSQL."""

    @pytest.fixture
    def repo(self) -> SQLiteReadModelRepository[OrderSummary]:
        """Create repository for testing."""
        mock_conn = MagicMock()
        return SQLiteReadModelRepository(mock_conn, OrderSummary, enable_tracing=False)

    def test_uses_positional_parameters(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test that SQLite uses ? positional parameters, not :named."""
        query = Query(filters=[Filter.eq("status", "active")])
        sql, params = repo._build_select_query(query)

        # Should use ? not :param
        assert "?" in sql
        assert ":p0" not in sql
        assert ":status" not in sql

    def test_in_operator_uses_multiple_placeholders(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test IN operator uses multiple ? placeholders, not ANY()."""
        query = Query(filters=[Filter.in_("status", ["a", "b", "c"])])
        sql, params = repo._build_select_query(query)

        # SQLite uses IN (?,?,?) not ANY(:param)
        assert "IN (?,?,?)" in sql
        assert "ANY" not in sql
        assert params == ("a", "b", "c")

    def test_not_in_operator_uses_multiple_placeholders(
        self, repo: SQLiteReadModelRepository[OrderSummary]
    ) -> None:
        """Test NOT IN operator uses multiple ? placeholders, not ALL()."""
        query = Query(filters=[Filter.not_in("status", ["x", "y"])])
        sql, params = repo._build_select_query(query)

        # SQLite uses NOT IN (?,?) not != ALL(:param)
        assert "NOT IN (?,?)" in sql
        assert "ALL" not in sql
        assert params == ("x", "y")


__all__ = [
    "TestBuildCountQuery",  # pragma: allowlist secret
    "TestBuildSelectQuery",  # pragma: allowlist secret
    "TestQueryBuilderIntegration",  # pragma: allowlist secret
    "TestSQLiteSyntaxDifferences",  # pragma: allowlist secret
]
