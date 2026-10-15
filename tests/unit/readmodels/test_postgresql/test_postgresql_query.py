"""Unit tests for PostgreSQLReadModelRepository query building and integration."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_postgresql.test_postgresql_model import OrderSummary

Repo = PostgreSQLReadModelRepository[OrderSummary]


@pytest.fixture
def repo() -> Repo:
    return PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)


class TestBuildSelectQuery:
    def test_basic_select(self, repo: Repo) -> None:
        sql, params = repo._build_select_query(Query())
        assert "SELECT" in sql
        assert "FROM order_summaries" in sql
        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert params == {}

    def test_select_with_filter(self, repo: Repo) -> None:
        query = Query(filters=[Filter.eq("status", "active")])
        sql, params = repo._build_select_query(query)
        assert "WHERE" in sql
        assert "deleted_at IS NULL" in sql
        assert "status = :p0" in sql
        assert params == {"p0": "active"}

    def test_select_with_multiple_filters(self, repo: Repo) -> None:
        query = Query(
            filters=[
                Filter.eq("status", "active"),
                Filter.gt("total_amount", Decimal("100")),
            ]
        )
        sql, params = repo._build_select_query(query)
        assert "status = :p0" in sql
        assert "total_amount > :p1" in sql
        assert " AND " in sql
        assert params["p0"] == "active"
        assert params["p1"] == Decimal("100")

    def test_select_with_ordering(self, repo: Repo) -> None:
        query = Query(order_by="created_at", order_direction="desc")
        sql, _ = repo._build_select_query(query)
        assert "ORDER BY created_at DESC" in sql

    def test_select_with_ordering_asc(self, repo: Repo) -> None:
        query = Query(order_by="order_number", order_direction="asc")
        sql, _ = repo._build_select_query(query)
        assert "ORDER BY order_number ASC" in sql

    def test_select_with_limit(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query(limit=10))
        assert "LIMIT 10" in sql

    def test_select_with_offset(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query(offset=5))
        assert "OFFSET 5" in sql

    def test_select_with_pagination(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query(limit=10, offset=20))
        assert "LIMIT 10" in sql and "OFFSET 20" in sql

    def test_select_include_deleted(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query(include_deleted=True))
        assert "deleted_at IS NULL" not in sql

    def test_select_full_query(self, repo: Repo) -> None:
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
        assert "status = :p0" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert params["p0"] == "active"


class TestBuildCountQuery:
    def test_basic_count(self, repo: Repo) -> None:
        sql, params = repo._build_count_query(Query())
        assert "SELECT COUNT(*)" in sql
        assert "FROM order_summaries" in sql
        assert "deleted_at IS NULL" in sql
        assert params == {}

    def test_count_with_filter(self, repo: Repo) -> None:
        query = Query(filters=[Filter.eq("status", "shipped")])
        sql, params = repo._build_count_query(query)
        assert "SELECT COUNT(*)" in sql
        assert "status = :p0" in sql
        assert params["p0"] == "shipped"

    def test_count_include_deleted(self, repo: Repo) -> None:
        sql, _ = repo._build_count_query(Query(include_deleted=True))
        assert "deleted_at IS NULL" not in sql


class TestQueryBuilderIntegration:
    def test_query_with_filter_method(self, repo: Repo) -> None:
        filtered = Query(limit=100).with_filter(Filter.eq("status", "active"))
        sql, params = repo._build_select_query(filtered)
        assert "status = :p0" in sql
        assert "LIMIT 100" in sql
        assert params["p0"] == "active"

    def test_query_with_order_method(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query().with_order("created_at", "desc"))
        assert "ORDER BY created_at DESC" in sql

    def test_query_with_pagination_method(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query().with_pagination(limit=20, offset=40))
        assert "LIMIT 20" in sql and "OFFSET 40" in sql

    def test_query_with_deleted_method(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query().with_deleted(True))
        assert "deleted_at IS NULL" not in sql

    def test_chained_query_methods(self, repo: Repo) -> None:
        query = (
            Query()
            .with_filter(Filter.eq("status", "shipped"))
            .with_filter(Filter.gt("total_amount", Decimal("50")))
            .with_order("created_at", "desc")
            .with_pagination(limit=10, offset=0)
        )
        sql, params = repo._build_select_query(query)
        assert "status = :p0" in sql
        assert "total_amount > :p1" in sql
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 10" in sql
        assert params["p0"] == "shipped"
        assert params["p1"] == Decimal("50")
