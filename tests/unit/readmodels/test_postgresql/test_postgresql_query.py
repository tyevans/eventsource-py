"""Unit tests for PostgreSQLReadModelRepository query building and integration."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_postgresql.test_postgresql_model import OrderSummary

Repo = PostgreSQLReadModelRepository[OrderSummary]


def _mock_conn(rows: list[Any] | None = None, scalar: Any = None) -> AsyncConnection:
    conn = MagicMock(spec=AsyncConnection)
    conn.in_transaction.return_value = True
    result = MagicMock()
    result.fetchall.return_value = rows or []
    result.fetchone.return_value = (scalar,) if scalar is not None else (rows[0] if rows else None)
    conn.execute = AsyncMock(return_value=result)
    return conn


@pytest.fixture
def repo() -> Repo:
    return PostgreSQLReadModelRepository(MagicMock(), OrderSummary, enable_tracing=False)


class TestBuildSelectQuery:
    def test_basic_select(self, repo: Repo) -> None:
        sql, params = repo._build_select_query(Query())
        assert "SELECT" in sql and "FROM order_summaries" in sql
        assert "WHERE" in sql and "deleted_at IS NULL" in sql and params == {}

    def test_select_with_filter(self, repo: Repo) -> None:
        sql, params = repo._build_select_query(Query(filters=[Filter.eq("status", "active")]))
        assert "WHERE" in sql and "deleted_at IS NULL" in sql and "status = :p0" in sql
        assert params == {"p0": "active"}

    def test_select_with_multiple_filters(self, repo: Repo) -> None:
        query = Query(
            filters=[Filter.eq("status", "active"), Filter.gt("total_amount", Decimal("100"))]
        )
        sql, params = repo._build_select_query(query)
        assert "status = :p0" in sql and "total_amount > :p1" in sql and " AND " in sql
        assert params["p0"] == "active" and params["p1"] == Decimal("100")

    def test_select_with_ordering(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query(order_by="created_at", order_direction="desc"))
        assert "ORDER BY created_at DESC" in sql

    def test_select_with_ordering_asc(self, repo: Repo) -> None:
        sql, _ = repo._build_select_query(Query(order_by="order_number", order_direction="asc"))
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
        assert "SELECT" in sql and "FROM order_summaries" in sql and "WHERE" in sql
        assert "deleted_at IS NULL" in sql and "status = :p0" in sql
        assert "ORDER BY created_at DESC" in sql and "LIMIT 10" in sql
        assert params["p0"] == "active"


class TestBuildCountQuery:
    def test_basic_count(self, repo: Repo) -> None:
        sql, params = repo._build_count_query(Query())
        assert "SELECT COUNT(*)" in sql and "FROM order_summaries" in sql
        assert "deleted_at IS NULL" in sql and params == {}

    def test_count_with_filter(self, repo: Repo) -> None:
        sql, params = repo._build_count_query(Query(filters=[Filter.eq("status", "shipped")]))
        assert "SELECT COUNT(*)" in sql and "status = :p0" in sql and params["p0"] == "shipped"

    def test_count_include_deleted(self, repo: Repo) -> None:
        sql, _ = repo._build_count_query(Query(include_deleted=True))
        assert "deleted_at IS NULL" not in sql


class TestQueryBuilderIntegration:
    def test_query_with_filter_method(self, repo: Repo) -> None:
        filtered = Query(limit=100).with_filter(Filter.eq("status", "active"))
        sql, params = repo._build_select_query(filtered)
        assert "status = :p0" in sql and "LIMIT 100" in sql and params["p0"] == "active"

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
        assert "status = :p0" in sql and "total_amount > :p1" in sql
        assert "ORDER BY created_at DESC" in sql and "LIMIT 10" in sql
        assert params["p0"] == "shipped" and params["p1"] == Decimal("50")

    @pytest.mark.asyncio
    async def test_find_one_eq_filters_and_iterables(self) -> None:
        now, uid = datetime.now(UTC), uuid4()
        row = (uid, now, now, 1, None, "ORD-001", "shipped", Decimal("25.00"))
        conn = _mock_conn(rows=[row])
        pg_repo = PostgreSQLReadModelRepository(conn, OrderSummary, enable_tracing=False)

        found = await pg_repo.find_one(Query(order_by="order_number", offset=1), status="shipped")
        assert found is not None and found.id == uid
        sql_arg = str(conn.execute.call_args[0][0])
        assert "status = :p0" in sql_arg and "LIMIT 1" in sql_arg and "OFFSET 1" in sql_arg
        assert conn.execute.call_args[0][1] == {"p0": "shipped"}

        empty_repo = PostgreSQLReadModelRepository(_mock_conn(rows=[]), OrderSummary)
        assert await empty_repo.find_one(status="missing") is None

        count_conn = _mock_conn(scalar=3)
        count_repo = PostgreSQLReadModelRepository(count_conn, OrderSummary, enable_tracing=False)
        assert await count_repo.count(status="shipped") == 3
        assert count_conn.execute.call_args[0][1] == {"p0": "shipped"}

        models_from_gen = await pg_repo.get_many(x for x in {uid})
        assert [m.id for m in models_from_gen] == [uid]
        assert isinstance(conn.execute.call_args[0][1]["ids"], list)

        model = OrderSummary(
            id=uid, order_number="ORD-001", status="shipped", total_amount=Decimal("25.00")
        )
        await pg_repo.save_many(m for m in (model,))
