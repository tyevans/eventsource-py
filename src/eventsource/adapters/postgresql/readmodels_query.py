"""Query operations mixin for PostgreSQLReadModelRepository."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._common import POSTGRESQL, filter_to_sql
from eventsource.adapters._sql.connection import sql_connection
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query


def _resolve_query(query: Query | None, eq_filters: dict[str, Any]) -> Query:
    base = query if query is not None else Query()
    if not eq_filters:
        return base
    return replace(base, filters=[*base.filters, *(Filter.eq(k, v) for k, v in eq_filters.items())])


class PostgreSQLReadModelQueryMixin[TModel: ReadModel]:
    """Mixin providing query building and execution for PostgreSQL read models."""

    _tracer: Tracer
    _conn: AsyncConnection | AsyncEngine
    _model_class: type[TModel]
    _table_name: str
    _field_names: list[str]

    def _row_to_model(self, row: Any) -> TModel:
        """Convert a database row to a model instance."""
        data = dict(zip(self._field_names, row, strict=True))
        return self._model_class.model_validate(data)

    def _filter_to_sql(self, filter_: Filter, index: int) -> tuple[str, dict[str, Any]]:
        """Convert a Filter to a SQL clause and its bound parameters."""
        params: dict[str, Any] = {}

        def bind(value: Any) -> str:
            name = f"p{index}" if not params else f"p{index}_{len(params)}"
            params[name] = value
            return f":{name}"

        return filter_to_sql(self._model_class, filter_, POSTGRESQL, bind), params

    def _build_where_clause(
        self, query: Query, *, base_clauses: list[str]
    ) -> tuple[str, dict[str, Any]]:
        where_clauses = list(base_clauses)
        params: dict[str, Any] = {}
        for i, filter_ in enumerate(query.filters):
            clause, filter_params = self._filter_to_sql(filter_, i)
            where_clauses.append(clause)
            params.update(filter_params)
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        return where_sql, params

    def _append_order_limit_offset(self, parts: list[str], query: Query) -> None:
        if query.order_by:
            parts.append(f"ORDER BY {query.order_by} {query.order_direction.upper()}")
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")

    def _build_select_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """Build SELECT SQL and parameter dict from Query."""
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        base = [] if query.include_deleted else ["deleted_at IS NULL"]
        where_sql, params = self._build_where_clause(query, base_clauses=base)
        if where_sql:
            parts.append(where_sql)
        self._append_order_limit_offset(parts, query)
        return " ".join(parts), params

    def _build_count_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """Build COUNT SQL and parameter dict from Query."""
        parts = [f"SELECT COUNT(*) FROM {self._table_name}"]  # nosec B608
        base = [] if query.include_deleted else ["deleted_at IS NULL"]
        where_sql, params = self._build_where_clause(query, base_clauses=base)
        if where_sql:
            parts.append(where_sql)
        return " ".join(parts), params

    def _build_select_deleted_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """Build SELECT SQL from Query for soft-deleted records only."""
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        where_sql, params = self._build_where_clause(query, base_clauses=["deleted_at IS NOT NULL"])
        parts.append(where_sql)
        self._append_order_limit_offset(parts, query)
        return " ".join(parts), params

    async def find(self, query: Query | None = None, **eq_filters: Any) -> list[TModel]:
        """Find read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_QUERY_FILTER_COUNT: len(query.filters),
            ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.find", attrs):
            sql, params = self._build_select_query(query)
            async with sql_connection(self._conn, write=False) as conn:
                rows = (await conn.execute(text(sql), params)).fetchall()
            return [self._row_to_model(row) for row in rows]

    async def find_one(self, query: Query | None = None, **eq_filters: Any) -> TModel | None:
        """Find the first read model matching `query` and `**eq_filters`, or None."""
        effective = replace(_resolve_query(query, eq_filters), limit=1)
        results = await self.find(effective)
        return results[0] if results else None

    async def count(self, query: Query | None = None, **eq_filters: Any) -> int:
        """Count read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_QUERY_FILTER_COUNT: len(query.filters),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.count", attrs):
            sql, params = self._build_count_query(query)
            async with sql_connection(self._conn, write=False) as conn:
                row = (await conn.execute(text(sql), params)).fetchone()
            return row[0] if row else 0
