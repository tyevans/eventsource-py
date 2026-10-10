"""
Query operations mixin for PostgreSQLReadModelRepository.

Provides SQL query generation and execution for SELECT and COUNT operations.
"""

from __future__ import annotations

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
        """
        Convert a Filter to a SQL clause and its bound parameters.

        Delegates the operator dispatch to `adapters/_common` so this
        adapter cannot drift from its siblings -- see
        `ReadModelRepository.find` for the semantics.

        Args:
            filter_: Filter condition to convert
            index: Index used to keep parameter names unique across filters

        Returns:
            Tuple of (SQL clause, parameter dict)

        Raises:
            ValueError: On an unknown field name or an unknown operator
        """
        params: dict[str, Any] = {}

        def bind(value: Any) -> str:
            # `p{index}` for the first (usually only) value of this filter;
            # suffixed only if a dialect ever binds more than one.
            name = f"p{index}" if not params else f"p{index}_{len(params)}"
            params[name] = value
            return f":{name}"

        clause = filter_to_sql(self._model_class, filter_, POSTGRESQL, bind)
        return clause, params

    def _build_select_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """
        Build SELECT SQL from Query.

        Args:
            query: Query specification with filters, ordering, pagination

        Returns:
            Tuple of (SQL string, parameter dict)
        """
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: dict[str, Any] = {}

        # Build WHERE clause
        where_clauses = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for i, filter_ in enumerate(query.filters):
            clause, filter_params = self._filter_to_sql(filter_, i)
            where_clauses.append(clause)
            params.update(filter_params)

        if where_clauses:
            parts.append("WHERE " + " AND ".join(where_clauses))

        # ORDER BY
        if query.order_by:
            direction = query.order_direction.upper()
            parts.append(f"ORDER BY {query.order_by} {direction}")

        # LIMIT / OFFSET
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")

        return " ".join(parts), params

    def _build_count_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """
        Build COUNT SQL from Query.

        Args:
            query: Query specification with filters

        Returns:
            Tuple of (SQL string, parameter dict)
        """
        parts = [f"SELECT COUNT(*) FROM {self._table_name}"]  # nosec B608
        params: dict[str, Any] = {}

        where_clauses = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for i, filter_ in enumerate(query.filters):
            clause, filter_params = self._filter_to_sql(filter_, i)
            where_clauses.append(clause)
            params.update(filter_params)

        if where_clauses:
            parts.append("WHERE " + " AND ".join(where_clauses))

        return " ".join(parts), params

    def _build_select_deleted_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """
        Build SELECT SQL from Query for soft-deleted records only.

        Args:
            query: Query specification with filters, ordering, pagination

        Returns:
            Tuple of (SQL string, parameter dict)
        """
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: dict[str, Any] = {}

        # Build WHERE clause - always require deleted_at IS NOT NULL
        where_clauses = ["deleted_at IS NOT NULL"]

        for i, filter_ in enumerate(query.filters):
            clause, filter_params = self._filter_to_sql(filter_, i)
            where_clauses.append(clause)
            params.update(filter_params)

        parts.append("WHERE " + " AND ".join(where_clauses))

        # ORDER BY
        if query.order_by:
            direction = query.order_direction.upper()
            parts.append(f"ORDER BY {query.order_by} {direction}")

        # LIMIT / OFFSET
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")

        return " ".join(parts), params

    async def find(self, query: Query | None = None) -> list[TModel]:
        """
        Find read models matching a query.

        Supports filtering, ordering, and pagination. Soft-deleted records
        are excluded unless `include_deleted=True` is set in the query.

        Args:
            query: Query with filters, ordering, pagination.
                   If None, returns all non-deleted records.

        Returns:
            List of matching read models
        """
        if query is None:
            query = Query()

        with self._tracer.span(
            "eventsource.readmodel.find",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
                ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_select_query(query)

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(text(sql), params)
                rows = result.fetchall()

            return [self._row_to_model(row) for row in rows]

    async def count(self, query: Query | None = None) -> int:
        """
        Count read models matching a query.

        Useful for pagination or checking how many records match
        without retrieving them all.

        Args:
            query: Query with filters.
                   If None, counts all non-deleted records.

        Returns:
            Number of matching read models
        """
        if query is None:
            query = Query()

        with self._tracer.span(
            "eventsource.readmodel.count",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_count_query(query)

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(text(sql), params)
                row = result.fetchone()

            return row[0] if row else 0
