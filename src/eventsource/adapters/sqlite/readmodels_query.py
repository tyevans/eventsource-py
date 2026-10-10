"""Query operations mixin for SQLiteReadModelRepository.

Provides querying, counting, and existence checks for SQLite read models.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.adapters._common import SQLITE, filter_to_sql
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel as _BaseReadModel
from eventsource.ports.readmodels.query import Filter, Query

if TYPE_CHECKING:
    import aiosqlite


class SQLiteReadModelQueryMixin[TModel: _BaseReadModel]:
    """Mixin providing query operations for SQLite read models."""

    _tracer: Tracer
    _connection: aiosqlite.Connection
    _model_class: type[TModel]
    _table_name: str
    _field_names: list[str]

    def _row_to_model(self, row: Sequence[Any]) -> TModel:
        """Convert a database row to a model instance."""
        data: dict[str, Any] = {}
        for i, field_name in enumerate(self._field_names):
            value = row[i]

            # Convert TEXT to UUID for id field
            if field_name == "id" and value is not None:
                value = UUID(value)

            # Convert TEXT to datetime for timestamp fields
            if (
                field_name in ("created_at", "updated_at", "deleted_at")
                and value is not None
                and isinstance(value, str)
            ):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))

            data[field_name] = value

        return self._model_class.model_validate(data)

    async def get_many(self, ids: list[UUID]) -> list[TModel]:
        """Get multiple read models by their IDs.

        Efficiently retrieves multiple records in a single database query.
        Missing IDs are silently ignored (not included in results).

        Args:
            ids: List of unique identifiers

        Returns:
            List of found read models. Order is not guaranteed to match
            input order. Missing IDs are not included.
        """
        if not ids:
            return []

        with self._tracer.span(
            "eventsource.readmodel.get_many",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_BATCH_SIZE: len(ids),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            placeholders = ",".join("?" * len(ids))
            query = f"""
                SELECT {", ".join(self._field_names)}
                FROM {self._table_name}
                WHERE id IN ({placeholders}) AND deleted_at IS NULL
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, tuple(str(id_) for id_ in ids))
            rows = await cursor.fetchall()

            return [self._row_to_model(row) for row in rows]

    async def exists(self, id: UUID) -> bool:
        """Check if a read model exists (and is not soft-deleted).

        More efficient than `get()` when you only need to check existence.

        Args:
            id: Unique identifier to check

        Returns:
            True if the read model exists and is not soft-deleted,
            False otherwise
        """
        with self._tracer.span(
            "eventsource.readmodel.exists",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = f"""
                SELECT 1 FROM {self._table_name}
                WHERE id = ? AND deleted_at IS NULL
                LIMIT 1
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, (str(id),))
            row = await cursor.fetchone()
            return row is not None

    async def find(self, query: Query | None = None) -> list[TModel]:
        """Find read models matching a query.

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_select_query(query)

            cursor = await self._connection.execute(sql, params)
            rows = await cursor.fetchall()

            return [self._row_to_model(row) for row in rows]

    async def count(self, query: Query | None = None) -> int:
        """Count read models matching a query.

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_count_query(query)

            cursor = await self._connection.execute(sql, params)
            row = await cursor.fetchone()

            return int(row[0]) if row else 0

    def _build_select_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """Build SELECT SQL from Query.

        Args:
            query: Query specification with filters, ordering, pagination

        Returns:
            Tuple of (SQL string, parameter tuple)
        """
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: list[Any] = []

        # Build WHERE clause
        where_clauses: list[str] = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)

        if where_clauses:
            parts.append("WHERE " + " AND ".join(where_clauses))

        # ORDER BY
        if query.order_by:
            direction = query.order_direction.upper()
            parts.append(f"ORDER BY {query.order_by} {direction}")

        # LIMIT / OFFSET. SQLite rejects a bare OFFSET, so an offset with no
        # limit needs LIMIT -1 (SQLite's "no limit") to stay a legal statement.
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        elif query.offset:
            parts.append("LIMIT -1")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")

        return " ".join(parts), tuple(params)

    def _build_count_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """Build COUNT SQL from Query.

        Args:
            query: Query specification with filters

        Returns:
            Tuple of (SQL string, parameter tuple)
        """
        parts = [f"SELECT COUNT(*) FROM {self._table_name}"]  # nosec B608
        params: list[Any] = []

        where_clauses: list[str] = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)

        if where_clauses:
            parts.append("WHERE " + " AND ".join(where_clauses))

        return " ".join(parts), tuple(params)

    def _filter_to_sql(self, filter_: Filter) -> tuple[str, list[Any]]:
        """Convert a Filter to SQL clause with parameters.

        Delegates the operator dispatch to `adapters/_common` so this
        adapter cannot drift from its siblings -- see
        `ReadModelRepository.find` for the semantics. UUID-to-text coercion
        is this dialect's contribution, declared once in the shared
        `SQLITE` dialect.

        Args:
            filter_: Filter condition to convert

        Returns:
            Tuple of (SQL clause, parameter list)

        Raises:
            ValueError: On an unknown field name or an unknown operator
        """
        params: list[Any] = []

        def bind(value: Any) -> str:
            params.append(value)
            return "?"

        clause = filter_to_sql(self._model_class, filter_, SQLITE, bind)
        return clause, params


__all__ = ["SQLiteReadModelQueryMixin"]
