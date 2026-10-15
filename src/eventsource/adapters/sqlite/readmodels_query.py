"""Query operations mixin for SQLiteReadModelRepository."""

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
            if field_name == "id" and value is not None:
                value = UUID(value)
            if field_name in ("created_at", "updated_at", "deleted_at") and isinstance(value, str):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            data[field_name] = value
        return self._model_class.model_validate(data)

    async def get_many(self, ids: list[UUID]) -> list[TModel]:
        """Get multiple read models by their IDs (missing IDs are ignored)."""
        if not ids:
            return []
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_BATCH_SIZE: len(ids),
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.get_many", attrs):
            placeholders = ",".join("?" * len(ids))
            fields = ", ".join(self._field_names)
            query = (
                f"SELECT {fields} FROM {self._table_name} "  # nosec B608 - trusted table_name
                f"WHERE id IN ({placeholders}) AND deleted_at IS NULL"
            )
            cursor = await self._connection.execute(query, tuple(str(id_) for id_ in ids))
            rows = await cursor.fetchall()
            return [self._row_to_model(row) for row in rows]

    async def exists(self, id: UUID) -> bool:
        """Check if a read model exists and is not soft-deleted."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.exists", attrs):
            query = (
                f"SELECT 1 FROM {self._table_name} "  # nosec B608 - trusted table_name
                "WHERE id = ? AND deleted_at IS NULL LIMIT 1"
            )
            cursor = await self._connection.execute(query, (str(id),))
            return (await cursor.fetchone()) is not None

    async def find(self, query: Query | None = None) -> list[TModel]:
        """Find read models matching a query (filtering, ordering, pagination)."""
        query = query or Query()
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_QUERY_FILTER_COUNT: len(query.filters),
            ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.find", attrs):
            sql, params = self._build_select_query(query)
            cursor = await self._connection.execute(sql, params)
            rows = await cursor.fetchall()
            return [self._row_to_model(row) for row in rows]

    async def count(self, query: Query | None = None) -> int:
        """Count read models matching a query."""
        query = query or Query()
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_QUERY_FILTER_COUNT: len(query.filters),
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.count", attrs):
            sql, params = self._build_count_query(query)
            cursor = await self._connection.execute(sql, params)
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    def _build_where_clause(self, query: Query) -> tuple[str, list[Any]]:
        """Build WHERE SQL fragment and parameter list from Query."""
        where_clauses: list[str] = [] if query.include_deleted else ["deleted_at IS NULL"]
        params: list[Any] = []
        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        return where_sql, params

    def _build_select_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """Build SELECT SQL and parameter tuple from Query."""
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        where_sql, params = self._build_where_clause(query)
        if where_sql:
            parts.append(where_sql)
        if query.order_by:
            parts.append(f"ORDER BY {query.order_by} {query.order_direction.upper()}")
        # SQLite rejects a bare OFFSET, so an offset with no limit needs LIMIT -1.
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        elif query.offset:
            parts.append("LIMIT -1")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")
        return " ".join(parts), tuple(params)

    def _build_count_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """Build COUNT SQL and parameter tuple from Query."""
        parts = [f"SELECT COUNT(*) FROM {self._table_name}"]  # nosec B608
        where_sql, params = self._build_where_clause(query)
        if where_sql:
            parts.append(where_sql)
        return " ".join(parts), tuple(params)

    def _filter_to_sql(self, filter_: Filter) -> tuple[str, list[Any]]:
        """Convert a Filter to SQL clause with parameters via shared SQLITE dialect."""
        params: list[Any] = []

        def bind(value: Any) -> str:
            params.append(value)
            return "?"

        return filter_to_sql(self._model_class, filter_, SQLITE, bind), params


__all__ = ["SQLiteReadModelQueryMixin"]
