"""Query operations mixin for SQLiteReadModelRepository."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import replace
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


def _resolve_query(query: Query | None, eq_filters: dict[str, Any]) -> Query:
    base = query if query is not None else Query()
    if not eq_filters:
        return base
    return replace(base, filters=[*base.filters, *(Filter.eq(k, v) for k, v in eq_filters.items())])


class SQLiteReadModelQueryMixin[TModel: _BaseReadModel]:
    """Mixin providing query operations for SQLite read models."""

    _tracer: Tracer
    _connection: aiosqlite.Connection
    _model_class: type[TModel]
    _table_name: str
    _field_names: list[str]

    def _row_to_model(self, row: Sequence[Any]) -> TModel:
        data: dict[str, Any] = {}
        for i, field_name in enumerate(self._field_names):
            value = row[i]
            if field_name == "id" and value is not None:
                value = UUID(value)
            if field_name in ("created_at", "updated_at", "deleted_at") and isinstance(value, str):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            data[field_name] = value
        return self._model_class.model_validate(data)

    def _span_attrs(self, **extra: Any) -> dict[str, Any]:
        return {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_DB_SYSTEM: "sqlite", ATTR_DB_OPERATION: "SELECT", **extra}  # fmt: skip

    async def get_many(self, ids: Iterable[UUID]) -> list[TModel]:
        """Get multiple read models by their IDs (missing IDs are ignored)."""
        if not (id_list := list(ids)):
            return []
        attrs = self._span_attrs(**{ATTR_BATCH_SIZE: len(id_list)})
        with self._tracer.span("eventsource.readmodel.get_many", attrs):
            placeholders, fields = ",".join("?" * len(id_list)), ", ".join(self._field_names)
            query = (
                f"SELECT {fields} FROM {self._table_name} "  # nosec B608 - trusted table_name
                f"WHERE id IN ({placeholders}) AND deleted_at IS NULL"
            )
            cursor = await self._connection.execute(query, tuple(str(id_) for id_ in id_list))
            return [self._row_to_model(row) for row in await cursor.fetchall()]

    async def exists(self, id: UUID) -> bool:
        """Check if a read model exists and is not soft-deleted."""
        attrs = self._span_attrs(**{ATTR_READMODEL_ID: str(id)})
        with self._tracer.span("eventsource.readmodel.exists", attrs):
            query = f"SELECT 1 FROM {self._table_name} WHERE id = ? AND deleted_at IS NULL LIMIT 1"  # nosec B608
            cursor = await self._connection.execute(query, (str(id),))
            return (await cursor.fetchone()) is not None

    async def find(self, query: Query | None = None, **eq_filters: Any) -> list[TModel]:
        """Find read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)
        limit_val = query.limit if query.limit is not None else -1
        attrs = self._span_attrs(**{ATTR_QUERY_FILTER_COUNT: len(query.filters), ATTR_QUERY_LIMIT: limit_val})  # fmt: skip
        with self._tracer.span("eventsource.readmodel.find", attrs):
            sql, params = self._build_select_query(query)
            cursor = await self._connection.execute(sql, params)
            return [self._row_to_model(row) for row in await cursor.fetchall()]

    async def find_one(self, query: Query | None = None, **eq_filters: Any) -> TModel | None:
        """Find the first read model matching `query` and `**eq_filters`, or None."""
        results = await self.find(replace(_resolve_query(query, eq_filters), limit=1))
        return results[0] if results else None

    async def count(self, query: Query | None = None, **eq_filters: Any) -> int:
        """Count read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)
        attrs = self._span_attrs(**{ATTR_QUERY_FILTER_COUNT: len(query.filters)})
        with self._tracer.span("eventsource.readmodel.count", attrs):
            sql, params = self._build_count_query(query)
            row = await (await self._connection.execute(sql, params)).fetchone()
            return int(row[0]) if row else 0

    def _build_where_clause(self, query: Query) -> tuple[str, list[Any]]:
        where_clauses: list[str] = [] if query.include_deleted else ["deleted_at IS NULL"]
        params: list[Any] = []
        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)
        return (("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""), params

    def _build_select_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        where_sql, params = self._build_where_clause(query)
        if where_sql:
            parts.append(where_sql)
        if query.order_by:
            parts.append(f"ORDER BY {query.order_by} {query.order_direction.upper()}")
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        elif query.offset:
            parts.append("LIMIT -1")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")
        return " ".join(parts), tuple(params)

    def _build_count_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        parts = [f"SELECT COUNT(*) FROM {self._table_name}"]  # nosec B608
        where_sql, params = self._build_where_clause(query)
        if where_sql:
            parts.append(where_sql)
        return " ".join(parts), tuple(params)

    def _filter_to_sql(self, filter_: Filter) -> tuple[str, list[Any]]:
        params: list[Any] = []

        def bind(value: Any) -> str:
            params.append(value)
            return "?"

        return filter_to_sql(self._model_class, filter_, SQLITE, bind), params


__all__ = ["SQLiteReadModelQueryMixin"]
