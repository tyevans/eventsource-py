"""Soft delete operations mixin for SQLiteReadModelRepository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from eventsource.adapters.sqlite.readmodels_query import SQLiteReadModelQueryMixin
from eventsource.observability.attributes import (
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel as _BaseReadModel
from eventsource.ports.readmodels.query import Query


class SQLiteReadModelSoftDeleteMixin[TModel: _BaseReadModel](SQLiteReadModelQueryMixin[TModel]):
    """Mixin providing soft-delete operations for SQLite read models."""

    async def soft_delete(self, id: UUID) -> bool:
        """Soft delete a read model by setting deleted_at timestamp."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "UPDATE",
        }
        with self._tracer.span("eventsource.readmodel.soft_delete", attrs):
            now = datetime.now(UTC).isoformat()
            query = (
                f"UPDATE {self._table_name} SET deleted_at = ?, updated_at = ? "  # nosec B608
                "WHERE id = ? AND deleted_at IS NULL"
            )
            cursor = await self._connection.execute(query, (now, now, str(id)))
            await self._connection.commit()
            return bool(cursor.rowcount and cursor.rowcount > 0)

    async def restore(self, id: UUID) -> bool:
        """Restore a soft-deleted read model by clearing deleted_at."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "UPDATE",
        }
        with self._tracer.span("eventsource.readmodel.restore", attrs):
            now = datetime.now(UTC).isoformat()
            query = (
                f"UPDATE {self._table_name} SET deleted_at = NULL, updated_at = ? "  # nosec B608
                "WHERE id = ? AND deleted_at IS NOT NULL"
            )
            cursor = await self._connection.execute(query, (now, str(id)))
            await self._connection.commit()
            return bool(cursor.rowcount and cursor.rowcount > 0)

    async def get_deleted(self, id: UUID) -> TModel | None:
        """Get a soft-deleted read model by ID."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.get_deleted", attrs):
            fields = ", ".join(self._field_names)
            query = (
                f"SELECT {fields} FROM {self._table_name} "  # nosec B608
                "WHERE id = ? AND deleted_at IS NOT NULL"
            )
            cursor = await self._connection.execute(query, (str(id),))
            row = await cursor.fetchone()
            return None if row is None else self._row_to_model(row)

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """Find only soft-deleted read models matching a query."""
        if query is None:
            query = Query()
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_QUERY_FILTER_COUNT: len(query.filters),
            ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
            ATTR_DB_SYSTEM: "sqlite",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.find_deleted", attrs):
            sql, params = self._build_select_deleted_query(query)
            cursor = await self._connection.execute(sql, params)
            rows = await cursor.fetchall()
            return [self._row_to_model(row) for row in rows]

    def _build_select_deleted_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """Build SELECT SQL from Query for soft-deleted records only."""
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: list[Any] = []
        where_clauses: list[str] = ["deleted_at IS NOT NULL"]
        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)
        parts.append("WHERE " + " AND ".join(where_clauses))
        if query.order_by:
            parts.append(f"ORDER BY {query.order_by} {query.order_direction.upper()}")
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        elif query.offset:
            parts.append("LIMIT -1")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")
        return " ".join(parts), tuple(params)


__all__ = ["SQLiteReadModelSoftDeleteMixin"]
