"""Soft delete operations mixin for PostgreSQLReadModelRepository."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import text

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters.postgresql.readmodels_query import (
    PostgreSQLReadModelQueryMixin,
)
from eventsource.observability.attributes import (
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Query


class PostgreSQLReadModelSoftDeleteMixin[TModel: ReadModel](
    PostgreSQLReadModelQueryMixin[TModel],
):
    """Mixin providing soft-delete operations for PostgreSQL read models."""

    async def soft_delete(self, id: UUID) -> bool:
        """Soft delete a read model by setting deleted_at timestamp."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "UPDATE",
        }
        with self._tracer.span("eventsource.readmodel.soft_delete", attrs):
            now = datetime.now(UTC)
            query = text(
                f"UPDATE {self._table_name} SET deleted_at = :now, updated_at = :now "  # nosec B608
                "WHERE id = :id AND deleted_at IS NULL"
            )
            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"id": id, "now": now})
                return result.rowcount > 0

    async def restore(self, id: UUID) -> bool:
        """Restore a soft-deleted read model by clearing deleted_at."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "UPDATE",
        }
        with self._tracer.span("eventsource.readmodel.restore", attrs):
            now = datetime.now(UTC)
            query = text(
                f"UPDATE {self._table_name} SET deleted_at = NULL, updated_at = :now "  # nosec B608
                "WHERE id = :id AND deleted_at IS NOT NULL"
            )
            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"id": id, "now": now})
                return result.rowcount > 0

    async def get_deleted(self, id: UUID) -> TModel | None:
        """Get a soft-deleted read model by ID, or None if not found/not deleted."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.get_deleted", attrs):
            cols = ", ".join(self._field_names)
            query = text(
                f"SELECT {cols} FROM {self._table_name} "  # nosec B608 - table_name from trusted class
                "WHERE id = :id AND deleted_at IS NOT NULL"
            )
            async with sql_connection(self._conn, write=False) as conn:
                row = (await conn.execute(query, {"id": id})).fetchone()
            return None if row is None else self._row_to_model(row)

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """Find only soft-deleted read models matching a query."""
        if query is None:
            query = Query()
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_QUERY_FILTER_COUNT: len(query.filters),
            ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.find_deleted", attrs):
            sql, params = self._build_select_deleted_query(query)
            async with sql_connection(self._conn, write=False) as conn:
                rows = (await conn.execute(text(sql), params)).fetchall()
            return [self._row_to_model(row) for row in rows]
