"""
Soft delete operations mixin for PostgreSQLReadModelRepository.

Provides soft-delete, restore, and soft-deleted querying for PostgreSQL read models.
"""

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
        """
        Soft delete a read model by setting deleted_at timestamp.

        The record remains in the database but is excluded from normal
        queries. Use `restore()` to undo a soft delete.

        Args:
            id: Unique identifier of the read model to soft delete

        Returns:
            True if a record was soft-deleted, False if not found
            or already soft-deleted
        """
        with self._tracer.span(
            "eventsource.readmodel.soft_delete",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPDATE",
            },
        ):
            now = datetime.now(UTC)
            query = text(f"""
                UPDATE {self._table_name}
                SET deleted_at = :now, updated_at = :now
                WHERE id = :id AND deleted_at IS NULL
            """)  # nosec B608

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"id": id, "now": now})
                return result.rowcount > 0

    async def restore(self, id: UUID) -> bool:
        """
        Restore a soft-deleted read model.

        Clears the `deleted_at` timestamp, making the record visible
        in normal queries again.

        Args:
            id: Unique identifier of the read model to restore

        Returns:
            True if a record was restored, False if not found
            or was not soft-deleted
        """
        with self._tracer.span(
            "eventsource.readmodel.restore",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPDATE",
            },
        ):
            now = datetime.now(UTC)
            query = text(f"""
                UPDATE {self._table_name}
                SET deleted_at = NULL, updated_at = :now
                WHERE id = :id AND deleted_at IS NOT NULL
            """)  # nosec B608

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"id": id, "now": now})
                return result.rowcount > 0

    async def get_deleted(self, id: UUID) -> TModel | None:
        """
        Get a soft-deleted read model by ID.

        Only returns the model if it has been soft-deleted.

        Args:
            id: Unique identifier of the read model

        Returns:
            The soft-deleted read model if found, None otherwise
        """
        with self._tracer.span(
            "eventsource.readmodel.get_deleted",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = text(f"""
                SELECT {", ".join(self._field_names)}
                FROM {self._table_name}
                WHERE id = :id AND deleted_at IS NOT NULL
            """)  # nosec B608 - table_name from trusted class

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"id": id})
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_model(row)

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """
        Find only soft-deleted read models matching a query.

        Args:
            query: Query with filters, ordering, pagination

        Returns:
            List of soft-deleted read models
        """
        if query is None:
            query = Query()

        with self._tracer.span(
            "eventsource.readmodel.find_deleted",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
                ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_select_deleted_query(query)

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(text(sql), params)
                rows = result.fetchall()

            return [self._row_to_model(row) for row in rows]
