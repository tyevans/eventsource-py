"""PostgreSQL implementation of read model repository."""

from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters.postgresql.readmodels_mutation import (
    PostgreSQLReadModelMutationMixin,
)
from eventsource.adapters.postgresql.readmodels_query import (
    PostgreSQLReadModelQueryMixin,
)
from eventsource.adapters.postgresql.readmodels_soft_delete import (
    PostgreSQLReadModelSoftDeleteMixin,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel


class PostgreSQLReadModelRepository[TModel: ReadModel](
    PostgreSQLReadModelMutationMixin[TModel],
    PostgreSQLReadModelSoftDeleteMixin[TModel],
    PostgreSQLReadModelQueryMixin[TModel],
):
    """PostgreSQL implementation of ReadModelRepository using native types and UPSERTs."""

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        model_class: type[TModel],
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the PostgreSQL repository for ``model_class``."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn
        self._model_class = model_class
        self._table_name = model_class.table_name()
        self._field_names = model_class.field_names()

    async def get(self, id: UUID) -> TModel | None:
        """Get a read model by ID if found and not soft-deleted."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.get", attrs):
            query = text(
                f"SELECT {', '.join(self._field_names)} FROM {self._table_name} "  # nosec B608
                "WHERE id = :id AND deleted_at IS NULL"
            )
            async with sql_connection(self._conn, write=False) as conn:
                row = (await conn.execute(query, {"id": id})).fetchone()
            return None if row is None else self._row_to_model(row)

    async def get_many(self, ids: Iterable[UUID]) -> list[TModel]:
        """Get multiple non-deleted read models by their IDs in a single query."""
        id_list = list(ids)
        if not id_list:
            return []
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_BATCH_SIZE: len(id_list),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.get_many", attrs):
            query = text(
                f"SELECT {', '.join(self._field_names)} FROM {self._table_name} "  # nosec B608
                "WHERE id = ANY(:ids) AND deleted_at IS NULL"
            )
            async with sql_connection(self._conn, write=False) as conn:
                rows = (await conn.execute(query, {"ids": id_list})).fetchall()
            return [self._row_to_model(row) for row in rows]

    async def delete(self, id: UUID) -> bool:
        """Hard-delete a read model by ID; returns True if a record was deleted."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "DELETE",
        }
        with self._tracer.span("eventsource.readmodel.delete", attrs):
            query = text(f"DELETE FROM {self._table_name} WHERE id = :id")  # nosec B608
            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"id": id})
                return result.rowcount > 0

    async def exists(self, id: UUID) -> bool:
        """Check if a read model exists and is not soft-deleted."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_READMODEL_ID: str(id),
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "SELECT",
        }
        with self._tracer.span("eventsource.readmodel.exists", attrs):
            query = text(
                f"SELECT 1 FROM {self._table_name} "  # nosec B608
                "WHERE id = :id AND deleted_at IS NULL LIMIT 1"
            )
            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"id": id})
                return result.fetchone() is not None

    async def truncate(self) -> int:
        """Delete all read models (including soft-deleted) and return count deleted."""
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_DB_SYSTEM: "postgresql",
            ATTR_DB_OPERATION: "DELETE",
        }
        with self._tracer.span("eventsource.readmodel.truncate", attrs):
            query = text(f"DELETE FROM {self._table_name}")  # nosec B608
            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query)
                return result.rowcount

    @property
    def model_class(self) -> type[TModel]:
        """Get the model class this repository manages."""
        return self._model_class

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"PostgreSQLReadModelRepository("
            f"model={self._model_class.__name__}, "
            f"table={self._table_name}, "
            f"tracing={'enabled' if self._enable_tracing else 'disabled'})"
        )


__all__ = [
    "PostgreSQLReadModelMutationMixin",
    "PostgreSQLReadModelQueryMixin",
    "PostgreSQLReadModelRepository",
    "PostgreSQLReadModelSoftDeleteMixin",
]
