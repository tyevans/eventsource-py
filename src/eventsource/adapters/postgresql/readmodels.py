"""PostgreSQL implementation of read model repository."""

from __future__ import annotations

from typing import Any
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
        with self._tracer.span(
            "eventsource.readmodel.get",
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
                WHERE id = :id AND deleted_at IS NULL
            """)  # nosec B608 - table_name from trusted class

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"id": id})
                row = result.fetchone()

            return None if row is None else self._row_to_model(row)

    async def get_many(self, ids: list[UUID]) -> list[TModel]:
        """Get multiple non-deleted read models by their IDs in a single query."""
        if not ids:
            return []

        with self._tracer.span(
            "eventsource.readmodel.get_many",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_BATCH_SIZE: len(ids),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = text(f"""
                SELECT {", ".join(self._field_names)}
                FROM {self._table_name}
                WHERE id = ANY(:ids) AND deleted_at IS NULL
            """)  # nosec B608

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"ids": ids})
                rows = result.fetchall()

            return [self._row_to_model(row) for row in rows]

    async def delete(self, id: UUID) -> bool:
        """Hard-delete a read model by ID; returns True if a record was deleted."""
        with self._tracer.span(
            "eventsource.readmodel.delete",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = text(f"DELETE FROM {self._table_name} WHERE id = :id")  # nosec B608

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"id": id})
                return result.rowcount > 0

    async def exists(self, id: UUID) -> bool:
        """Check if a read model exists and is not soft-deleted."""
        with self._tracer.span(
            "eventsource.readmodel.exists",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = text(f"""
                SELECT 1 FROM {self._table_name}
                WHERE id = :id AND deleted_at IS NULL
                LIMIT 1
            """)  # nosec B608

            async with sql_connection(self._conn, write=False) as conn:
                result = await conn.execute(query, {"id": id})
                return result.fetchone() is not None

    async def truncate(self) -> int:
        """Delete all read models (including soft-deleted) and return count deleted."""
        with self._tracer.span(
            "eventsource.readmodel.truncate",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = text(f"DELETE FROM {self._table_name}")  # nosec B608

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query)
                return result.rowcount

    def _row_to_model(self, row: Any) -> TModel:
        """Convert a database row to a validated model instance."""
        data = dict(zip(self._field_names, row, strict=True))
        return self._model_class.model_validate(data)

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
