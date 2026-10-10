"""
PostgreSQL implementation of read model repository.

Provides production-ready persistence for read models using PostgreSQL's
native types (UUID, TIMESTAMP WITH TIME ZONE) and efficient UPSERT operations.
"""

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
    """
    PostgreSQL implementation of ReadModelRepository.

    Stores read models in a PostgreSQL table derived from the model class name.
    Uses native PostgreSQL types and efficient UPSERT operations.

    Requirements:
        - Table must exist with matching schema (use schema generation utility)
        - Columns: id (UUID), created_at, updated_at, version, deleted_at, plus model fields
        - Primary key on id column

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLReadModelRepository(conn, OrderSummary)
        ...     await repo.save(OrderSummary(id=uuid4(), ...))
        ...     summary = await repo.get(some_id)

    Note:
        - Table names are derived from model class (e.g., OrderSummary -> order_summaries)
        - Override via model's __table_name__ class attribute if needed
        - All datetime values are stored as TIMESTAMP WITH TIME ZONE (UTC)
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        model_class: type[TModel],
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the PostgreSQL repository.

        Args:
            conn: Database connection or engine
            model_class: The ReadModel subclass this repository will manage
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn
        self._model_class = model_class
        self._table_name = model_class.table_name()
        self._field_names = model_class.field_names()

    async def get(self, id: UUID) -> TModel | None:
        """
        Get a read model by ID.

        Args:
            id: Unique identifier of the read model

        Returns:
            The read model if found and not soft-deleted, None otherwise
        """
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

            if row is None:
                return None

            return self._row_to_model(row)

    async def get_many(self, ids: list[UUID]) -> list[TModel]:
        """
        Get multiple read models by their IDs.

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            # PostgreSQL supports ANY() for array comparison
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
        """
        Delete a read model by ID (hard delete).

        Permanently removes the record from the database. Use `soft_delete()`
        if you need to be able to recover the record later.

        Args:
            id: Unique identifier of the read model to delete

        Returns:
            True if a record was deleted, False if the ID was not found
        """
        with self._tracer.span(
            "eventsource.readmodel.delete",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = text(f"""
                DELETE FROM {self._table_name}
                WHERE id = :id
            """)  # nosec B608

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, {"id": id})
                return result.rowcount > 0

    async def exists(self, id: UUID) -> bool:
        """
        Check if a read model exists (and is not soft-deleted).

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
        """
        Delete all read models (for projection reset).

        Removes ALL records from the repository, including soft-deleted ones.
        Use with caution - this is typically only called during projection
        rebuilds.

        Returns:
            Number of records deleted
        """
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
        """
        Convert a database row to a model instance.

        Args:
            row: Database row with values in field_names order

        Returns:
            Validated model instance
        """
        data = dict(zip(self._field_names, row, strict=True))
        return self._model_class.model_validate(data)

    @property
    def model_class(self) -> type[TModel]:
        """
        Get the model class this repository manages.

        Returns:
            The ReadModel subclass
        """
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
