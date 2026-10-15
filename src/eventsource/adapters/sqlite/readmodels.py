"""SQLite implementation of read model repository.

Provides lightweight, embedded persistence for read models using SQLite.
Suitable for development, testing, and embedded deployments.

SQLite-specific adaptations:
- UUIDs stored as TEXT (36-character hyphenated format)
- Datetimes stored as TEXT (ISO 8601 format)
- Uses UPSERT with ON CONFLICT syntax (SQLite 3.24+)
- Positional parameters (?) instead of named parameters
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.adapters.sqlite.readmodels_mutation import (
    SQLiteReadModelMutationMixin,
)
from eventsource.adapters.sqlite.readmodels_query import (
    SQLiteReadModelQueryMixin,
)
from eventsource.adapters.sqlite.readmodels_soft_delete import (
    SQLiteReadModelSoftDeleteMixin,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel as _BaseReadModel

if TYPE_CHECKING:
    import aiosqlite


class SQLiteReadModelRepository[TModel: _BaseReadModel](
    SQLiteReadModelMutationMixin[TModel],
    SQLiteReadModelSoftDeleteMixin[TModel],
    SQLiteReadModelQueryMixin[TModel],
):
    """SQLite implementation of ReadModelRepository.

    Stores read models in a SQLite table with type conversions for
    SQLite's limited type system (UUID -> TEXT, datetime -> ISO 8601 TEXT,
    Decimal -> REAL, dict/list -> JSON TEXT). Requires SQLite 3.24+ for UPSERT.
    """

    def __init__(
        self,
        connection: aiosqlite.Connection,
        model_class: type[TModel],
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the SQLite repository for ``model_class``."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._connection = connection
        self._model_class = model_class
        self._table_name = model_class.table_name()
        self._field_names = model_class.field_names()

    async def get(self, id: UUID) -> TModel | None:
        """Get a read model by ID if found and not soft-deleted, else None."""
        with self._tracer.span(
            "eventsource.readmodel.get",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = f"""
                SELECT {", ".join(self._field_names)}
                FROM {self._table_name}
                WHERE id = ? AND deleted_at IS NULL
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, (str(id),))
            row = await cursor.fetchone()
            return None if row is None else self._row_to_model(row)

    @property
    def model_class(self) -> type[TModel]:
        """Get the model class this repository manages."""
        return self._model_class

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"SQLiteReadModelRepository("
            f"model={self._model_class.__name__}, "
            f"table={self._table_name}, "
            f"tracing={'enabled' if self._enable_tracing else 'disabled'})"
        )


__all__ = ["SQLiteReadModelRepository"]
