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

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any
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
    SQLite's limited type system.

    Type Conversions:
        - UUID -> TEXT (36-char hyphenated format)
        - datetime -> TEXT (ISO 8601 format)
        - Decimal -> REAL (may lose precision for large values)
        - dict/list -> TEXT (JSON serialized)

    Requirements:
        - SQLite 3.24+ (for UPSERT support)
        - Table must exist with matching schema
        - Primary key on id column

    Example:
        >>> import aiosqlite
        >>> async with aiosqlite.connect("readmodels.db") as db:
        ...     repo = SQLiteReadModelRepository(db, OrderSummary)
        ...     await repo.save(OrderSummary(id=uuid4(), ...))

    Note:
        - All datetime values are stored in ISO 8601 format (UTC)
        - Query performance is suitable for small to medium datasets
        - Consider PostgreSQL for production workloads
    """

    def __init__(
        self,
        connection: aiosqlite.Connection,
        model_class: type[TModel],
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the SQLite repository.

        Args:
            connection: aiosqlite database connection
            model_class: The ReadModel subclass this repository will manage
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._connection = connection
        self._model_class = model_class
        self._table_name = model_class.table_name()
        self._field_names = model_class.field_names()

    async def get(self, id: UUID) -> TModel | None:
        """Get a read model by ID.

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

            if row is None:
                return None

            return self._row_to_model(row)

    def _row_to_model(self, row: Sequence[Any]) -> TModel:
        """Convert a database row to a model instance.

        Handles SQLite type conversions:
        - TEXT -> UUID for id field
        - TEXT -> datetime for timestamp fields

        Args:
            row: Database row with values in field_names order (tuple or aiosqlite.Row)

        Returns:
            Validated model instance
        """
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

    def model_class(self) -> type[TModel]:
        """Get the model class this repository manages.

        Returns:
            The ReadModel subclass
        """
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
