"""Mutation operations mixin for SQLiteReadModelRepository.

Provides save, batch save, delete, truncate, and version-checked save operations.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_EXPECTED_VERSION,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.exceptions import (
    ReadModelNotFoundError,
    ReadModelVersionConflictError,
)
from eventsource.ports.readmodels.model import ReadModel as _BaseReadModel

if TYPE_CHECKING:
    import aiosqlite


class SQLiteReadModelMutationMixin[TModel: _BaseReadModel]:
    """Mixin providing save, batch save, and mutation operations for SQLite read models."""

    _tracer: Tracer
    _connection: aiosqlite.Connection
    _model_class: type[TModel]
    _table_name: str
    _field_names: list[str]

    def _model_to_values(self, model: TModel, updated_at: datetime) -> tuple[Any, ...]:
        """Convert a model to a tuple of values for SQL.

        Handles SQLite type conversions:
        - UUID -> TEXT (string representation)
        - datetime -> TEXT (ISO 8601 format)
        - dict/list -> TEXT (JSON serialized)

        Args:
            model: The read model to convert
            updated_at: Timestamp to use for updated_at field

        Returns:
            Tuple of values in field_names order
        """
        values: list[Any] = []
        data = model.model_dump(mode="json")

        for field_name in self._field_names:
            value = data.get(field_name)

            # Override updated_at with provided timestamp
            if field_name == "updated_at":
                value = updated_at.isoformat()
            elif isinstance(value, (dict, list)):
                # Serialize complex types to JSON string
                value = json.dumps(value)

            values.append(value)

        return tuple(values)

    async def save(self, model: TModel) -> None:
        """Save or update a read model (upsert semantics).

        If the model doesn't exist (by ID), it will be inserted.
        If the model exists, it will be updated.

        Automatic behaviors:
            - Sets `created_at` on insert (if not already set)
            - Updates `updated_at` on every save
            - Increments `version` on update (for optimistic locking)

        Args:
            model: The read model to save
        """
        with self._tracer.span(
            "eventsource.readmodel.save",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(model.id),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "UPSERT",
            },
        ):
            now = datetime.now(UTC)
            columns = ", ".join(self._field_names)
            placeholders = ", ".join("?" * len(self._field_names))

            # Build UPDATE SET clause (exclude id and created_at)
            update_fields = [f for f in self._field_names if f not in ("id", "created_at")]
            update_clause = ", ".join(f"{f} = excluded.{f}" for f in update_fields)

            query = f"""
                INSERT INTO {self._table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT(id) DO UPDATE SET
                    {update_clause},
                    version = version + 1
            """  # nosec B608 - table_name from trusted class

            values = self._model_to_values(model, now)
            await self._connection.execute(query, values)
            await self._connection.commit()

    async def save_many(self, models: list[TModel]) -> None:
        """Save multiple read models in a batch.

        More efficient than calling `save()` multiple times as it uses
        a single transaction.

        Args:
            models: List of read models to save
        """
        if not models:
            return

        with self._tracer.span(
            "eventsource.readmodel.save_many",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_BATCH_SIZE: len(models),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "UPSERT",
            },
        ):
            now = datetime.now(UTC)
            columns = ", ".join(self._field_names)
            placeholders = ", ".join("?" * len(self._field_names))
            update_fields = [f for f in self._field_names if f not in ("id", "created_at")]
            update_clause = ", ".join(f"{f} = excluded.{f}" for f in update_fields)

            query = f"""
                INSERT INTO {self._table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT(id) DO UPDATE SET
                    {update_clause},
                    version = version + 1
            """  # nosec B608 - table_name from trusted class

            for model in models:
                values = self._model_to_values(model, now)
                await self._connection.execute(query, values)

            await self._connection.commit()

    async def delete(self, id: UUID) -> bool:
        """Delete a read model by ID (hard delete).

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = f"DELETE FROM {self._table_name} WHERE id = ?"  # nosec B608

            cursor = await self._connection.execute(query, (str(id),))
            await self._connection.commit()
            return bool(cursor.rowcount and cursor.rowcount > 0)

    async def truncate(self) -> int:
        """Delete all read models (for projection reset).

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = f"DELETE FROM {self._table_name}"  # nosec B608

            cursor = await self._connection.execute(query)
            await self._connection.commit()
            return int(cursor.rowcount) if cursor.rowcount is not None else 0

    async def save_with_version_check(self, model: TModel) -> None:
        """Save a read model with optimistic locking version check.

        Verifies that the current database version matches the model's
        version before updating. If versions don't match, raises
        ReadModelVersionConflictError. On successful save, the version is incremented.

        Args:
            model: The read model to save

        Raises:
            ReadModelVersionConflictError: If the version in database doesn't match
                the model's version
            ReadModelNotFoundError: If the model doesn't exist in database
        """
        with self._tracer.span(
            "eventsource.readmodel.save_with_version_check",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(model.id),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "UPDATE",
                ATTR_EXPECTED_VERSION: model.version,
            },
        ):
            now = datetime.now(UTC)

            # Build UPDATE with version check - exclude id, created_at, version
            update_fields = [
                f for f in self._field_names if f not in ("id", "created_at", "version")
            ]
            set_clause = ", ".join(f"{f} = ?" for f in update_fields)

            query = f"""
                UPDATE {self._table_name}
                SET {set_clause}, version = version + 1
                WHERE id = ? AND version = ?
            """  # nosec B608

            # Build values in same order as set_clause, then id and version
            values: list[Any] = []
            data = model.model_dump(mode="json")
            data["updated_at"] = now.isoformat()

            for f in update_fields:
                value = data.get(f)
                # Convert complex types to JSON for SQLite
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                values.append(value)

            # Add WHERE clause parameters
            values.extend([str(model.id), model.version])

            cursor = await self._connection.execute(query, tuple(values))
            await self._connection.commit()

            if cursor.rowcount == 0:
                # Either model doesn't exist or version mismatch - check which
                check_cursor = await self._connection.execute(
                    f"SELECT version FROM {self._table_name} WHERE id = ?",  # nosec B608
                    (str(model.id),),
                )
                check_row = await check_cursor.fetchone()

                if check_row is None:
                    raise ReadModelNotFoundError(model.id)
                else:
                    raise ReadModelVersionConflictError(
                        model.id,
                        expected_version=model.version,
                        actual_version=check_row[0],
                    )


__all__ = ["SQLiteReadModelMutationMixin"]
