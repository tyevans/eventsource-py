"""Mutation operations mixin for SQLiteReadModelRepository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.adapters.serialization import json_dumps
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
        """Convert a model to a tuple of SQLite-compatible values in field_names order."""
        values: list[Any] = []
        data = model.model_dump(mode="json")

        for field_name in self._field_names:
            value = data.get(field_name)
            if field_name == "updated_at":
                value = updated_at.isoformat()
            elif isinstance(value, dict | list):
                value = json_dumps(value)
            values.append(value)

        return tuple(values)

    def _build_upsert_query(self) -> str:
        """Build the SQLite INSERT ... ON CONFLICT DO UPDATE query."""
        columns = ", ".join(self._field_names)
        placeholders = ", ".join("?" * len(self._field_names))
        update_fields = [f for f in self._field_names if f not in ("id", "created_at")]
        update_clause = ", ".join(f"{f} = excluded.{f}" for f in update_fields)
        return f"""
            INSERT INTO {self._table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT(id) DO UPDATE SET
                {update_clause},
                version = version + 1
        """  # nosec B608 - table_name from trusted class

    async def save(self, model: TModel) -> None:
        """Save or update a read model with upsert semantics and version increment."""
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
            query = self._build_upsert_query()
            values = self._model_to_values(model, now)
            await self._connection.execute(query, values)
            await self._connection.commit()

    async def save_many(self, models: list[TModel]) -> None:
        """Save multiple read models in a batch within a single transaction."""
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
            query = self._build_upsert_query()
            for model in models:
                await self._connection.execute(query, self._model_to_values(model, now))
            await self._connection.commit()

    async def delete(self, id: UUID) -> bool:
        """Hard-delete a read model by ID; returns True if a record was deleted."""
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
        """Delete all read models (including soft-deleted) and return count deleted."""
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
        """Save a read model with optimistic locking version check."""
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
            update_fields = [
                f for f in self._field_names if f not in ("id", "created_at", "version")
            ]
            set_clause = ", ".join(f"{f} = ?" for f in update_fields)
            query = f"""
                UPDATE {self._table_name}
                SET {set_clause}, version = version + 1
                WHERE id = ? AND version = ?
            """  # nosec B608

            values: list[Any] = []
            data = model.model_dump(mode="json")
            data["updated_at"] = now.isoformat()
            for f in update_fields:
                value = data.get(f)
                if isinstance(value, dict | list):
                    value = json_dumps(value)
                values.append(value)
            values.extend([str(model.id), model.version])

            cursor = await self._connection.execute(query, tuple(values))
            await self._connection.commit()

            if cursor.rowcount == 0:
                check_cursor = await self._connection.execute(
                    f"SELECT version FROM {self._table_name} WHERE id = ?",  # nosec B608
                    (str(model.id),),
                )
                check_row = await check_cursor.fetchone()
                if check_row is None:
                    raise ReadModelNotFoundError(model.id)
                raise ReadModelVersionConflictError(
                    model.id,
                    expected_version=model.version,
                    actual_version=check_row[0],
                )


__all__ = ["SQLiteReadModelMutationMixin"]
