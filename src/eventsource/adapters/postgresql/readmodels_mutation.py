"""Mutation operations mixin for PostgreSQLReadModelRepository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
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
from eventsource.ports.readmodels.model import ReadModel


class PostgreSQLReadModelMutationMixin[TModel: ReadModel]:
    """Mixin providing save, batch save, and version-checked save operations."""

    _tracer: Tracer
    _conn: AsyncConnection | AsyncEngine
    _model_class: type[TModel]
    _table_name: str
    _field_names: list[str]

    def _build_upsert_query(self) -> Any:
        columns = ", ".join(self._field_names)
        placeholders = ", ".join(f":{f}" for f in self._field_names)
        update_fields = [f for f in self._field_names if f not in ("id", "created_at", "version")]
        update_clause = ", ".join(f"{f} = EXCLUDED.{f}" for f in update_fields)
        return text(f"""
            INSERT INTO {self._table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET
                {update_clause},
                version = {self._table_name}.version + 1
        """)  # nosec B608

    async def save(self, model: TModel) -> None:
        """Save or update a read model (upsert semantics)."""
        with self._tracer.span(
            "eventsource.readmodel.save",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(model.id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPSERT",
            },
        ):
            data = model.model_dump(mode="python")
            data["updated_at"] = datetime.now(UTC)
            async with sql_connection(self._conn, write=True) as conn:
                await conn.execute(self._build_upsert_query(), data)

    async def save_many(self, models: list[TModel]) -> None:
        """Save multiple read models in a batch within a single transaction."""
        if not models:
            return

        with self._tracer.span(
            "eventsource.readmodel.save_many",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_BATCH_SIZE: len(models),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPSERT",
            },
        ):
            now = datetime.now(UTC)
            query = self._build_upsert_query()
            async with sql_connection(self._conn, write=True) as conn:
                for model in models:
                    data = model.model_dump(mode="python")
                    data["updated_at"] = now
                    await conn.execute(query, data)

    async def save_with_version_check(self, model: TModel) -> None:
        """Save a read model with optimistic locking version check."""
        with self._tracer.span(
            "eventsource.readmodel.save_with_version_check",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(model.id),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPDATE",
                ATTR_EXPECTED_VERSION: model.version,
            },
        ):
            update_fields = [
                f for f in self._field_names if f not in ("id", "created_at", "version")
            ]
            set_clause = ", ".join(f"{f} = :{f}" for f in update_fields)
            query = text(f"""
                UPDATE {self._table_name}
                SET {set_clause}, version = version + 1
                WHERE id = :id AND version = :expected_version
                RETURNING version
            """)  # nosec B608

            data = model.model_dump(mode="python")
            data["updated_at"] = datetime.now(UTC)
            data["expected_version"] = model.version

            async with sql_connection(self._conn, write=True) as conn:
                row = (await conn.execute(query, data)).fetchone()

            if row is None:
                check_query = text(f"SELECT version FROM {self._table_name} WHERE id = :id")  # nosec B608
                async with sql_connection(self._conn, write=False) as conn:
                    check_row = (await conn.execute(check_query, {"id": model.id})).fetchone()
                if check_row is None:
                    raise ReadModelNotFoundError(model.id)
                raise ReadModelVersionConflictError(
                    model.id, expected_version=model.version, actual_version=check_row[0]
                )
