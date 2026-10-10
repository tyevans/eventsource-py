"""
Mutation operations mixin for PostgreSQLReadModelRepository.

Provides save, batch save, and version-checked save operations.
"""

from __future__ import annotations

from datetime import UTC, datetime

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

    async def save(self, model: TModel) -> None:
        """
        Save or update a read model (upsert semantics).

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPSERT",
            },
        ):
            now = datetime.now(UTC)
            columns = ", ".join(self._field_names)
            placeholders = ", ".join(f":{f}" for f in self._field_names)

            # Build UPDATE SET clause (exclude id, created_at, and version - version is handled separately)
            update_fields = [
                f for f in self._field_names if f not in ("id", "created_at", "version")
            ]
            update_clause = ", ".join(f"{f} = EXCLUDED.{f}" for f in update_fields)

            query = text(f"""
                INSERT INTO {self._table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET
                    {update_clause},
                    version = {self._table_name}.version + 1
            """)  # nosec B608

            data = model.model_dump(mode="python")
            data["updated_at"] = now

            async with sql_connection(self._conn, write=True) as conn:
                await conn.execute(query, data)

    async def save_many(self, models: list[TModel]) -> None:
        """
        Save multiple read models in a batch.

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPSERT",
            },
        ):
            now = datetime.now(UTC)
            columns = ", ".join(self._field_names)
            placeholders = ", ".join(f":{f}" for f in self._field_names)
            # Exclude id, created_at, and version from update - version is handled separately
            update_fields = [
                f for f in self._field_names if f not in ("id", "created_at", "version")
            ]
            update_clause = ", ".join(f"{f} = EXCLUDED.{f}" for f in update_fields)

            query = text(f"""
                INSERT INTO {self._table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET
                    {update_clause},
                    version = {self._table_name}.version + 1
            """)  # nosec B608

            async with sql_connection(self._conn, write=True) as conn:
                for model in models:
                    data = model.model_dump(mode="python")
                    data["updated_at"] = now
                    await conn.execute(query, data)

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Verifies that the current database version matches the model's
        version before updating. If versions don't match, raises
        ReadModelVersionConflictError. On successful save, the version is incremented.

        Args:
            model: The read model to save

        Raises:
            ReadModelVersionConflictError: If the version in database doesn't match
                the model's version
            ReadModelNotFoundError: If the model doesn't exist in database

        Example:
            >>> summary = await repo.get(order_id)
            >>> summary.status = "shipped"
            >>> try:
            ...     await repo.save_with_version_check(summary)
            ... except ReadModelVersionConflictError as e:
            ...     print(f"Conflict: expected v{e.expected_version}")
        """
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
            now = datetime.now(UTC)

            # Build UPDATE with version check - exclude id, created_at, version
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
            data["updated_at"] = now
            data["expected_version"] = model.version

            async with sql_connection(self._conn, write=True) as conn:
                result = await conn.execute(query, data)
                row = result.fetchone()

            if row is None:
                # Either model doesn't exist or version mismatch - check which
                check_query = text(f"""
                    SELECT version FROM {self._table_name} WHERE id = :id
                """)  # nosec B608

                async with sql_connection(self._conn, write=False) as conn:
                    result = await conn.execute(check_query, {"id": model.id})
                    check_row = result.fetchone()

                if check_row is None:
                    raise ReadModelNotFoundError(model.id)
                else:
                    raise ReadModelVersionConflictError(
                        model.id,
                        expected_version=model.version,
                        actual_version=check_row[0],
                    )
