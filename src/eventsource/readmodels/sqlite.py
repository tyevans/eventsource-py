"""
SQLite implementation of read model repository.

Provides lightweight, embedded persistence for read models using SQLite.
Suitable for development, testing, and embedded deployments.

SQLite-specific adaptations:
- UUIDs stored as TEXT (36-character hyphenated format)
- Datetimes stored as TEXT (ISO 8601 format)
- Uses UPSERT with ON CONFLICT syntax (SQLite 3.24+)
- Positional parameters (?) instead of named parameters
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic
from uuid import UUID

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_EXPECTED_VERSION,
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.readmodels.exceptions import OptimisticLockError, ReadModelNotFoundError
from eventsource.readmodels.query import Filter, Query

if TYPE_CHECKING:
    import aiosqlite

# Type variable for read model types - reuse from base
# Create type var locally to avoid import issues
from typing import TypeVar

from eventsource.readmodels.base import ReadModel as _BaseReadModel

TModel = TypeVar("TModel", bound=_BaseReadModel)


class SQLiteReadModelRepository(Generic[TModel]):
    """
    SQLite implementation of ReadModelRepository.

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
        """
        Initialize the SQLite repository.

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            placeholders = ",".join("?" * len(ids))
            query = f"""
                SELECT {", ".join(self._field_names)}
                FROM {self._table_name}
                WHERE id IN ({placeholders}) AND deleted_at IS NULL
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, tuple(str(id_) for id_ in ids))
            rows = await cursor.fetchall()

            return [self._row_to_model(row) for row in rows]

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = f"DELETE FROM {self._table_name} WHERE id = ?"  # nosec B608

            cursor = await self._connection.execute(query, (str(id),))
            await self._connection.commit()
            return cursor.rowcount > 0

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "UPDATE",
            },
        ):
            now = datetime.now(UTC).isoformat()
            query = f"""
                UPDATE {self._table_name}
                SET deleted_at = ?, updated_at = ?
                WHERE id = ? AND deleted_at IS NULL
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, (now, now, str(id)))
            await self._connection.commit()
            return cursor.rowcount > 0

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "UPDATE",
            },
        ):
            now = datetime.now(UTC).isoformat()
            query = f"""
                UPDATE {self._table_name}
                SET deleted_at = NULL, updated_at = ?
                WHERE id = ? AND deleted_at IS NOT NULL
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, (now, str(id)))
            await self._connection.commit()
            return cursor.rowcount > 0

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = f"""
                SELECT {", ".join(self._field_names)}
                FROM {self._table_name}
                WHERE id = ? AND deleted_at IS NOT NULL
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, (str(id),))
            row = await cursor.fetchone()

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_select_deleted_query(query)

            cursor = await self._connection.execute(sql, params)
            rows = await cursor.fetchall()

            return [self._row_to_model(row) for row in rows]

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = f"""
                SELECT 1 FROM {self._table_name}
                WHERE id = ? AND deleted_at IS NULL
                LIMIT 1
            """  # nosec B608 - table_name from trusted class

            cursor = await self._connection.execute(query, (str(id),))
            row = await cursor.fetchone()
            return row is not None

    async def find(self, query: Query | None = None) -> list[TModel]:
        """
        Find read models matching a query.

        Supports filtering, ordering, and pagination. Soft-deleted records
        are excluded unless `include_deleted=True` is set in the query.

        Args:
            query: Query with filters, ordering, pagination.
                   If None, returns all non-deleted records.

        Returns:
            List of matching read models
        """
        if query is None:
            query = Query()

        with self._tracer.span(
            "eventsource.readmodel.find",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
                ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_select_query(query)

            cursor = await self._connection.execute(sql, params)
            rows = await cursor.fetchall()

            return [self._row_to_model(row) for row in rows]

    async def count(self, query: Query | None = None) -> int:
        """
        Count read models matching a query.

        Useful for pagination or checking how many records match
        without retrieving them all.

        Args:
            query: Query with filters.
                   If None, counts all non-deleted records.

        Returns:
            Number of matching read models
        """
        if query is None:
            query = Query()

        with self._tracer.span(
            "eventsource.readmodel.count",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_count_query(query)

            cursor = await self._connection.execute(sql, params)
            row = await cursor.fetchone()

            return row[0] if row else 0

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
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = f"DELETE FROM {self._table_name}"  # nosec B608

            cursor = await self._connection.execute(query)
            await self._connection.commit()
            return cursor.rowcount

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Verifies that the current database version matches the model's
        version before updating. If versions don't match, raises
        OptimisticLockError. On successful save, the version is incremented.

        Args:
            model: The read model to save

        Raises:
            OptimisticLockError: If the version in database doesn't match
                the model's version
            ReadModelNotFoundError: If the model doesn't exist in database

        Example:
            >>> summary = await repo.get(order_id)
            >>> summary.status = "shipped"
            >>> try:
            ...     await repo.save_with_version_check(summary)
            ... except OptimisticLockError as e:
            ...     print(f"Conflict: expected v{e.expected_version}")
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
                    raise OptimisticLockError(
                        model.id,
                        expected_version=model.version,
                        actual_version=check_row[0],
                    )

    def _row_to_model(self, row: Sequence[Any]) -> TModel:
        """
        Convert a database row to a model instance.

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

    def _model_to_values(self, model: TModel, updated_at: datetime) -> tuple[Any, ...]:
        """
        Convert a model to a tuple of values for SQL.

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

    def _build_select_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """
        Build SELECT SQL from Query.

        Args:
            query: Query specification with filters, ordering, pagination

        Returns:
            Tuple of (SQL string, parameter tuple)
        """
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: list[Any] = []

        # Build WHERE clause
        where_clauses: list[str] = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)

        if where_clauses:
            parts.append("WHERE " + " AND ".join(where_clauses))

        # ORDER BY
        if query.order_by:
            direction = query.order_direction.upper()
            parts.append(f"ORDER BY {query.order_by} {direction}")

        # LIMIT / OFFSET
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")

        return " ".join(parts), tuple(params)

    def _build_count_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """
        Build COUNT SQL from Query.

        Args:
            query: Query specification with filters

        Returns:
            Tuple of (SQL string, parameter tuple)
        """
        parts = [f"SELECT COUNT(*) FROM {self._table_name}"]  # nosec B608
        params: list[Any] = []

        where_clauses: list[str] = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)

        if where_clauses:
            parts.append("WHERE " + " AND ".join(where_clauses))

        return " ".join(parts), tuple(params)

    def _build_select_deleted_query(self, query: Query) -> tuple[str, tuple[Any, ...]]:
        """
        Build SELECT SQL from Query for soft-deleted records only.

        Args:
            query: Query specification with filters, ordering, pagination

        Returns:
            Tuple of (SQL string, parameter tuple)
        """
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: list[Any] = []

        # Build WHERE clause - always require deleted_at IS NOT NULL
        where_clauses: list[str] = ["deleted_at IS NOT NULL"]

        for filter_ in query.filters:
            clause, filter_params = self._filter_to_sql(filter_)
            where_clauses.append(clause)
            params.extend(filter_params)

        parts.append("WHERE " + " AND ".join(where_clauses))

        # ORDER BY
        if query.order_by:
            direction = query.order_direction.upper()
            parts.append(f"ORDER BY {query.order_by} {direction}")

        # LIMIT / OFFSET
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        if query.offset:
            parts.append(f"OFFSET {query.offset}")

        return " ".join(parts), tuple(params)

    def _filter_to_sql(self, filter_: Filter) -> tuple[str, list[Any]]:
        """
        Convert a Filter to SQL clause with parameters.

        Args:
            filter_: Filter condition to convert

        Returns:
            Tuple of (SQL clause, parameter list)

        Raises:
            ValueError: If operator is unknown
        """
        field = filter_.field
        value = filter_.value

        # Convert UUID values to string for SQLite
        if isinstance(value, UUID):
            value = str(value)
        elif isinstance(value, list):
            value = [str(v) if isinstance(v, UUID) else v for v in value]

        if filter_.operator == "eq":
            return f"{field} = ?", [value]
        elif filter_.operator == "ne":
            return f"{field} != ?", [value]
        elif filter_.operator == "gt":
            return f"{field} > ?", [value]
        elif filter_.operator == "gte":
            return f"{field} >= ?", [value]
        elif filter_.operator == "lt":
            return f"{field} < ?", [value]
        elif filter_.operator == "lte":
            return f"{field} <= ?", [value]
        elif filter_.operator == "in":
            placeholders = ",".join("?" * len(value))
            return f"{field} IN ({placeholders})", list(value)
        elif filter_.operator == "not_in":
            placeholders = ",".join("?" * len(value))
            return f"{field} NOT IN ({placeholders})", list(value)
        else:
            raise ValueError(f"Unknown operator: {filter_.operator}")

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
            f"SQLiteReadModelRepository("
            f"model={self._model_class.__name__}, "
            f"table={self._table_name}, "
            f"tracing={'enabled' if self._enable_tracing else 'disabled'})"
        )
