"""
PostgreSQL implementation of read model repository.

Provides production-ready persistence for read models using PostgreSQL's
native types (UUID, TIMESTAMP WITH TIME ZONE) and efficient UPSERT operations.
"""

from datetime import UTC, datetime
from typing import Any, Generic, TypeVar
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

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
from eventsource.readmodels.base import ReadModel
from eventsource.readmodels.exceptions import OptimisticLockError, ReadModelNotFoundError
from eventsource.readmodels.query import Filter, Query
from eventsource.repositories._connection import execute_with_connection

# Type variable for read model types
TModel = TypeVar("TModel", bound=ReadModel)


class PostgreSQLReadModelRepository(Generic[TModel]):
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

            async with execute_with_connection(self._conn, transactional=False) as conn:
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

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"ids": ids})
                rows = result.fetchall()

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
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

            async with execute_with_connection(self._conn, transactional=True) as conn:
                for model in models:
                    data = model.model_dump(mode="python")
                    data["updated_at"] = now
                    await conn.execute(query, data)

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query, {"id": id})
                return result.rowcount > 0

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPDATE",
            },
        ):
            now = datetime.now(UTC)
            query = text(f"""
                UPDATE {self._table_name}
                SET deleted_at = :now, updated_at = :now
                WHERE id = :id AND deleted_at IS NULL
            """)  # nosec B608

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query, {"id": id, "now": now})
                return result.rowcount > 0

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "UPDATE",
            },
        ):
            now = datetime.now(UTC)
            query = text(f"""
                UPDATE {self._table_name}
                SET deleted_at = NULL, updated_at = :now
                WHERE id = :id AND deleted_at IS NOT NULL
            """)  # nosec B608

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query, {"id": id, "now": now})
                return result.rowcount > 0

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = text(f"""
                SELECT {", ".join(self._field_names)}
                FROM {self._table_name}
                WHERE id = :id AND deleted_at IS NOT NULL
            """)  # nosec B608 - table_name from trusted class

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"id": id})
                row = result.fetchone()

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_select_deleted_query(query)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(text(sql), params)
                rows = result.fetchall()

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            query = text(f"""
                SELECT 1 FROM {self._table_name}
                WHERE id = :id AND deleted_at IS NULL
                LIMIT 1
            """)  # nosec B608

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"id": id})
                return result.fetchone() is not None

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_select_query(query)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(text(sql), params)
                rows = result.fetchall()

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "SELECT",
            },
        ):
            sql, params = self._build_count_query(query)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(text(sql), params)
                row = result.fetchone()

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
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_OPERATION: "DELETE",
            },
        ):
            query = text(f"DELETE FROM {self._table_name}")  # nosec B608

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query)
                return result.rowcount

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

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query, data)
                row = result.fetchone()

            if row is None:
                # Either model doesn't exist or version mismatch - check which
                check_query = text(f"""
                    SELECT version FROM {self._table_name} WHERE id = :id
                """)  # nosec B608

                async with execute_with_connection(self._conn, transactional=False) as conn:
                    result = await conn.execute(check_query, {"id": model.id})
                    check_row = result.fetchone()

                if check_row is None:
                    raise ReadModelNotFoundError(model.id)
                else:
                    raise OptimisticLockError(
                        model.id,
                        expected_version=model.version,
                        actual_version=check_row[0],
                    )

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

    def _build_select_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """
        Build SELECT SQL from Query.

        Args:
            query: Query specification with filters, ordering, pagination

        Returns:
            Tuple of (SQL string, parameter dict)
        """
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: dict[str, Any] = {}

        # Build WHERE clause
        where_clauses = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for i, filter_ in enumerate(query.filters):
            clause, param_name = self._filter_to_sql(filter_, i)
            where_clauses.append(clause)
            params[param_name] = filter_.value

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

        return " ".join(parts), params

    def _build_count_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """
        Build COUNT SQL from Query.

        Args:
            query: Query specification with filters

        Returns:
            Tuple of (SQL string, parameter dict)
        """
        parts = [f"SELECT COUNT(*) FROM {self._table_name}"]  # nosec B608
        params: dict[str, Any] = {}

        where_clauses = []
        if not query.include_deleted:
            where_clauses.append("deleted_at IS NULL")

        for i, filter_ in enumerate(query.filters):
            clause, param_name = self._filter_to_sql(filter_, i)
            where_clauses.append(clause)
            params[param_name] = filter_.value

        if where_clauses:
            parts.append("WHERE " + " AND ".join(where_clauses))

        return " ".join(parts), params

    def _build_select_deleted_query(self, query: Query) -> tuple[str, dict[str, Any]]:
        """
        Build SELECT SQL from Query for soft-deleted records only.

        Args:
            query: Query specification with filters, ordering, pagination

        Returns:
            Tuple of (SQL string, parameter dict)
        """
        parts = [f"SELECT {', '.join(self._field_names)} FROM {self._table_name}"]  # nosec B608
        params: dict[str, Any] = {}

        # Build WHERE clause - always require deleted_at IS NOT NULL
        where_clauses = ["deleted_at IS NOT NULL"]

        for i, filter_ in enumerate(query.filters):
            clause, param_name = self._filter_to_sql(filter_, i)
            where_clauses.append(clause)
            params[param_name] = filter_.value

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

        return " ".join(parts), params

    def _filter_to_sql(self, filter_: Filter, index: int) -> tuple[str, str]:
        """
        Convert a Filter to SQL clause.

        Args:
            filter_: Filter condition to convert
            index: Index for parameter naming

        Returns:
            Tuple of (SQL clause, parameter name)

        Raises:
            ValueError: If operator is unknown
        """
        param_name = f"p{index}"
        field = filter_.field

        if filter_.operator == "eq":
            return f"{field} = :{param_name}", param_name
        elif filter_.operator == "ne":
            return f"{field} != :{param_name}", param_name
        elif filter_.operator == "gt":
            return f"{field} > :{param_name}", param_name
        elif filter_.operator == "gte":
            return f"{field} >= :{param_name}", param_name
        elif filter_.operator == "lt":
            return f"{field} < :{param_name}", param_name
        elif filter_.operator == "lte":
            return f"{field} <= :{param_name}", param_name
        elif filter_.operator == "in":
            return f"{field} = ANY(:{param_name})", param_name
        elif filter_.operator == "not_in":
            return f"{field} != ALL(:{param_name})", param_name
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
            f"PostgreSQLReadModelRepository("
            f"model={self._model_class.__name__}, "
            f"table={self._table_name}, "
            f"tracing={'enabled' if self._enable_tracing else 'disabled'})"
        )
