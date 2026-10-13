"""
Protocol for read model repositories.

Read model repositories provide CRUD operations for persisting and querying
read models from projection handlers. They abstract away database-specific
details, enabling the same projection code to work with different backends.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable
from uuid import UUID

from .model import ReadModel

if TYPE_CHECKING:
    from .query import Query

# Type variable for read model types
TModel = TypeVar("TModel", bound=ReadModel)


@runtime_checkable
class ReadModelRepository(Protocol[TModel]):
    """
    Protocol for read model persistence.

    Implementations provide CRUD operations for read models, abstracting
    away database-specific details. All methods are async following the
    library's async-first design (ADR-0001).

    Type Parameters:
        TModel: The read model type this repository manages, bound to ReadModel

    Key Design Decisions:
        - `save()` uses upsert semantics (insert or update)
        - `delete()` is hard delete; use `soft_delete()` for recoverable deletion
        - `find()` excludes soft-deleted records by default
        - All operations that modify data update `updated_at` automatically

    Example:
        >>> class OrderSummary(ReadModel):
        ...     order_number: str
        ...     status: str
        ...
        >>> repo: ReadModelRepository[OrderSummary] = InMemoryReadModelRepository(OrderSummary)
        >>> await repo.save(OrderSummary(id=uuid4(), order_number="ORD-001", status="pending"))
        >>> summary = await repo.get(some_uuid)
    """

    async def get(self, id: UUID) -> TModel | None:
        """
        Get a read model by ID.

        Args:
            id: Unique identifier of the read model

        Returns:
            The read model if found, None otherwise (including soft-deleted records).
        """
        ...

    async def get_many(self, ids: list[UUID]) -> list[TModel]:
        """
        Get multiple read models by their IDs.

        Efficiently retrieves multiple records in a single database query.
        Missing IDs and soft-deleted records are excluded from results.

        Args:
            ids: List of unique identifiers

        Returns:
            List of found read models (order is not guaranteed to match input).
        """
        ...

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
        ...

    async def save_many(self, models: list[TModel]) -> None:
        """
        Save multiple read models in a batch.

        More efficient than calling `save()` multiple times as it uses
        a single database operation where possible.

        Args:
            models: List of read models to save
        """
        ...

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
        ...

    async def soft_delete(self, id: UUID) -> bool:
        """
        Soft delete a read model by setting deleted_at timestamp.

        The record remains in the database but is excluded from normal
        queries. Use `restore()` to undo a soft delete.

        Args:
            id: Unique identifier of the read model to soft delete

        Returns:
            True if a record was soft-deleted, False if not found or already soft-deleted
        """
        ...

    async def restore(self, id: UUID) -> bool:
        """
        Restore a soft-deleted read model.

        Clears the `deleted_at` timestamp, making the record visible
        in normal queries again.

        Args:
            id: Unique identifier of the read model to restore

        Returns:
            True if a record was restored, False if not found or was not soft-deleted
        """
        ...

    async def get_deleted(self, id: UUID) -> TModel | None:
        """
        Get a soft-deleted read model by ID.

        Only returns the model if it has been soft-deleted.
        Returns None for non-deleted models or if the ID doesn't exist.

        Args:
            id: Unique identifier of the read model

        Returns:
            The soft-deleted read model if found, None otherwise
        """
        ...

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """
        Find only soft-deleted read models matching a query.

        Args:
            query: Query with filters, ordering, pagination.
                   If None, returns all soft-deleted records.

        Returns:
            List of soft-deleted read models
        """
        ...

    async def exists(self, id: UUID) -> bool:
        """
        Check if a read model exists.

        More efficient than `get()` when you only need to check existence.

        Args:
            id: Unique identifier to check

        Returns:
            True if the read model exists and is not soft-deleted, False otherwise
        """
        ...

    async def find(self, query: Query | None = None) -> list[TModel]:
        """
        Find read models matching a query.

        Supports filtering, ordering, and pagination. Soft-deleted records
        are excluded unless `include_deleted=True` is set in the query.
        Filters combine with AND. `offset` skips from the start of the
        ordered result, and applies with or without a `limit`.

        Filter semantics (identical on every backend):

        **A field with no value is distinct from every value.** Where the
        stored field is NULL/None, `eq`, `in`, and the four ordering
        operators (`gt`, `gte`, `lt`, `lte`) do NOT match -- "no value" is
        neither equal to nor ordered against one -- while `ne` and `not_in`
        DO match, because a field with no value differs from whatever was
        asked for.

        This is Python's semantics, not SQL's three-valued logic, and it is
        a deliberate choice against the grain of the SQL adapters. The SQL
        adapters therefore emit an explicit `IS NULL OR ...` for the two
        NULL-tolerant operators.

        **`in` with an empty list matches nothing; `not_in` with an empty
        list matches everything.**

        Args:
            query: Query with filters, ordering, pagination.
                   If None, returns all non-deleted records.

        Returns:
            List of matching read models

        Raises:
            ValueError: If a filter names a field the read model does not
                have, or an operator outside `Filter`'s eight.
        """
        ...

    async def count(self, query: Query | None = None) -> int:
        """
        Count read models matching a query.

        Filters, NULL handling, and the unknown-field/unknown-operator
        errors are exactly `find()`'s -- `count(q)` is always
        `len(find(q))` with `limit`/`offset` ignored.

        Args:
            query: Query with filters. If None, counts all non-deleted records.

        Returns:
            Number of matching read models

        Raises:
            ValueError: As `find()` -- unknown field name or unknown operator.
        """
        ...

    async def truncate(self) -> int:
        """
        Delete all read models (for projection reset).

        Removes ALL records from the repository, including soft-deleted ones.

        Returns:
            Number of records deleted
        """
        ...

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Verifies that the current database version matches the model's
        version before updating. If versions don't match, raises
        ReadModelVersionConflictError. On successful save, the version is incremented.

        Args:
            model: The read model to save. Must already exist in the database.

        Raises:
            ReadModelVersionConflictError: If the version in database doesn't match
                the model's version (concurrent modification detected)
            ReadModelNotFoundError: If the model doesn't exist in the database
        """
        ...


# Type alias for backwards compatibility and convenience
ReadModelRepositoryProtocol = ReadModelRepository


__all__ = ["ReadModelRepository", "ReadModelRepositoryProtocol"]
