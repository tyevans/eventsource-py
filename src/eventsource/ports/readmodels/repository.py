"""
Protocol for read model repositories.

Read model repositories provide CRUD operations for persisting and querying
read models from projection handlers, abstracting away database-specific details.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable
from uuid import UUID

from .model import ReadModel

if TYPE_CHECKING:
    from .query import Query

TModel = TypeVar("TModel", bound=ReadModel)


@runtime_checkable
class ReadModelRepository(Protocol[TModel]):
    """
    Protocol for read model persistence.

    Implementations provide async CRUD operations for read models (ADR-0001).

    Key Design Decisions:
        - `save()` uses upsert semantics (insert or update)
        - `delete()` is hard delete; use `soft_delete()` for recoverable deletion
        - `find()` excludes soft-deleted records by default
        - All operations that modify data update `updated_at` automatically
    """

    async def get(self, id: UUID) -> TModel | None:
        """
        Get a read model by ID.

        Returns:
            The read model if found, None otherwise (including soft-deleted records).
        """
        ...

    async def get_many(self, ids: list[UUID]) -> list[TModel]:
        """
        Get multiple read models by their IDs.

        Missing IDs and soft-deleted records are excluded from results.
        """
        ...

    async def save(self, model: TModel) -> None:
        """
        Save or update a read model (upsert semantics).

        Automatic behaviors:
            - Sets `created_at` on insert (if not already set)
            - Updates `updated_at` on every save
            - Increments `version` on update (for optimistic locking)
        """
        ...

    async def save_many(self, models: list[TModel]) -> None:
        """Save multiple read models in a batch."""
        ...

    async def delete(self, id: UUID) -> bool:
        """
        Delete a read model by ID (hard delete).

        Returns:
            True if a record was deleted, False if the ID was not found
        """
        ...

    async def soft_delete(self, id: UUID) -> bool:
        """
        Soft delete a read model by setting deleted_at timestamp.

        Returns:
            True if a record was soft-deleted, False if not found or already soft-deleted
        """
        ...

    async def restore(self, id: UUID) -> bool:
        """
        Restore a soft-deleted read model by clearing `deleted_at`.

        Returns:
            True if a record was restored, False if not found or was not soft-deleted
        """
        ...

    async def get_deleted(self, id: UUID) -> TModel | None:
        """
        Get a soft-deleted read model by ID.

        Returns None for non-deleted models or if the ID doesn't exist.
        """
        ...

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """Find only soft-deleted read models matching a query."""
        ...

    async def exists(self, id: UUID) -> bool:
        """
        Check if a read model exists and is not soft-deleted.
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

        Raises:
            ValueError: As `find()` -- unknown field name or unknown operator.
        """
        ...

    async def truncate(self) -> int:
        """
        Delete all read models (for projection reset), including soft-deleted ones.

        Returns:
            Number of records deleted
        """
        ...

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Verifies that the current database version matches the model's
        version before updating. On successful save, the version is incremented.

        Raises:
            ReadModelVersionConflictError: If the version in database doesn't match
                the model's version (concurrent modification detected)
            ReadModelNotFoundError: If the model doesn't exist in the database
        """
        ...


__all__ = ["ReadModelRepository"]
