"""
Protocol for read model repositories.

Read model repositories provide CRUD operations for persisting and querying
read models from projection handlers, abstracting away database-specific details.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable
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
        """Get a read model by ID if found and not soft-deleted, else None."""
        ...

    async def get_many(self, ids: Iterable[UUID]) -> list[TModel]:
        """Get multiple read models by their IDs (missing and soft-deleted excluded)."""
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

    async def save_many(self, models: Iterable[TModel]) -> None:
        """Save multiple read models in a batch."""
        ...

    async def delete(self, id: UUID) -> bool:
        """Delete a read model by ID (hard delete). Returns True if deleted."""
        ...

    async def soft_delete(self, id: UUID) -> bool:
        """Soft delete a read model by setting deleted_at. Returns True if soft-deleted."""
        ...

    async def restore(self, id: UUID) -> bool:
        """Restore a soft-deleted read model by clearing `deleted_at`. Returns True if restored."""
        ...

    async def get_deleted(self, id: UUID) -> TModel | None:
        """Get a soft-deleted read model by ID, or None if not deleted or not found."""
        ...

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """Find only soft-deleted read models matching a query."""
        ...

    async def exists(self, id: UUID) -> bool:
        """Check if a read model exists and is not soft-deleted."""
        ...

    async def find(self, query: Query | None = None, **eq_filters: Any) -> list[TModel]:
        """
        Find read models matching a query and optional equality keyword filters.

        Supports filtering, ordering, and pagination. Soft-deleted records
        are excluded unless `include_deleted=True` is set in the query.
        Filters combine with AND; any `**eq_filters` are appended as `Filter.eq`
        conditions. `offset` skips from the start of the ordered result, and
        applies with or without a `limit`.

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

    async def find_one(self, query: Query | None = None, **eq_filters: Any) -> TModel | None:
        """
        Find the first read model matching `query` and `**eq_filters`, or None.

        Preserves `query.order_by`, `query.offset`, and `query.include_deleted`
        while forcing `limit=1`.
        """
        ...

    async def count(self, query: Query | None = None, **eq_filters: Any) -> int:
        """
        Count read models matching a query and optional equality keyword filters.

        Filters, NULL handling, and the unknown-field/unknown-operator
        errors are exactly `find()`'s -- `count(q)` is always
        `len(find(q))` with `limit`/`offset` ignored.

        Raises:
            ValueError: As `find()` -- unknown field name or unknown operator.
        """
        ...

    async def truncate(self) -> int:
        """Delete all read models (including soft-deleted) and return count deleted."""
        ...

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Raises:
            ReadModelVersionConflictError: If database version doesn't match model version
            ReadModelNotFoundError: If the model doesn't exist in the database
        """
        ...


__all__ = ["ReadModelRepository"]
