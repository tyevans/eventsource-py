"""
Protocol for read model repositories.

Read model repositories provide CRUD operations for persisting and querying
read models from projection handlers. They abstract away database-specific
details, enabling the same projection code to work with different backends.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable
from uuid import UUID

from eventsource.readmodels.base import ReadModel

if TYPE_CHECKING:
    from eventsource.readmodels.query import Query

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
        >>> # Using with a concrete implementation
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
            The read model if found, None otherwise

        Note:
            Returns None for soft-deleted records unless the implementation
            is configured otherwise.

        Example:
            >>> summary = await repo.get(order_id)
            >>> if summary is None:
            ...     print("Order not found")
        """
        ...

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

        Note:
            Soft-deleted records are excluded from results.

        Example:
            >>> summaries = await repo.get_many([id1, id2, id3])
            >>> # len(summaries) may be less than 3 if some IDs don't exist
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

        Note:
            Implementations may raise OptimisticLockError if version
            conflicts are detected (when optimistic locking is enabled).

        Example:
            >>> summary = OrderSummary(id=uuid4(), order_number="ORD-001", status="pending")
            >>> await repo.save(summary)
            >>> # Later...
            >>> summary.status = "shipped"
            >>> await repo.save(summary)  # Updates existing record
        """
        ...

    async def save_many(self, models: list[TModel]) -> None:
        """
        Save multiple read models in a batch.

        More efficient than calling `save()` multiple times as it uses
        a single database operation where possible.

        Args:
            models: List of read models to save

        Note:
            All models should be of the same type. The operation is
            typically atomic (all succeed or all fail).

        Example:
            >>> models = [model1, model2, model3]
            >>> await repo.save_many(models)  # Single database round-trip
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

        Example:
            >>> deleted = await repo.delete(order_id)
            >>> if not deleted:
            ...     print("Order was already deleted or never existed")
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
            True if a record was soft-deleted, False if not found
            or already soft-deleted

        Example:
            >>> await repo.soft_delete(order_id)
            >>> # Record still exists but won't appear in find() results
            >>> summary = await repo.get(order_id)  # Returns None
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
            True if a record was restored, False if not found
            or was not soft-deleted

        Example:
            >>> await repo.soft_delete(order_id)
            >>> # Later, user wants to undo...
            >>> await repo.restore(order_id)
            >>> summary = await repo.get(order_id)  # Now returns the record
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

        Note:
            Use this method for admin interfaces, auditing, or
            implementing restore functionality.

        Example:
            >>> deleted_order = await repo.get_deleted(order_id)
            >>> if deleted_order:
            ...     print(f"Order was deleted at {deleted_order.deleted_at}")
        """
        ...

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """
        Find only soft-deleted read models matching a query.

        Convenience method for retrieving soft-deleted records.
        Only returns records that have been soft-deleted.

        Args:
            query: Query with filters, ordering, pagination.
                   If None, returns all soft-deleted records.

        Returns:
            List of soft-deleted read models

        Note:
            Useful for implementing "trash" or "recently deleted"
            views in admin interfaces.

        Example:
            >>> deleted_orders = await repo.find_deleted(Query(limit=100))
            >>> for order in deleted_orders:
            ...     print(f"{order.id} deleted at {order.deleted_at}")
        """
        ...

    async def exists(self, id: UUID) -> bool:
        """
        Check if a read model exists.

        More efficient than `get()` when you only need to check existence.

        Args:
            id: Unique identifier to check

        Returns:
            True if the read model exists and is not soft-deleted,
            False otherwise

        Example:
            >>> if await repo.exists(order_id):
            ...     print("Order exists")
        """
        ...

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

        Warning:
            Without a limit, this may return a large number of records.
            Consider using pagination for production queries.

        Example:
            >>> from eventsource.readmodels import Query, Filter
            >>> query = Query(
            ...     filters=[Filter.eq("status", "shipped")],
            ...     order_by="created_at",
            ...     order_direction="desc",
            ...     limit=100,
            ... )
            >>> shipped_orders = await repo.find(query)
        """
        ...

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

        Example:
            >>> total = await repo.count()  # Total non-deleted records
            >>> pending = await repo.count(Query(filters=[Filter.eq("status", "pending")]))
        """
        ...

    async def truncate(self) -> int:
        """
        Delete all read models (for projection reset).

        Removes ALL records from the repository, including soft-deleted ones.
        Use with caution - this is typically only called during projection
        rebuilds.

        Returns:
            Number of records deleted

        Warning:
            This operation is irreversible and removes all data.

        Example:
            >>> # During projection reset
            >>> deleted_count = await repo.truncate()
            >>> print(f"Cleared {deleted_count} records")
        """
        ...

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Verifies that the current database version matches the model's
        version before updating. If versions don't match, raises
        OptimisticLockError. On successful save, the version is incremented.

        This method is useful when:
        - Multiple processes may update the same read model
        - You need to ensure no lost updates
        - You want explicit control over concurrency

        The optimistic locking pattern:
        1. Read model with current version N
        2. When saving, check that DB version is still N
        3. If match, save with version N+1
        4. If mismatch, raise OptimisticLockError

        Args:
            model: The read model to save. Must already exist in the database.

        Raises:
            OptimisticLockError: If the version in database doesn't match
                the model's version (concurrent modification detected)
            ReadModelNotFoundError: If the model doesn't exist in the database

        Note:
            Unlike `save()` which uses upsert semantics, this method requires
            the model to already exist. Use `save()` to create new models,
            then `save_with_version_check()` for subsequent updates when
            optimistic locking is needed.

        Example:
            >>> # Load a model
            >>> summary = await repo.get(order_id)
            >>> if summary is None:
            ...     raise ValueError("Order not found")
            ...
            >>> # Modify it
            >>> summary.status = "shipped"
            >>>
            >>> # Save with version check
            >>> try:
            ...     await repo.save_with_version_check(summary)
            ... except OptimisticLockError as e:
            ...     # Handle conflict - reload and retry or fail
            ...     print(f"Conflict: expected v{e.expected_version}, "
            ...           f"found v{e.actual_version}")
            ...     # Reload and retry if desired
            ...     fresh = await repo.get(order_id)
        """
        ...


# Type alias for backwards compatibility and convenience
ReadModelRepositoryProtocol = ReadModelRepository
