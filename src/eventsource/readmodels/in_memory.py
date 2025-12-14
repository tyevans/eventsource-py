"""
In-memory implementation of read model repository.

Provides a simple, fast repository for testing and development.
All data is stored in memory and lost when the process terminates.
"""

import asyncio
from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_EXPECTED_VERSION,
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.readmodels.base import ReadModel
from eventsource.readmodels.exceptions import OptimisticLockError, ReadModelNotFoundError
from eventsource.readmodels.query import Filter, Query

# Type variable for read model types
TModel = TypeVar("TModel", bound=ReadModel)


class InMemoryReadModelRepository(Generic[TModel]):
    """
    In-memory implementation of ReadModelRepository for testing.

    Stores read models in a Python dictionary, keyed by UUID. All
    operations are O(1) for ID-based access and O(n) for queries.

    This implementation is thread-safe via asyncio.Lock, making it
    suitable for testing async projections.

    Attributes:
        model_class: The ReadModel subclass this repository manages

    Example:
        >>> class OrderSummary(ReadModel):
        ...     order_number: str
        ...     status: str
        ...
        >>> repo = InMemoryReadModelRepository(OrderSummary)
        >>> await repo.save(OrderSummary(id=uuid4(), order_number="ORD-001", status="pending"))
        >>> summary = await repo.get(some_uuid)

    Note:
        - All data is lost when the repository instance is garbage collected
        - Use `clear()` method for test teardown
        - Query performance is O(n) - acceptable for testing but not production
    """

    def __init__(
        self,
        model_class: type[TModel],
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the in-memory repository.

        Args:
            model_class: The ReadModel subclass this repository will manage
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._model_class = model_class
        self._models: dict[UUID, TModel] = {}
        self._lock = asyncio.Lock()

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
            },
        ):
            async with self._lock:
                model = self._models.get(id)
                if model is None or model.is_deleted():
                    return None
                return model

    async def get_many(self, ids: list[UUID]) -> list[TModel]:
        """
        Get multiple read models by their IDs.

        Args:
            ids: List of unique identifiers

        Returns:
            List of found read models (soft-deleted excluded)
        """
        with self._tracer.span(
            "eventsource.readmodel.get_many",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_BATCH_SIZE: len(ids),
            },
        ):
            async with self._lock:
                result = []
                for id_ in ids:
                    model = self._models.get(id_)
                    if model is not None and not model.is_deleted():
                        result.append(model)
                return result

    async def save(self, model: TModel) -> None:
        """
        Save or update a read model (upsert semantics).

        Args:
            model: The read model to save
        """
        with self._tracer.span(
            "eventsource.readmodel.save",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(model.id),
            },
        ):
            async with self._lock:
                now = datetime.now(UTC)
                existing = self._models.get(model.id)

                if existing is None:
                    # Insert - set updated_at
                    model.updated_at = now
                else:
                    # Update - increment version and update timestamp
                    model.version = existing.version + 1
                    model.updated_at = now

                self._models[model.id] = model

    async def save_many(self, models: list[TModel]) -> None:
        """
        Save multiple read models in a batch.

        Args:
            models: List of read models to save
        """
        with self._tracer.span(
            "eventsource.readmodel.save_many",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_BATCH_SIZE: len(models),
            },
        ):
            async with self._lock:
                now = datetime.now(UTC)
                for model in models:
                    existing = self._models.get(model.id)

                    if existing is None:
                        model.updated_at = now
                    else:
                        model.version = existing.version + 1
                        model.updated_at = now

                    self._models[model.id] = model

    async def delete(self, id: UUID) -> bool:
        """
        Delete a read model by ID (hard delete).

        Args:
            id: Unique identifier of the read model to delete

        Returns:
            True if deleted, False if not found
        """
        with self._tracer.span(
            "eventsource.readmodel.delete",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
            },
        ):
            async with self._lock:
                if id in self._models:
                    del self._models[id]
                    return True
                return False

    async def soft_delete(self, id: UUID) -> bool:
        """
        Soft delete a read model by setting deleted_at.

        Args:
            id: Unique identifier of the read model to soft delete

        Returns:
            True if soft-deleted, False if not found or already deleted
        """
        with self._tracer.span(
            "eventsource.readmodel.soft_delete",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
            },
        ):
            async with self._lock:
                model = self._models.get(id)
                if model is None or model.is_deleted():
                    return False

                model.deleted_at = datetime.now(UTC)
                model.updated_at = model.deleted_at
                return True

    async def restore(self, id: UUID) -> bool:
        """
        Restore a soft-deleted read model.

        Args:
            id: Unique identifier of the read model to restore

        Returns:
            True if restored, False if not found or not deleted
        """
        with self._tracer.span(
            "eventsource.readmodel.restore",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
            },
        ):
            async with self._lock:
                model = self._models.get(id)
                if model is None or not model.is_deleted():
                    return False

                model.deleted_at = None
                model.updated_at = datetime.now(UTC)
                return True

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
            },
        ):
            async with self._lock:
                model = self._models.get(id)
                if model is None or not model.is_deleted():
                    return None
                return model

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
            },
        ):
            async with self._lock:
                # Start with only deleted models
                results = [m for m in self._models.values() if m.is_deleted()]

                # Apply filters
                for filter_ in query.filters:
                    results = [m for m in results if self._apply_filter(m, filter_)]

                # Apply ordering
                if query.order_by:
                    reverse = query.order_direction == "desc"
                    results.sort(
                        key=lambda m: getattr(m, query.order_by),  # type: ignore[arg-type]
                        reverse=reverse,
                    )

                # Apply pagination
                if query.offset:
                    results = results[query.offset :]
                if query.limit is not None:
                    results = results[: query.limit]

                return results

    async def exists(self, id: UUID) -> bool:
        """
        Check if a read model exists (and is not soft-deleted).

        Args:
            id: Unique identifier to check

        Returns:
            True if exists and not soft-deleted, False otherwise
        """
        with self._tracer.span(
            "eventsource.readmodel.exists",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_READMODEL_ID: str(id),
            },
        ):
            async with self._lock:
                model = self._models.get(id)
                return model is not None and not model.is_deleted()

    async def find(self, query: Query | None = None) -> list[TModel]:
        """
        Find read models matching a query.

        Args:
            query: Query with filters, ordering, pagination

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
            },
        ):
            async with self._lock:
                # Start with all models
                results = list(self._models.values())

                # Filter out soft-deleted unless requested
                if not query.include_deleted:
                    results = [m for m in results if not m.is_deleted()]

                # Apply filters
                for filter_ in query.filters:
                    results = [m for m in results if self._apply_filter(m, filter_)]

                # Apply ordering
                if query.order_by:
                    reverse = query.order_direction == "desc"
                    results.sort(
                        key=lambda m: getattr(m, query.order_by),  # type: ignore[arg-type]
                        reverse=reverse,
                    )

                # Apply pagination
                if query.offset:
                    results = results[query.offset :]
                if query.limit is not None:
                    results = results[: query.limit]

                return results

    async def count(self, query: Query | None = None) -> int:
        """
        Count read models matching a query.

        Args:
            query: Query with filters

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
            },
        ):
            async with self._lock:
                results = list(self._models.values())

                # Filter out soft-deleted unless requested
                if not query.include_deleted:
                    results = [m for m in results if not m.is_deleted()]

                # Apply filters
                for filter_ in query.filters:
                    results = [m for m in results if self._apply_filter(m, filter_)]

                return len(results)

    async def truncate(self) -> int:
        """
        Delete all read models.

        Returns:
            Number of records deleted
        """
        with self._tracer.span(
            "eventsource.readmodel.truncate",
            {ATTR_READMODEL_TYPE: self._model_class.__name__},
        ):
            async with self._lock:
                count = len(self._models)
                self._models.clear()
                return count

    async def clear(self) -> None:
        """
        Clear all data. Alias for truncate() for test compatibility.
        """
        await self.truncate()

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Verifies that the current database version matches the model's
        version before updating. If versions don't match, raises
        OptimisticLockError. On successful save, the version is incremented.

        Args:
            model: The read model to save

        Raises:
            OptimisticLockError: If the version in storage doesn't match
                the model's version
            ReadModelNotFoundError: If the model doesn't exist in storage

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
                ATTR_EXPECTED_VERSION: model.version,
            },
        ):
            async with self._lock:
                existing = self._models.get(model.id)

                if existing is None:
                    raise ReadModelNotFoundError(model.id)

                if existing.version != model.version:
                    raise OptimisticLockError(
                        model.id,
                        expected_version=model.version,
                        actual_version=existing.version,
                    )

                now = datetime.now(UTC)
                model.version = existing.version + 1
                model.updated_at = now
                self._models[model.id] = model

    def _apply_filter(self, model: TModel, filter_: Filter) -> bool:
        """
        Apply a single filter to a model.

        Args:
            model: The model to check
            filter_: The filter condition

        Returns:
            True if the model matches the filter
        """
        value = getattr(model, filter_.field, None)

        if filter_.operator == "eq":
            return bool(value == filter_.value)
        elif filter_.operator == "ne":
            return bool(value != filter_.value)
        elif filter_.operator == "gt":
            return bool(value > filter_.value)
        elif filter_.operator == "gte":
            return bool(value >= filter_.value)
        elif filter_.operator == "lt":
            return bool(value < filter_.value)
        elif filter_.operator == "lte":
            return bool(value <= filter_.value)
        elif filter_.operator == "in":
            return bool(value in filter_.value)
        elif filter_.operator == "not_in":
            return bool(value not in filter_.value)
        else:
            return False

    @property
    def model_class(self) -> type[TModel]:
        """Get the model class this repository manages."""
        return self._model_class

    def __len__(self) -> int:
        """Return the number of models (including soft-deleted)."""
        return len(self._models)
