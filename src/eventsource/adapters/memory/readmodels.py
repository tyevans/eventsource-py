"""
In-memory implementation of read model repository.

Provides a simple, fast repository for testing and development.
All data is stored in memory and lost when the process terminates.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from eventsource.adapters.memory.readmodels_query import InMemoryReadModelQueryMixin
from eventsource.adapters.memory.readmodels_soft_delete import (
    InMemoryReadModelSoftDeleteMixin,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_EXPECTED_VERSION,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.exceptions import (
    ReadModelNotFoundError,
    ReadModelVersionConflictError,
)
from eventsource.ports.readmodels.model import ReadModel


class InMemoryReadModelRepository[TModel: ReadModel](
    InMemoryReadModelSoftDeleteMixin[TModel],
    InMemoryReadModelQueryMixin[TModel],
):
    """
    In-memory implementation of ReadModelRepository for testing.

    Stores read models in a Python dictionary, keyed by UUID. All
    operations are O(1) for ID-based access and O(n) for queries.

    This implementation is thread-safe via asyncio.Lock, making it
    suitable for testing async projections.

    Stored models are private to the repository. Reads hand back a copy and
    writes take a copy, so a caller can never hold a reference into the
    store -- matching the SQL adapters, which hydrate a fresh instance from
    a row on every read and mutate only the row on every write. Without
    that, a model a caller saved or fetched could be mutated underneath it
    by a later unrelated write, on the memory backend only.

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

    @staticmethod
    def _detach(model: TModel) -> TModel:
        """Return an independent copy, so no reference crosses the boundary.

        Used on both sides: on read so the caller cannot reach the stored
        object, and on write so a later store-side mutation (`soft_delete`,
        `restore`, a version bump) cannot reach the caller's object.
        """
        return model.model_copy(deep=True)

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
                return self._detach(model)

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
                        result.append(self._detach(model))
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

                stored = self._detach(model)
                if existing is not None:
                    stored.version = existing.version + 1
                stored.updated_at = now

                self._models[model.id] = stored

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

                    stored = self._detach(model)
                    if existing is not None:
                        stored.version = existing.version + 1
                    stored.updated_at = now

                    self._models[model.id] = stored

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
        ReadModelVersionConflictError. On successful save, the version is incremented.

        Args:
            model: The read model to save

        Raises:
            ReadModelVersionConflictError: If the version in storage doesn't match
                the model's version
            ReadModelNotFoundError: If the model doesn't exist in storage

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
                ATTR_EXPECTED_VERSION: model.version,
            },
        ):
            async with self._lock:
                existing = self._models.get(model.id)

                if existing is None:
                    raise ReadModelNotFoundError(model.id)

                if existing.version != model.version:
                    raise ReadModelVersionConflictError(
                        model.id,
                        expected_version=model.version,
                        actual_version=existing.version,
                    )

                stored = self._detach(model)
                stored.version = existing.version + 1
                stored.updated_at = datetime.now(UTC)
                self._models[model.id] = stored

    @property
    def model_class(self) -> type[TModel]:
        """Get the model class this repository manages."""
        return self._model_class

    def __len__(self) -> int:
        """Return the number of models (including soft-deleted)."""
        return len(self._models)


__all__ = [
    "InMemoryReadModelQueryMixin",
    "InMemoryReadModelRepository",
    "InMemoryReadModelSoftDeleteMixin",
]
