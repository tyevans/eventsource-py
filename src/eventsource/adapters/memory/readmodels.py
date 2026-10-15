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

    Stores read models in a Python dictionary keyed by UUID, protected by
    `asyncio.Lock`. Stored models are private to the repository: reads return
    a deep copy and writes store a deep copy, matching the SQL adapters so
    callers never hold a mutable reference into the store.
    """

    def __init__(
        self,
        model_class: type[TModel],
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the in-memory repository for `model_class`."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._model_class = model_class
        self._models: dict[UUID, TModel] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _detach(model: TModel) -> TModel:
        """Return an independent deep copy so no reference crosses the boundary."""
        return model.model_copy(deep=True)

    async def get(self, id: UUID) -> TModel | None:
        """Get a read model by ID if found and not soft-deleted, else None."""
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
        """Get multiple read models by their IDs (excluding soft-deleted)."""
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
        """Save or update a read model (upsert semantics)."""
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
        """Save multiple read models in a batch."""
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
        """Delete a read model by ID (hard delete). Returns True if deleted."""
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
        """Check if a read model exists and is not soft-deleted."""
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
        """Delete all read models and return the number of records deleted."""
        with self._tracer.span(
            "eventsource.readmodel.truncate",
            {ATTR_READMODEL_TYPE: self._model_class.__name__},
        ):
            async with self._lock:
                count = len(self._models)
                self._models.clear()
                return count

    async def save_with_version_check(self, model: TModel) -> None:
        """
        Save a read model with optimistic locking version check.

        Raises:
            ReadModelNotFoundError: If the model doesn't exist in storage
            ReadModelVersionConflictError: If stored version != model.version
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
