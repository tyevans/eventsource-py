"""In-memory implementation of read model repository."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime
from uuid import UUID

from eventsource.adapters.memory.readmodels_query import InMemoryReadModelQueryMixin
from eventsource.adapters.memory.readmodels_soft_delete import InMemoryReadModelSoftDeleteMixin
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (  # fmt: skip
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
    """In-memory ReadModelRepository storing deep copies under an asyncio.Lock."""

    def __init__(
        self,
        model_class: type[TModel],
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._model_class = model_class
        self._models: dict[UUID, TModel] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _detach(model: TModel) -> TModel:
        return model.model_copy(deep=True)

    def _store_unlocked(self, model: TModel, now: datetime) -> None:
        existing, stored = self._models.get(model.id), self._detach(model)
        if existing is not None:
            stored.version = existing.version + 1
        stored.updated_at = now
        self._models[model.id] = stored

    async def get(self, id: UUID) -> TModel | None:
        """Get a read model by ID if found and not soft-deleted, else None."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(id)}
        with self._tracer.span("eventsource.readmodel.get", attrs):
            async with self._lock:
                model = self._models.get(id)
                return None if model is None or model.is_deleted() else self._detach(model)

    async def get_or_raise(self, id: UUID) -> TModel:
        """Get a read model by ID or raise ReadModelNotFoundError if missing/deleted."""
        if (model := await self.get(id)) is None:
            raise ReadModelNotFoundError(id)
        return model

    async def get_many(self, ids: Iterable[UUID]) -> list[TModel]:
        """Get multiple read models by their IDs (excluding soft-deleted)."""
        id_list = list(ids)
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_BATCH_SIZE: len(id_list)}
        with self._tracer.span("eventsource.readmodel.get_many", attrs):
            async with self._lock:
                return [self._detach(m) for id_ in id_list if (m := self._models.get(id_)) is not None and not m.is_deleted()]  # fmt: skip

    async def save(self, model: TModel) -> None:
        """Save or update a read model (upsert semantics)."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(model.id)}
        with self._tracer.span("eventsource.readmodel.save", attrs):
            async with self._lock:
                self._store_unlocked(model, datetime.now(UTC))

    async def save_many(self, models: Iterable[TModel]) -> None:
        """Save multiple read models in a batch."""
        model_list = list(models)
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_BATCH_SIZE: len(model_list)}
        with self._tracer.span("eventsource.readmodel.save_many", attrs):
            async with self._lock:
                now = datetime.now(UTC)
                for model in model_list:
                    self._store_unlocked(model, now)

    async def delete(self, id: UUID) -> bool:
        """Delete a read model by ID (hard delete). Returns True if deleted."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(id)}
        with self._tracer.span("eventsource.readmodel.delete", attrs):
            async with self._lock:
                return self._models.pop(id, None) is not None

    async def exists(self, id: UUID) -> bool:
        """Check if a read model exists and is not soft-deleted."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(id)}
        with self._tracer.span("eventsource.readmodel.exists", attrs):
            async with self._lock:
                model = self._models.get(id)
                return model is not None and not model.is_deleted()

    async def truncate(self) -> int:
        """Delete all read models and return the number of records deleted."""
        with self._tracer.span(
            "eventsource.readmodel.truncate", {ATTR_READMODEL_TYPE: self._model_class.__name__}
        ):
            async with self._lock:
                count = len(self._models)
                self._models.clear()
                return count

    async def clear(self) -> int:
        """Clear all stored read models (alias for truncate)."""
        return await self.truncate()

    async def save_with_version_check(self, model: TModel) -> None:
        """Save a read model with optimistic locking version check."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(model.id), ATTR_EXPECTED_VERSION: model.version}  # fmt: skip
        with self._tracer.span("eventsource.readmodel.save_with_version_check", attrs):
            async with self._lock:
                if (existing := self._models.get(model.id)) is None:
                    raise ReadModelNotFoundError(model.id)
                if existing.version != model.version:
                    raise ReadModelVersionConflictError(
                        model.id, expected_version=model.version, actual_version=existing.version
                    )
                self._store_unlocked(model, datetime.now(UTC))

    @property
    def model_class(self) -> type[TModel]:
        return self._model_class

    def __len__(self) -> int:
        return len(self._models)

    def __bool__(self) -> bool:
        return True


__all__ = ["InMemoryReadModelQueryMixin", "InMemoryReadModelRepository", "InMemoryReadModelSoftDeleteMixin"]  # fmt: skip
