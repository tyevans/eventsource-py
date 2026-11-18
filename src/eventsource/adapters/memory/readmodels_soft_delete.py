"""Soft delete operations mixin for InMemoryReadModelRepository."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from eventsource.adapters._common import check_query
from eventsource.adapters.memory.readmodels_query import shape_results
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Query


class InMemoryReadModelSoftDeleteMixin[TModel: ReadModel]:
    """Mixin providing soft-delete operations for in-memory read models."""

    _tracer: Tracer
    _model_class: type[TModel]
    _models: dict[UUID, TModel]
    _lock: asyncio.Lock

    @staticmethod
    def _detach(model: TModel) -> TModel:
        """Return an independent copy, so no reference crosses the boundary."""
        return model.model_copy(deep=True)

    async def soft_delete(self, id: UUID) -> bool:
        """Soft delete a read model by setting deleted_at."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(id)}
        with self._tracer.span("eventsource.readmodel.soft_delete", attrs):
            async with self._lock:
                model = self._models.get(id)
                if model is None or model.is_deleted():
                    return False
                model.deleted_at = datetime.now(UTC)
                model.updated_at = model.deleted_at
                return True

    async def restore(self, id: UUID) -> bool:
        """Restore a soft-deleted read model."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(id)}
        with self._tracer.span("eventsource.readmodel.restore", attrs):
            async with self._lock:
                model = self._models.get(id)
                if model is None or not model.is_deleted():
                    return False
                model.deleted_at = None
                model.updated_at = datetime.now(UTC)
                return True

    async def get_deleted(self, id: UUID) -> TModel | None:
        """Get a soft-deleted read model by ID, or None if not found or not deleted."""
        attrs = {ATTR_READMODEL_TYPE: self._model_class.__name__, ATTR_READMODEL_ID: str(id)}
        with self._tracer.span("eventsource.readmodel.get_deleted", attrs):
            async with self._lock:
                model = self._models.get(id)
                if model is None or not model.is_deleted():
                    return None
                return self._detach(model)

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        """Find only soft-deleted read models matching a query."""
        query = query or Query()
        check_query(self._model_class, query)
        attrs = {
            ATTR_READMODEL_TYPE: self._model_class.__name__,
            ATTR_QUERY_FILTER_COUNT: len(query.filters),
            ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
        }
        with self._tracer.span("eventsource.readmodel.find_deleted", attrs):
            async with self._lock:
                deleted = [m for m in self._models.values() if m.is_deleted()]
                results = shape_results(deleted, query)
                return [self._detach(m) for m in results]
