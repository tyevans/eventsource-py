"""
Soft delete operations mixin for InMemoryReadModelRepository.

Provides soft-delete, restore, and soft-deleted querying for in-memory read models.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from eventsource.adapters._common import check_filters, matches_filter
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_ID,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query


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

    def _apply_filter(self, model: TModel, filter_: Filter) -> bool:
        """Apply a single filter to a model."""
        return matches_filter(model, filter_)

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
                return self._detach(model)

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

                # Apply filters. Validated first: an empty candidate set
                # must not hide a typo'd field or a bad operator.
                check_filters(self._model_class, query.filters)
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

                return [self._detach(m) for m in results]
