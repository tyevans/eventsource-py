"""
Query operations mixin for InMemoryReadModelRepository.

Provides filtering, sorting, and pagination logic for in-memory read models.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

from eventsource.adapters._common import check_filters, matches_filter
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query


class InMemoryReadModelQueryMixin[TModel: ReadModel]:
    """Mixin providing query operations for in-memory read models."""

    _tracer: Tracer
    _model_class: type[TModel]
    _models: dict[UUID, TModel]
    _lock: asyncio.Lock

    @staticmethod
    def _detach(model: TModel) -> TModel:
        """Return an independent copy, so no reference crosses the boundary."""
        return model.model_copy(deep=True)

    def _apply_filter(self, model: TModel, filter_: Filter) -> bool:
        """
        Apply a single filter to a model.

        Delegates to the shared dispatch in `adapters/_common` so this
        adapter cannot drift from the SQL ones -- see
        `ReadModelRepository.find` for the semantics.

        Args:
            model: The model to check
            filter_: The filter condition

        Returns:
            True if the model matches the filter

        Raises:
            ValueError: On an unknown field name or an unknown operator.
        """
        return matches_filter(model, filter_)

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

                # Apply filters. Validated first: an empty candidate set
                # must not hide a typo'd field or a bad operator.
                check_filters(self._model_class, query.filters)
                for filter_ in query.filters:
                    results = [m for m in results if self._apply_filter(m, filter_)]

                return len(results)
