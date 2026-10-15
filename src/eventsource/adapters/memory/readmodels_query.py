"""
Query operations mixin for InMemoryReadModelRepository.

Provides filtering, sorting, and pagination logic for in-memory read models.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any
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


def _resolve_query(query: Query | None, eq_filters: dict[str, Any]) -> Query:
    base = query if query is not None else Query()
    if not eq_filters:
        return base
    return replace(
        base,
        filters=[*base.filters, *(Filter.eq(k, v) for k, v in eq_filters.items())],
    )


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
        """Apply a single filter to a model via shared dispatch in `adapters/_common`."""
        return matches_filter(model, filter_)

    async def find(self, query: Query | None = None, **eq_filters: Any) -> list[TModel]:
        """Find read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)

        with self._tracer.span(
            "eventsource.readmodel.find",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
                ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
            },
        ):
            async with self._lock:
                results = list(self._models.values())
                if not query.include_deleted:
                    results = [m for m in results if not m.is_deleted()]

                check_filters(self._model_class, query.filters)
                for filter_ in query.filters:
                    results = [m for m in results if self._apply_filter(m, filter_)]

                if query.order_by:
                    reverse = query.order_direction == "desc"
                    results.sort(
                        key=lambda m: getattr(m, query.order_by),  # type: ignore[arg-type]
                        reverse=reverse,
                    )

                if query.offset:
                    results = results[query.offset :]
                if query.limit is not None:
                    results = results[: query.limit]

                return [self._detach(m) for m in results]

    async def find_one(self, query: Query | None = None, **eq_filters: Any) -> TModel | None:
        """Find the first read model matching `query` and `**eq_filters`, or None."""
        effective = replace(_resolve_query(query, eq_filters), limit=1)
        results = await self.find(effective)
        return results[0] if results else None

    async def count(self, query: Query | None = None, **eq_filters: Any) -> int:
        """Count read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)

        with self._tracer.span(
            "eventsource.readmodel.count",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
            },
        ):
            async with self._lock:
                results = list(self._models.values())
                if not query.include_deleted:
                    results = [m for m in results if not m.is_deleted()]

                check_filters(self._model_class, query.filters)
                for filter_ in query.filters:
                    results = [m for m in results if self._apply_filter(m, filter_)]

                return len(results)
