"""
Query operations mixin for InMemoryReadModelRepository.

Provides filtering, sorting, and pagination logic for in-memory read models.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any
from uuid import UUID

from eventsource.adapters._common import check_query, matches_filter
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_QUERY_FILTER_COUNT,
    ATTR_QUERY_LIMIT,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query


def filter_models[TModel: ReadModel](models: list[TModel], filters: list[Filter]) -> list[TModel]:
    """Keep the models that satisfy every filter (validated by `check_query` first)."""
    return [m for m in models if all(matches_filter(m, f) for f in filters)]


def shape_results[TModel: ReadModel](models: list[TModel], query: Query) -> list[TModel]:
    """Apply the filters, ordering and paging of a validated query to `models`.

    The caller runs `check_query` first and restricts `models` to the rows
    it may see (live rows for `find`, soft-deleted rows for `find_deleted`).
    """
    results = filter_models(models, query.filters)
    if query.order_by:
        results.sort(
            key=lambda m: getattr(m, query.order_by),  # type: ignore[arg-type]
            reverse=query.order_direction == "desc",
        )
    if query.offset:
        results = results[query.offset :]
    if query.limit is not None:
        results = results[: query.limit]
    return results


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

    async def find(self, query: Query | None = None, **eq_filters: Any) -> list[TModel]:
        """Find read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)

        check_query(self._model_class, query)
        with self._tracer.span(
            "eventsource.readmodel.find",
            {
                ATTR_READMODEL_TYPE: self._model_class.__name__,
                ATTR_QUERY_FILTER_COUNT: len(query.filters),
                ATTR_QUERY_LIMIT: query.limit if query.limit is not None else -1,
            },
        ):
            async with self._lock:
                live = list(self._models.values())
                if not query.include_deleted:
                    live = [m for m in live if not m.is_deleted()]
                results = shape_results(live, query)
                return [self._detach(m) for m in results]

    async def find_one(self, query: Query | None = None, **eq_filters: Any) -> TModel | None:
        """Find the first read model matching `query` and `**eq_filters`, or None."""
        base = _resolve_query(query, eq_filters)
        check_query(self._model_class, base)
        effective = replace(base, limit=1)
        results = await self.find(effective)
        return results[0] if results else None

    async def count(self, query: Query | None = None, **eq_filters: Any) -> int:
        """Count read models matching a query and optional equality keyword filters."""
        query = _resolve_query(query, eq_filters)

        check_query(self._model_class, query)
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
                return len(filter_models(results, query.filters))
