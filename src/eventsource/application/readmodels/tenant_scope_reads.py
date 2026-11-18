"""Tenant-filtered reads for the scoped read model repository (ADR-0114).

Split out of `tenant_scope.py` to keep both modules under the line ceiling.
Use `TenantScopedReadModelRepository`, which composes this mixin. Do not
import this module directly.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from uuid import UUID

from eventsource.domain.tenant_context import get_current_tenant, get_required_tenant
from eventsource.ports.readmodels import Filter, Query, ReadModel, ReadModelRepository

_TENANT_FIELD = "tenant_id"


class ScopedReadsMixin[TModel: ReadModel]:
    """Reads that add the active tenant as an equality filter.

    The host class must set `_repository` and `_require_tenant_context`.
    """

    _repository: ReadModelRepository[TModel]
    _require_tenant_context: bool

    def _active_tenant(self) -> UUID | None:
        """Active tenant, or None when unscoped. Raises if a scope is required and absent."""
        return get_required_tenant() if self._require_tenant_context else get_current_tenant()

    def _scoped_query(self, query: Query | None, eq_filters: dict[str, Any], tenant: UUID) -> Query:
        """Fold the caller's query and keyword filters into one query with the tenant filter."""
        if _TENANT_FIELD in eq_filters:
            raise ValueError(
                "tenant_id is set by the active tenant scope and cannot be passed as a filter"
            )
        base = query if query is not None else Query()
        return base.with_filter(**eq_filters, **{_TENANT_FIELD: tenant})

    async def get(self, id: UUID) -> TModel | None:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.get(id)
        return await self._repository.find_one(id=id, tenant_id=tenant)

    async def get_many(self, ids: Iterable[UUID]) -> list[TModel]:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.get_many(ids)
        query = Query.where(Filter.in_("id", list(ids)))
        return await self._repository.find(self._scoped_query(query, {}, tenant))

    async def get_deleted(self, id: UUID) -> TModel | None:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.get_deleted(id)
        found = await self._repository.find_deleted(
            self._scoped_query(Query.where(id=id), {}, tenant)
        )
        return found[0] if found else None

    async def find_deleted(self, query: Query | None = None) -> list[TModel]:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.find_deleted(query)
        return await self._repository.find_deleted(self._scoped_query(query, {}, tenant))

    async def exists(self, id: UUID) -> bool:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.exists(id)
        return await self._repository.count(Query.where(id=id, tenant_id=tenant)) > 0

    async def find(self, query: Query | None = None, **eq_filters: Any) -> list[TModel]:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.find(query, **eq_filters)
        return await self._repository.find(self._scoped_query(query, eq_filters, tenant))

    async def find_one(self, query: Query | None = None, **eq_filters: Any) -> TModel | None:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.find_one(query, **eq_filters)
        return await self._repository.find_one(self._scoped_query(query, eq_filters, tenant))

    async def count(self, query: Query | None = None, **eq_filters: Any) -> int:
        if (tenant := self._active_tenant()) is None:
            return await self._repository.count(query, **eq_filters)
        return await self._repository.count(self._scoped_query(query, eq_filters, tenant))
