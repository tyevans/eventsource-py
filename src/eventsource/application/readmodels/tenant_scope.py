"""Tenant-scoped read model repository wrapper (ADR-0114).

The wrapper sits in the application ring and uses only the public
`ReadModelRepository` port. Under an active tenant scope, every read adds a
`tenant_id` equality filter. Rows of other tenants, and rows with no tenant,
are not found. With no scope, every call passes through unchanged.

Writes check the owner of each id first. A foreign save raises
`TenantMismatchError`. A foreign delete, soft delete, or restore returns `False`.
`truncate` raises `TenantScopeOperationError` under a scope.
"""

from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

from eventsource.application.readmodels.owner_check import check_save_owner, id_owned_by
from eventsource.application.readmodels.tenant_scope_reads import ScopedReadsMixin
from eventsource.domain.exceptions.tenant import TenantScopeOperationError
from eventsource.ports.readmodels import ReadModel, ReadModelRepository


class TenantScopedReadModelRepository[TModel: ReadModel](ScopedReadsMixin[TModel]):
    """Wrap a read model repository and apply the active tenant to its reads.

    The read model must declare a `tenant_id` field. If it does not, the
    wrapped repository raises `ValueError` for the unknown field under a scope.
    The wrapper does not check the model class itself.

    Under a scope, a caller cannot pass its own `tenant_id` keyword filter.
    The wrapper raises `ValueError` for it. A `Query` filter on `tenant_id`
    still works, because it only narrows the result.
    """

    def __init__(
        self,
        repository: ReadModelRepository[TModel],
        *,
        require_tenant_context: bool = False,
    ) -> None:
        self._repository = repository
        self._require_tenant_context = require_tenant_context

    @property
    def repository(self) -> ReadModelRepository[TModel]:
        """The wrapped repository."""
        return self._repository

    # ---- Writes: owner checks under a scope ---------------------------------

    async def _check_saves(self, models: list[TModel]) -> None:
        """Check all models before any write, so a refused batch writes nothing."""
        if (tenant := self._active_tenant()) is not None:
            for model in models:
                await check_save_owner(self._repository, model, tenant)

    async def _owns_id(self, id: UUID) -> bool:
        """True when unscoped or when the active tenant owns the id."""
        if (tenant := self._active_tenant()) is None:
            return True
        return await id_owned_by(self._repository, id, tenant)

    async def save(self, model: TModel) -> None:
        await self._check_saves([model])
        await self._repository.save(model)

    async def save_many(self, models: Iterable[TModel]) -> None:
        batch = list(models)
        await self._check_saves(batch)
        await self._repository.save_many(batch)

    async def save_with_version_check(self, model: TModel) -> None:
        await self._check_saves([model])
        await self._repository.save_with_version_check(model)

    async def delete(self, id: UUID) -> bool:
        if not await self._owns_id(id):
            return False
        return await self._repository.delete(id)

    async def soft_delete(self, id: UUID) -> bool:
        if not await self._owns_id(id):
            return False
        return await self._repository.soft_delete(id)

    async def restore(self, id: UUID) -> bool:
        if not await self._owns_id(id):
            return False
        return await self._repository.restore(id)

    async def truncate(self) -> int:
        if self._active_tenant() is not None:
            raise TenantScopeOperationError("truncate")
        return await self._repository.truncate()


__all__ = ["TenantScopedReadModelRepository"]
