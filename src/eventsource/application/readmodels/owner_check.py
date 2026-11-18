"""Owner checks for tenant-scoped read model writes (ADR-0114 T2).

One rule, used only by `TenantScopedReadModelRepository`. An id belongs to the
tenant that first saved it. A write under a scope is allowed only when the id
is free or already owned by the active tenant. The lookup includes soft-deleted
rows, so a soft-deleted foreign row still protects its id.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from eventsource.domain.exceptions.tenant import TenantMismatchError
from eventsource.ports.readmodels import Query, ReadModel, ReadModelRepository

_TENANT_FIELD = "tenant_id"


def tenant_of(model: object) -> UUID | None:
    """Tenant that owns a read model instance, or None when it has no tenant field."""
    return getattr(model, _TENANT_FIELD, None)


async def owner_row(repository: ReadModelRepository[Any], id: UUID) -> ReadModel | None:
    """Row with this id, soft-deleted rows included, read without any tenant filter."""
    return await repository.find_one(Query(include_deleted=True), id=id)


async def id_owned_by(repository: ReadModelRepository[Any], id: UUID, tenant: UUID) -> bool:
    """True when the row with this id exists and belongs to the tenant."""
    row = await owner_row(repository, id)
    return row is not None and tenant_of(row) == tenant


async def check_save_owner(
    repository: ReadModelRepository[Any], model: ReadModel, tenant: UUID
) -> None:
    """Raise TenantMismatchError unless the model may be written under this tenant.

    Refused when the model's tenant is not the active tenant, or when a row with
    the same id belongs to another tenant. The error reveals that the id exists
    for the other tenant. ADR-0114 accepts that residual risk.
    """
    if (model_tenant := tenant_of(model)) != tenant:
        raise TenantMismatchError(expected=tenant, actual=model_tenant, event_ids=[model.id])
    if (row := await owner_row(repository, model.id)) is not None and tenant_of(row) != tenant:
        raise TenantMismatchError(expected=tenant, actual=tenant_of(row), event_ids=[model.id])


__all__ = ["check_save_owner", "id_owned_by", "owner_row", "tenant_of"]
