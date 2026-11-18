"""The one rule for the owner keyword that library DLQ writers pass (ADR-0114).

Both the projection DLQ writer and the subscription error handler use this
function. A DLQ repository that predates the ``tenant_id`` keyword keeps
working, because the keyword is passed only when the failed event has a tenant.
"""

from __future__ import annotations

from typing import Any


def dlq_owner_kwargs(source: object) -> dict[str, Any]:
    """Return ``{"tenant_id": ...}`` when *source* has a tenant, else ``{}``."""
    tenant_id = getattr(source, "tenant_id", None)
    return {} if tenant_id is None else {"tenant_id": tenant_id}


__all__ = ["dlq_owner_kwargs"]
