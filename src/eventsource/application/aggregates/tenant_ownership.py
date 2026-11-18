"""Owner probe and append guard for TenantAwareRepository (ADR-0113).

The owner of a stream is the tenant of its first event. A stream that the
active tenant does not own reads as missing: the probe reports version 0, and
an append to it raises AggregateNotFoundError.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.domain.exceptions import AggregateNotFoundError
from eventsource.domain.tenant_context import get_current_tenant, get_required_tenant
from eventsource.ports.envelopes import StreamReadOptions

if TYPE_CHECKING:
    from eventsource.application.aggregates.repository import AggregateRepository

logger = logging.getLogger(__name__)


async def owned_stream_version(
    repository: AggregateRepository[Any], aggregate_id: UUID, tenant_id: UUID
) -> int:
    """Return the stream version when ``tenant_id`` owns the stream, else 0.

    Costs one version lookup, plus one read of the first event when the
    stream has events. A missing stream returns 0 without the read.
    """
    stream, store = repository.stream_for(aggregate_id), repository.event_store
    version = await store.get_stream_version(stream)
    if version == 0:
        return 0
    first = [env async for env in store.read_stream(stream, StreamReadOptions(limit=1))]
    if first and getattr(first[0].event, "tenant_id", None) == tenant_id:
        return version
    logger.warning(
        "Stream %s/%s is not owned by tenant %s. Reporting it as missing.",
        stream.category, aggregate_id, tenant_id,
    )  # fmt: skip
    return 0


async def require_owned_append(
    repository: AggregateRepository[Any], aggregate_id: UUID, tenant_id: UUID
) -> None:
    """Raise AggregateNotFoundError unless ``tenant_id`` owns a non-empty stream.

    Call before an append to a stream the caller believes already exists. A
    foreign stream raises the same error as a missing one, and nothing is appended.
    """
    if await owned_stream_version(repository, aggregate_id, tenant_id) == 0:
        raise AggregateNotFoundError(aggregate_id, repository.aggregate_type)


def active_scope_tenant(*, require_context: bool) -> UUID | None:
    """Active tenant, or None when unscoped. Raises when a scope is required and absent."""
    return get_required_tenant() if require_context else get_current_tenant()


__all__ = ["active_scope_tenant", "owned_stream_version", "require_owned_append"]
