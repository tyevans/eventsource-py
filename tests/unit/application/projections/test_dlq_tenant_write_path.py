"""A projection's permanent failure records the failed event's tenant in the DLQ.

Drives the real caller: a DeclarativeProjection whose handler always raises,
with no retries, so the failure reaches the DLQ write in send_to_dlq.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

from eventsource import tenant_scope
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.application.projections import DeclarativeProjection
from eventsource.application.projections.retry import NoRetryPolicy
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent


class OrderPlaced(DomainEvent):
    """Event that the failing projection subscribes to."""

    aggregate_type: str = "Order"


class _AlwaysFailing(DeclarativeProjection):
    def __init__(self, dlq_repo: Any) -> None:
        super().__init__(dlq_repo=dlq_repo, retry_policy=NoRetryPolicy())

    @handles(OrderPlaced)
    async def _on_placed(self, event: OrderPlaced) -> None:
        raise ValueError("handler failed")


class _LegacyDLQRepository:
    """Third-party DLQ repository that predates the tenant_id keyword."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        self.calls.append({"event_id": event_id, "projection_name": projection_name})


async def test_failed_event_entry_carries_event_tenant() -> None:
    dlq = InMemoryDLQRepository(enable_tracing=False)
    tenant_a = uuid4()
    event = OrderPlaced(aggregate_id=uuid4(), tenant_id=tenant_a)

    with pytest.raises(ValueError, match="handler failed"):
        await _AlwaysFailing(dlq).handle(event)

    entries = await dlq.get_failed_events()
    assert len(entries) == 1
    assert entries[0].event_id == event.event_id
    assert entries[0].tenant_id == tenant_a


async def test_failed_event_entry_is_invisible_under_other_tenant_scope() -> None:
    dlq = InMemoryDLQRepository(enable_tracing=False)
    tenant_a, tenant_b = uuid4(), uuid4()
    event = OrderPlaced(aggregate_id=uuid4(), tenant_id=tenant_a)

    with pytest.raises(ValueError, match="handler failed"):
        await _AlwaysFailing(dlq).handle(event)

    async with tenant_scope(tenant_b):
        assert await dlq.get_failed_events() == []

    async with tenant_scope(tenant_a):
        assert len(await dlq.get_failed_events()) == 1


async def test_tenantless_event_reaches_legacy_dlq_repository() -> None:
    dlq = _LegacyDLQRepository()
    event = OrderPlaced(aggregate_id=uuid4())

    with pytest.raises(ValueError, match="handler failed"):
        await _AlwaysFailing(dlq).handle(event)

    assert len(dlq.calls) == 1
    assert dlq.calls[0]["event_id"] == event.event_id
    assert dlq.calls[0]["projection_name"] == "_AlwaysFailing"
