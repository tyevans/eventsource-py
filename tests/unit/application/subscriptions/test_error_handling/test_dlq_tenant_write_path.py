"""A subscription error handler with a DLQ strategy records the failed event's tenant.

Drives the real caller: SubscriptionErrorHandler.handle_error, which classifies
the error as non-retryable and writes to the DLQ.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID, uuid4

from eventsource import tenant_scope
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.application.subscriptions.error_handling import (
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    SubscriptionErrorHandler,
)
from eventsource.domain.event import DomainEvent
from eventsource.observability.error_summary import error_summary


class OrderPlaced(DomainEvent):
    """Event that the failing subscription handles."""

    aggregate_type: str = "Order"


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
        self.calls.append({"event_id": event_id})


def _handler(dlq_repo: Any) -> SubscriptionErrorHandler:
    return SubscriptionErrorHandler(
        subscription_name="OrderSubscription",
        config=ErrorHandlingConfig(strategy=ErrorHandlingStrategy.RETRY_THEN_DLQ, dlq_enabled=True),
        dlq_repo=dlq_repo,
    )


async def test_failed_event_entry_carries_event_tenant() -> None:
    dlq = InMemoryDLQRepository(enable_tracing=False)
    tenant_a = uuid4()
    event = OrderPlaced(aggregate_id=uuid4(), tenant_id=tenant_a)

    info = await _handler(dlq).handle_error(ValueError("Invalid data"), event=event)

    assert info.sent_to_dlq is True
    entries = await dlq.get_failed_events()
    assert len(entries) == 1
    assert entries[0].event_id == event.event_id
    assert entries[0].tenant_id == tenant_a


async def test_failed_event_entry_is_invisible_under_other_tenant_scope() -> None:
    dlq = InMemoryDLQRepository(enable_tracing=False)
    tenant_a, tenant_b = uuid4(), uuid4()
    event = OrderPlaced(aggregate_id=uuid4(), tenant_id=tenant_a)

    await _handler(dlq).handle_error(ValueError("Invalid data"), event=event)

    async with tenant_scope(tenant_b):
        assert await dlq.get_failed_events() == []

    async with tenant_scope(tenant_a):
        assert len(await dlq.get_failed_events()) == 1


async def test_tenantless_event_reaches_legacy_dlq_repository() -> None:
    dlq = _LegacyDLQRepository()
    event = OrderPlaced(aggregate_id=uuid4())

    info = await _handler(dlq).handle_error(ValueError("Invalid data"), event=event)

    assert info.sent_to_dlq is True
    assert dlq.calls == [{"event_id": event.event_id}]


async def test_dlq_write_failure_logs_summary_without_traceback(caplog: Any) -> None:
    event = OrderPlaced(aggregate_id=uuid4())
    failure = RuntimeError("dlq store unavailable")

    class _Failing:
        async def add_failed_event(self, **kwargs: Any) -> None:
            raise failure

    with caplog.at_level(logging.ERROR):
        await _handler(_Failing()).handle_error(ValueError("Invalid data"), event=event)

    records = [r for r in caplog.records if "Failed to send event to DLQ" in r.getMessage()]
    assert len(records) == 1
    assert records[0].getMessage() == (f"Failed to send event to DLQ: {error_summary(failure)}")
    assert records[0].exc_info is None
