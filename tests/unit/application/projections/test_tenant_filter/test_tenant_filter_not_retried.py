"""A missing tenant scope is a configuration error: it is raised at once, never retried."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

from eventsource import TenantContextNotSetError, get_current_tenant
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.application.projections import DeclarativeProjection
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from tests.unit.application.projections.test_tenant_filter._fixtures import OrderCreated


class _CountingReader(DeclarativeProjection):
    """Uses the DEFAULT retry policy (two retries, 2s and 4s backoff) and counts attempts."""

    def __init__(self, dlq_repo: InMemoryDLQRepository) -> None:
        super().__init__(tenant_filter=get_current_tenant, dlq_repo=dlq_repo)
        self.attempts = 0

    async def _process_event(self, event: DomainEvent) -> None:
        self.attempts += 1
        await super()._process_event(event)

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        pass


class TestUnscopedFilterIsNotRetried:
    async def test_raises_promptly_under_default_retry_policy(self) -> None:
        dlq = InMemoryDLQRepository(enable_tracing=False)
        projection = _CountingReader(dlq)
        event = OrderCreated(aggregate_id=uuid4(), tenant_id=uuid4())

        started = time.perf_counter()
        with pytest.raises(TenantContextNotSetError):
            await projection.handle(event)
        elapsed = time.perf_counter() - started

        assert elapsed < 0.5
        assert projection.attempts == 1

    async def test_writes_no_dlq_row(self) -> None:
        dlq = InMemoryDLQRepository(enable_tracing=False)
        projection = _CountingReader(dlq)
        with pytest.raises(TenantContextNotSetError):
            await projection.handle(OrderCreated(aggregate_id=uuid4(), tenant_id=uuid4()))
        assert await dlq.get_failed_events() == []
