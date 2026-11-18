"""Fail-closed tenant filtering: an unscoped callable filter raises instead of fanning out."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource import TenantContextNotSetError, get_current_tenant, tenant_scope
from eventsource.application.projections import DeclarativeProjection, TenantFilter
from eventsource.application.projections.replay import ReplayFailedError, replay
from eventsource.application.projections.retry import NoRetryPolicy
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from tests.unit.application.projections.test_replay._fixtures import OrderPlaced, _store_with
from tests.unit.application.projections.test_tenant_filter._fixtures import OrderCreated


class _Reader(DeclarativeProjection):
    """Default (fail-closed) projection. No retries, so a raise surfaces at once."""

    def __init__(self, tenant_filter: TenantFilter) -> None:
        super().__init__(tenant_filter=tenant_filter, retry_policy=NoRetryPolicy())
        self.seen: list[DomainEvent] = []

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        self.seen.append(event)

    @handles(OrderPlaced)
    async def _on_order_placed(self, event: OrderPlaced) -> None:
        self.seen.append(event)


class _LegacyReader(_Reader):
    """Explicit opt-out: a callable that resolves to None processes every tenant."""

    tenant_filter_allow_unscoped = True


def _order(tenant_id: UUID | None) -> OrderCreated:
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id)


class TestUnscopedCallableFilterFailsClosed:
    async def test_handle_raises_when_callable_resolves_to_none(self) -> None:
        projection = _Reader(get_current_tenant)
        with pytest.raises(TenantContextNotSetError):
            await projection.handle(_order(uuid4()))
        assert projection.seen == []

    async def test_raise_applies_to_events_without_a_tenant_too(self) -> None:
        projection = _Reader(get_current_tenant)
        with pytest.raises(TenantContextNotSetError):
            await projection.handle(_order(None))
        assert projection.seen == []

    async def test_scoped_callable_still_filters_by_the_scope(
        self, tenant_id_1: UUID, tenant_id_2: UUID
    ) -> None:
        projection = _Reader(get_current_tenant)
        async with tenant_scope(tenant_id_1):
            await projection.handle(_order(tenant_id_1))
            await projection.handle(_order(tenant_id_2))
        assert [e.tenant_id for e in projection.seen] == [tenant_id_1]

    async def test_legacy_opt_out_processes_every_tenant(
        self, tenant_id_1: UUID, tenant_id_2: UUID
    ) -> None:
        projection = _LegacyReader(get_current_tenant)
        await projection.handle(_order(tenant_id_1))
        await projection.handle(_order(tenant_id_2))
        assert len(projection.seen) == 2

    async def test_replay_reports_the_error_as_a_failure(self) -> None:
        event = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(event)
        projection = _Reader(get_current_tenant)
        report = await replay(store, [projection])
        (failure,) = report.failures
        assert isinstance(failure.error, TenantContextNotSetError)
        assert failure.event_id == event.event_id
        assert report.applied == 0 and projection.seen == []

    async def test_strict_replay_raises_the_error(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        with pytest.raises(ReplayFailedError) as raised:
            await replay(store, [_Reader(get_current_tenant)], strict=True)
        assert isinstance(raised.value.failure.error, TenantContextNotSetError)


class TestStaticFilterIsUnaffected:
    async def test_uuid_filter_never_raises(self, tenant_id_1: UUID, tenant_id_2: UUID) -> None:
        projection = _Reader(tenant_id_1)
        await projection.handle(_order(tenant_id_1))
        await projection.handle(_order(tenant_id_2))
        assert [e.tenant_id for e in projection.seen] == [tenant_id_1]

    async def test_no_filter_processes_everything(self, tenant_id_1: UUID) -> None:
        projection = _Reader(None)
        await projection.handle(_order(None))
        await projection.handle(_order(tenant_id_1))
        assert len(projection.seen) == 2


class TestEventWithoutTenantNeverMatchesASetFilter:
    async def test_uuid_filter_skips_event_with_none_tenant(self, tenant_id_1: UUID) -> None:
        projection = _Reader(tenant_id_1)
        await projection.handle(_order(None))
        assert projection.seen == []

    async def test_callable_filter_skips_event_with_none_tenant(self, tenant_id_1: UUID) -> None:
        projection = _Reader(get_current_tenant)
        async with tenant_scope(tenant_id_1):
            await projection.handle(_order(None))
        assert projection.seen == []

    async def test_skipped_none_tenant_event_is_not_a_failure(self, tenant_id_1: UUID) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [_Reader(tenant_id_1)])
        assert report.failures == () and report.applied == 1
