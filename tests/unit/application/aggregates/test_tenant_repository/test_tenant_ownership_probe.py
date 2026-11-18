"""Scoped existence, version, and load-or-create checks against foreign streams (ADR-0113)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource import TenantContextNotSetError, clear_tenant_context, tenant_scope
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain import StreamId
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.domain.exceptions import AggregateNotFoundError, OptimisticLockError
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.aggregates.test_tenant_repository._fixtures import (
    NonTenantOrderCreated,
    TenantOrderAggregate,
)


class _ClearTenantMixin:
    def setup_method(self) -> None:
        clear_tenant_context()

    def teardown_method(self) -> None:
        clear_tenant_context()


class _Harness:
    def __init__(self, *, require_tenant_context: bool = True) -> None:
        self.store = InMemoryEventStore()
        self.base = AggregateRepository(self.store, TenantOrderAggregate)
        self.repo = TenantAwareRepository(self.base, require_tenant_context=require_tenant_context)

    async def create_owned(self, tenant: UUID, aggregate_id: UUID) -> None:
        async with tenant_scope(tenant):
            order = self.repo.create_new(aggregate_id)
            order.create(customer_id=uuid4())
            await self.repo.save(order)


class TestForeignStreamProbes(_ClearTenantMixin):
    @pytest.fixture
    def harness(self) -> _Harness:
        return _Harness()

    async def test_foreign_stream_reads_as_missing_for_exists_and_version(self, harness: _Harness) -> None:  # fmt: skip
        tenant_a, tenant_b, order_id = uuid4(), uuid4(), uuid4()
        await harness.create_owned(tenant_a, order_id)
        async with tenant_scope(tenant_b):
            assert await harness.repo.exists(order_id) is False
            assert await harness.repo.get_version(order_id) == 0

    async def test_owner_sees_real_existence_and_version(self, harness: _Harness) -> None:
        tenant_a, order_id = uuid4(), uuid4()
        await harness.create_owned(tenant_a, order_id)
        async with tenant_scope(tenant_a):
            assert await harness.repo.exists(order_id) is True
            assert await harness.repo.get_version(order_id) == 1

    async def test_missing_stream_probes_as_absent_under_scope(self, harness: _Harness) -> None:
        async with tenant_scope(uuid4()):
            assert await harness.repo.exists(uuid4()) is False
            assert await harness.repo.get_version(uuid4()) == 0

    async def test_untenanted_stream_is_not_owned_under_scope(self, harness: _Harness) -> None:
        """Fail closed: a stream whose first event has no tenant is not the scope's."""
        registry = EventRegistry()
        register_event(registry=registry)(NonTenantOrderCreated)
        store = InMemoryEventStore(event_registry=registry)
        repo = TenantAwareRepository(
            AggregateRepository(store, TenantOrderAggregate), require_tenant_context=True
        )
        order_id = uuid4()
        event = NonTenantOrderCreated(
            aggregate_id=order_id, aggregate_type="Order", aggregate_version=1, customer_id=uuid4()
        )
        stream = StreamId(aggregate_id=order_id, category="Order")
        await store.append(stream, [event], ExpectedVersion.exact(0))
        async with tenant_scope(uuid4()):
            assert await repo.exists(order_id) is False
            assert await repo.get_version(order_id) == 0

    async def test_scoped_probe_without_requirement_still_checks_owner(self) -> None:
        harness = _Harness(require_tenant_context=False)
        tenant_a, tenant_b, order_id = uuid4(), uuid4(), uuid4()
        await harness.create_owned(tenant_a, order_id)
        async with tenant_scope(tenant_b):
            assert await harness.repo.exists(order_id) is False
            assert await harness.repo.get_version(order_id) == 0

    async def test_unscoped_probe_without_requirement_is_unchanged(self) -> None:
        harness = _Harness(require_tenant_context=False)
        tenant_a, order_id = uuid4(), uuid4()
        await harness.create_owned(tenant_a, order_id)
        assert await harness.repo.exists(order_id) is True
        assert await harness.repo.get_version(order_id) == 1

    async def test_unscoped_probe_with_requirement_raises(self, harness: _Harness) -> None:
        with pytest.raises(TenantContextNotSetError):
            await harness.repo.exists(uuid4())
        with pytest.raises(TenantContextNotSetError):
            await harness.repo.get_version(uuid4())


class TestForeignStreamLoadOrCreate(_ClearTenantMixin):
    async def test_load_refuses_foreign_stream_and_get_or_none_is_none(self) -> None:
        harness = _Harness()
        tenant_a, tenant_b, order_id = uuid4(), uuid4(), uuid4()
        await harness.create_owned(tenant_a, order_id)
        async with tenant_scope(tenant_b):
            with pytest.raises(AggregateNotFoundError):
                await harness.repo.load(order_id)
            assert await harness.repo.get_or_none(order_id) is None

    async def test_load_or_create_under_foreign_id_fails_at_append_and_writes_nothing(self) -> None:  # fmt: skip
        harness = _Harness()
        tenant_a, tenant_b, order_id = uuid4(), uuid4(), uuid4()
        await harness.create_owned(tenant_a, order_id)
        async with tenant_scope(tenant_a):
            before = await harness.repo.get_version(order_id)

        async with tenant_scope(tenant_b):
            with pytest.raises(OptimisticLockError):
                await harness.repo.execute(
                    order_id, lambda agg: agg.create(customer_id=uuid4()), create_if_missing=True
                )

        async with tenant_scope(tenant_a):
            assert await harness.repo.get_version(order_id) == before == 1
            owned = await harness.repo.load(order_id)
        assert owned.version == 1

    async def test_load_or_create_with_context_required_and_no_scope_raises(self) -> None:
        harness = _Harness()
        with pytest.raises(TenantContextNotSetError):
            await harness.repo.load_or_create(uuid4())


__all__ = ["TestForeignStreamLoadOrCreate", "TestForeignStreamProbes"]
