"""Ownership-aware load: a scoped load refuses streams its tenant does not own (ADR-0113, T3)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource import clear_tenant_context, tenant_scope
from eventsource.application.aggregates.snapshotting import (
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain.exceptions import AggregateNotFoundError
from tests.unit.application.aggregates.test_tenant_repository._ownership_ledger import (
    OwnershipFixture,
    seed_owned_stream,
    store_snapshot,
)


class TestForeignStreamLoad:
    @pytest.fixture(autouse=True)
    def _isolate(self) -> None:
        clear_tenant_context()
        reset_snapshot_miss_counts()

    def teardown_method(self) -> None:
        clear_tenant_context()

    @pytest.fixture
    def fx(self) -> OwnershipFixture:
        return OwnershipFixture()

    async def test_foreign_load_raises_the_same_error_as_a_missing_id(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_b):
            with pytest.raises(AggregateNotFoundError) as foreign:
                await repo.load(aggregate_id)
            missing_id = uuid4()
            with pytest.raises(AggregateNotFoundError) as missing:
                await repo.load(missing_id)

        assert type(foreign.value) is type(missing.value)
        assert foreign.value.aggregate_type == missing.value.aggregate_type == "Ledger"
        assert foreign.value.aggregate_id == aggregate_id
        assert missing.value.aggregate_id == missing_id
        assert str(foreign.value).replace(str(aggregate_id), "<id>") == str(missing.value).replace(
            str(missing_id), "<id>"
        )

    async def test_foreign_load_ignores_snapshot_tagged_with_owner_tenant(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        await store_snapshot(fx, aggregate_id, tenant_a)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_b):
            with pytest.raises(AggregateNotFoundError):
                await repo.load(aggregate_id)

        assert snapshot_miss_counts().get("tenant_mismatch") == 1

    async def test_scoped_load_refuses_mixed_stream(self, fx: OwnershipFixture) -> None:
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        # Tenant B appends to A's stream through the base repository, which has no owner check.
        ledger = await fx.base.load(aggregate_id)
        async with tenant_scope(tenant_b):
            ledger.deposit(7)
            await fx.base.save(ledger)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_a):
            with pytest.raises(AggregateNotFoundError):
                await repo.load(aggregate_id)
        async with tenant_scope(tenant_b):
            with pytest.raises(AggregateNotFoundError):
                await repo.load(aggregate_id)

    async def test_scoped_load_refuses_untenanted_stream(self, fx: OwnershipFixture) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        ledger = fx.base.create_new(aggregate_id)
        ledger.open()
        await fx.base.save(ledger)  # no scope: events carry no tenant
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_a):
            with pytest.raises(AggregateNotFoundError):
                await repo.load(aggregate_id)

    async def test_unscoped_load_of_tenanted_stream_is_unchanged(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)

        ledger = await fx.base.load(aggregate_id)

        assert ledger.state is not None and ledger.state.total == 5
