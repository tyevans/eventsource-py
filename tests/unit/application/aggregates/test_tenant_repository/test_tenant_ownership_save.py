"""Write guard: a scoped save refuses to append to a stream another tenant owns (ADR-0113, T6)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource import clear_tenant_context, tenant_scope
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain.exceptions import AggregateNotFoundError, OptimisticLockError
from tests.unit.application.aggregates.test_tenant_repository._ownership_ledger import (
    Ledger,
    OwnershipFixture,
    seed_owned_stream,
)


class TestForeignSaveGuard:
    @pytest.fixture(autouse=True)
    def _isolate(self) -> None:
        clear_tenant_context()

    def teardown_method(self) -> None:
        clear_tenant_context()

    @pytest.fixture
    def fx(self) -> OwnershipFixture:
        return OwnershipFixture()

    async def test_forged_save_of_foreign_stream_is_refused_and_appends_nothing(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        version_before = await fx.base.get_version(aggregate_id)
        # Tenant B loads A's aggregate while unscoped, then saves it under its own scope.
        forged = await fx.base.load(aggregate_id)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_b):
            forged.deposit(7)
            with pytest.raises(AggregateNotFoundError) as refused:
                await repo.save(forged)

        assert refused.value.aggregate_id == aggregate_id
        assert refused.value.aggregate_type == "Ledger"
        assert await fx.base.get_version(aggregate_id) == version_before
        owned = await fx.base.load(aggregate_id)
        assert owned.state is not None and owned.state.total == 5

    async def test_foreign_save_refused_even_without_validate_on_save(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        forged = await fx.base.load(aggregate_id)
        repo = TenantAwareRepository(fx.base, validate_on_save=False)

        async with tenant_scope(tenant_b):
            forged.deposit(7)
            with pytest.raises(AggregateNotFoundError):
                await repo.save(forged)

        assert await fx.base.get_version(aggregate_id) == 2

    async def test_foreign_save_all_is_refused(self, fx: OwnershipFixture) -> None:
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        forged = await fx.base.load(aggregate_id)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_b):
            forged.deposit(7)
            with pytest.raises(AggregateNotFoundError):
                await repo.save_all(forged)

        assert await fx.base.get_version(aggregate_id) == 2

    async def test_new_aggregate_on_foreign_id_hits_store_conflict(
        self, fx: OwnershipFixture
    ) -> None:
        """Version zero skips the owner read; the store's concurrency check rejects the append."""
        tenant_a, tenant_b, aggregate_id = uuid4(), uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_b):
            fresh: Ledger = repo.create_new(aggregate_id)
            fresh.open()
            with pytest.raises(OptimisticLockError):
                await repo.save(fresh)

        assert await fx.base.get_version(aggregate_id) == 2

    async def test_owner_save_succeeds(self, fx: OwnershipFixture) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_a):
            ledger = await repo.load(aggregate_id)
            ledger.deposit(1)
            await repo.save(ledger)

        assert await fx.base.get_version(aggregate_id) == 3

    async def test_unscoped_save_is_unchanged(self, fx: OwnershipFixture) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        repo = TenantAwareRepository(fx.base, validate_on_save=False)

        ledger = await fx.base.load(aggregate_id)
        ledger.deposit(1)
        await repo.save(ledger)

        assert await fx.base.get_version(aggregate_id) == 3


__all__ = ["TestForeignSaveGuard"]
