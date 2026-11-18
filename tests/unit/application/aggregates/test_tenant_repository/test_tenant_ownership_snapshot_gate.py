"""Snapshot gate for ownership-aware load: only a snapshot tagged with the owner is used (ADR-0113, T3)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource import clear_tenant_context, tenant_scope
from eventsource.application.aggregates.snapshotting import (
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from tests.unit.application.aggregates.test_tenant_repository._ownership_ledger import (
    OwnershipFixture,
    seed_owned_stream,
    store_snapshot,
)


class TestSnapshotGate:
    @pytest.fixture(autouse=True)
    def _isolate(self) -> None:
        clear_tenant_context()
        reset_snapshot_miss_counts()

    def teardown_method(self) -> None:
        clear_tenant_context()

    @pytest.fixture
    def fx(self) -> OwnershipFixture:
        return OwnershipFixture()

    async def test_owner_load_uses_snapshot_tagged_with_its_tenant(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        await store_snapshot(fx, aggregate_id, tenant_a)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_a):
            ledger = await repo.load(aggregate_id)

        assert ledger.version == 2
        assert ledger.state is not None and ledger.state.total == 5
        # The load read only the tail after the snapshot (version 1), so from_version is 2.
        assert fx.events.read_options[-1] is not None
        assert fx.events.read_options[-1].from_version == 2
        assert snapshot_miss_counts() == {}

    async def test_owner_load_ignores_snapshot_without_tenant_and_replays(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        await store_snapshot(fx, aggregate_id, None)
        repo = TenantAwareRepository(fx.base, require_tenant_context=True)

        async with tenant_scope(tenant_a):
            ledger = await repo.load(aggregate_id)

        assert ledger.state is not None and ledger.state.total == 5
        assert fx.events.read_options[-1] is None  # full replay from version 0
        assert snapshot_miss_counts().get("tenant_mismatch") == 1

    async def test_unscoped_load_still_uses_untenanted_snapshot(self, fx: OwnershipFixture) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        await seed_owned_stream(fx, tenant_a, aggregate_id)
        await store_snapshot(fx, aggregate_id, None)

        ledger = await fx.base.load(aggregate_id)

        assert ledger.version == 2
        assert fx.events.read_options[-1] is not None
        assert fx.events.read_options[-1].from_version == 2
        assert snapshot_miss_counts() == {}
