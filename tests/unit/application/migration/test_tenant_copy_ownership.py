"""Tenant copy ownership: a migrated stream keeps its owner and the target read guard holds (ADR-0113, T10)."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource import clear_tenant_context, tenant_scope
from eventsource.application.aggregates.snapshotting import (
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.domain import StreamId
from eventsource.domain.exceptions import AggregateNotFoundError
from eventsource.ports import FeedReadOptions
from eventsource.ports.migration.models import Migration
from tests.unit.application.aggregates.test_tenant_repository._ownership_ledger import (
    Ledger,
    OwnershipFixture,
    seed_owned_stream,
    store_snapshot,
)
from tests.unit.application.migration.test_bulk_copier._fixtures import create_migration

LEDGER = "Ledger"


async def _copy_tenant(source: OwnershipFixture, target: OwnershipFixture, tenant_id) -> None:
    """Run the public bulk copy path for one tenant, from source store to target store."""
    migration: Migration = create_migration(tenant_id=tenant_id, batch_size=10)
    copier = BulkCopier(source.events, target.events, AsyncMock(), enable_tracing=False)
    async for _ in copier.run(migration):
        pass


class TestCopiedStreamOwnership:
    @pytest.fixture(autouse=True)
    def _isolate(self) -> None:
        clear_tenant_context()
        reset_snapshot_miss_counts()

    def teardown_method(self) -> None:
        clear_tenant_context()

    @pytest.fixture
    async def copied(self) -> tuple[OwnershipFixture, OwnershipFixture, object, object]:
        """Seed tenant A's stream and a snapshot on the source, then copy A into a fresh target."""
        source, target = OwnershipFixture(), OwnershipFixture()
        tenant_a, aggregate_id = uuid4(), uuid4()
        await seed_owned_stream(source, tenant_a, aggregate_id)
        await store_snapshot(source, aggregate_id, tenant_a)
        await _copy_tenant(source, target, tenant_a)
        return source, target, tenant_a, aggregate_id

    async def test_owner_loads_copied_aggregate_with_matching_state(self, copied) -> None:
        source, target, tenant_a, aggregate_id = copied
        repo = TenantAwareRepository(target.base, require_tenant_context=True)

        async with tenant_scope(tenant_a):
            ledger = await repo.load(aggregate_id)
            assert ledger.state is not None and ledger.state.total == 5
            assert await repo.exists(aggregate_id) is True
            assert await repo.get_version(aggregate_id) == 2

    async def test_copy_keeps_tenant_ids_on_target(self, copied) -> None:
        _, target, tenant_a, _ = copied
        tenant_b = uuid4()

        owned = [e async for e in target.events.read_all(None, FeedReadOptions(tenant_id=tenant_a))]
        foreign = [
            e async for e in target.events.read_all(None, FeedReadOptions(tenant_id=tenant_b))
        ]

        assert len(owned) == 2
        assert foreign == []

    async def test_foreign_tenant_gets_not_found_on_copied_aggregate(self, copied) -> None:
        _, target, _, aggregate_id = copied
        repo = TenantAwareRepository(target.base, require_tenant_context=True)

        async with tenant_scope(uuid4()):
            with pytest.raises(AggregateNotFoundError):
                await repo.load(aggregate_id)
            assert await repo.exists(aggregate_id) is False
            assert await repo.get_version(aggregate_id) == 0

    async def test_copy_does_not_carry_snapshots_so_owner_replays(self, copied) -> None:
        _, target, tenant_a, aggregate_id = copied
        repo = TenantAwareRepository(target.base, require_tenant_context=True)

        assert await target.snapshots.get_snapshot(aggregate_id, LEDGER) is None

        async with tenant_scope(tenant_a):
            ledger = await repo.load(aggregate_id)

        assert ledger.state is not None and ledger.state.total == 5
        assert snapshot_miss_counts().get("missing", 0) >= 1

    async def test_forged_save_on_copied_stream_is_refused_and_stream_unchanged(
        self, copied
    ) -> None:
        _, target, tenant_a, aggregate_id = copied
        repo = TenantAwareRepository(target.base, require_tenant_context=True)
        stream = StreamId(aggregate_id=aggregate_id, category=LEDGER)

        forged: Ledger = await target.base.load(aggregate_id)
        tenant_b = uuid4()

        async with tenant_scope(tenant_b):
            forged.deposit(7)
            with pytest.raises(AggregateNotFoundError):
                await repo.save(forged)

        assert await target.events.get_stream_version(stream) == 2
        async with tenant_scope(tenant_a):
            ledger = await repo.load(aggregate_id)
        assert ledger.state is not None and ledger.state.total == 5
