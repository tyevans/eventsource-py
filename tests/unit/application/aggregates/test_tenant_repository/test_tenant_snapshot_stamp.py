"""Snapshot writes record the owner tenant of the stream (ADR-0113, T4).

Every snapshot path (sync save, background save, manual create_snapshot) stamps
the owner, and the owner comes from the stream's events, never from the ambient
scope at write time.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource import clear_tenant_context, tenant_scope
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.snapshotting import (
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain import AggregateNotFoundError
from tests.unit.application.aggregates.test_tenant_repository._ownership_ledger import (
    Ledger,
    OwnershipFixture,
)


def _repo(
    fx: OwnershipFixture, *, snapshot_threshold: int | None = None, snapshot_mode: str = "sync"
) -> AggregateRepository[Ledger]:
    return AggregateRepository(
        fx.events,
        Ledger,
        snapshot_store=fx.snapshots,
        snapshot_threshold=snapshot_threshold,
        snapshot_mode=snapshot_mode,  # type: ignore[arg-type]
    )


async def _save_two_events(
    repo: AggregateRepository[Ledger], tenant_id: UUID, aggregate_id: UUID
) -> None:
    """Open and deposit under a scope, so both events carry the tenant."""
    scoped = TenantAwareRepository(repo, require_tenant_context=True)
    async with tenant_scope(tenant_id):
        ledger = repo.create_new(aggregate_id)
        ledger.open()
        ledger.deposit(5)
        await scoped.save(ledger)


class TestSnapshotStamp:
    @pytest.fixture(autouse=True)
    def _isolate(self) -> None:
        clear_tenant_context()
        reset_snapshot_miss_counts()

    def teardown_method(self) -> None:
        clear_tenant_context()

    @pytest.fixture
    def fx(self) -> OwnershipFixture:
        return OwnershipFixture()

    async def test_sync_save_under_scope_stamps_owner_and_owner_load_uses_it(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        repo = _repo(fx, snapshot_threshold=2)
        await _save_two_events(repo, tenant_a, aggregate_id)

        stored = await fx.snapshots.get_snapshot(aggregate_id, "Ledger")
        assert stored is not None
        assert stored.tenant_id == tenant_a

        scoped = TenantAwareRepository(repo, require_tenant_context=True)
        async with tenant_scope(tenant_a):
            ledger = await scoped.load(aggregate_id)

        assert ledger.state is not None and ledger.state.total == 5
        # The snapshot was used: the load read from the tail after it, not from version 0.
        assert fx.events.read_options[-1] is not None
        assert fx.events.read_options[-1].from_version == stored.version + 1
        assert snapshot_miss_counts() == {}

    async def test_background_save_under_scope_stamps_owner_after_scope_exits(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        repo = _repo(fx, snapshot_threshold=2, snapshot_mode="background")
        await _save_two_events(repo, tenant_a, aggregate_id)
        # The scope has exited when the background snapshot runs.
        await repo.await_pending_snapshots()

        stored = await fx.snapshots.get_snapshot(aggregate_id, "Ledger")
        assert stored is not None
        assert stored.tenant_id == tenant_a

        scoped = TenantAwareRepository(repo, require_tenant_context=True)
        async with tenant_scope(tenant_a):
            ledger = await scoped.load(aggregate_id)
        assert ledger.state is not None and ledger.state.total == 5
        assert fx.events.read_options[-1] is not None
        assert fx.events.read_options[-1].from_version == stored.version + 1

        async with tenant_scope(uuid4()):
            with pytest.raises(AggregateNotFoundError):
                await scoped.load(aggregate_id)

    async def test_manual_snapshot_stamps_stream_owner_not_ambient_scope(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        repo = _repo(fx)
        await _save_two_events(repo, tenant_a, aggregate_id)
        ledger = await repo.load(aggregate_id)

        # A foreign scope takes the snapshot. The stored tag must still be A.
        async with tenant_scope(uuid4()):
            snapshot = await repo.create_snapshot(ledger)

        assert snapshot.tenant_id == tenant_a
        stored = await fx.snapshots.get_snapshot(aggregate_id, "Ledger")
        assert stored is not None and stored.tenant_id == tenant_a

    async def test_manual_snapshot_under_owner_scope_stamps_owner(
        self, fx: OwnershipFixture
    ) -> None:
        tenant_a, aggregate_id = uuid4(), uuid4()
        repo = _repo(fx)
        await _save_two_events(repo, tenant_a, aggregate_id)

        async with tenant_scope(tenant_a):
            ledger = await repo.load(aggregate_id)
            snapshot = await repo.create_snapshot(ledger)

        assert snapshot.tenant_id == tenant_a

    async def test_unscoped_save_without_tenanted_events_stamps_none(
        self, fx: OwnershipFixture
    ) -> None:
        aggregate_id = uuid4()
        repo = _repo(fx, snapshot_threshold=2)
        ledger = repo.create_new(aggregate_id)
        ledger.open()
        ledger.deposit(5)
        await repo.save(ledger)

        stored = await fx.snapshots.get_snapshot(aggregate_id, "Ledger")
        assert stored is not None
        assert stored.tenant_id is None

        snapshot = await repo.create_snapshot(await repo.load(aggregate_id))
        assert snapshot.tenant_id is None
