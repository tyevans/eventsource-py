"""Single source of truth for the aggregate stream: `stream_for` drives every read and write.

A repository subclass that overrides `stream_for` moves every operation that
touches the aggregate's stream: load, save, exists, get_version, and the
stream read behind create_snapshot. No mixin builds its own StreamId.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource import tenant_scope
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import AggregateNotFoundError, StreamId
from tests.unit.application.aggregates.test_tenant_repository._ownership_ledger import (
    Ledger,
    OwnershipFixture,
)

SHADOW_CATEGORY = "LedgerShadow"


class _ShadowStreamRepository(AggregateRepository[Ledger]):
    """Keeps every Ledger in a stream category other than its aggregate type."""

    def stream_for(self, aggregate_id: UUID) -> StreamId:
        return StreamId(aggregate_id=aggregate_id, category=SHADOW_CATEGORY)


@pytest.fixture
def fx() -> OwnershipFixture:
    return OwnershipFixture()


def _opened_ledger(repo: AggregateRepository[Ledger], aggregate_id: UUID) -> Ledger:
    ledger = repo.create_new(aggregate_id)
    ledger.open()
    ledger.deposit(5)
    return ledger


async def _read(fx: OwnershipFixture, stream: StreamId) -> list[object]:
    return [env.event async for env in fx.events.read_stream(stream)]


async def test_save_appends_only_to_the_overridden_stream(fx: OwnershipFixture) -> None:
    repo = _ShadowStreamRepository(fx.events, Ledger, snapshot_store=fx.snapshots)
    aggregate_id = uuid4()

    await repo.save(_opened_ledger(repo, aggregate_id))

    shadow = StreamId(aggregate_id=aggregate_id, category=SHADOW_CATEGORY)
    default = StreamId(aggregate_id=aggregate_id, category="Ledger")
    assert len(await _read(fx, shadow)) == 2
    assert await _read(fx, default) == []


async def test_load_exists_and_version_read_the_overridden_stream(fx: OwnershipFixture) -> None:
    repo = _ShadowStreamRepository(fx.events, Ledger, snapshot_store=fx.snapshots)
    aggregate_id = uuid4()
    await repo.save(_opened_ledger(repo, aggregate_id))

    loaded = await repo.load(aggregate_id)

    assert loaded.state is not None and loaded.state.total == 5
    assert await repo.exists(aggregate_id) is True
    assert await repo.get_version(aggregate_id) == 2

    # Control: a plain repository on the same store looks at the default stream,
    # which holds nothing, so the same aggregate is missing there.
    plain = AggregateRepository(fx.events, Ledger, snapshot_store=fx.snapshots)
    assert await plain.exists(aggregate_id) is False
    with pytest.raises(AggregateNotFoundError):
        await plain.load(aggregate_id)


async def test_create_snapshot_reads_the_overridden_stream_for_its_owner(
    fx: OwnershipFixture,
) -> None:
    # Owner is taken from the events of the stream. Events written under a
    # tenant scope carry that tenant, so a snapshot that read the wrong (empty)
    # stream would be stored untenanted.
    repo = _ShadowStreamRepository(fx.events, Ledger, snapshot_store=fx.snapshots)
    tenant, aggregate_id = uuid4(), uuid4()
    async with tenant_scope(tenant):
        await repo.save(_opened_ledger(repo, aggregate_id))

    loaded = await repo.load(aggregate_id)
    snapshot = await repo.create_snapshot(loaded)

    assert snapshot.tenant_id == tenant
    stored = await fx.snapshots.get_snapshot(aggregate_id, "Ledger")
    assert stored is not None and stored.tenant_id == tenant
