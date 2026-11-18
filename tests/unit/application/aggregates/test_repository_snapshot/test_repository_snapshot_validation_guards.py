"""Identity and empty-stream checks for snapshots during load (TASK-0469).

Companion to ``test_repository_snapshot_validation``, which covers the
snapshot-ahead-of-stream check. A snapshot for another aggregate, or any
snapshot over an empty stream, never produces a loaded aggregate.
"""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.snapshotting import (
    SnapshotMissReason,
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)
from eventsource.domain.exceptions import AggregateNotFoundError
from tests.unit.application.aggregates.test_repository_snapshot._validation_support import (
    StoreReturning,
    append_events,
    build_repo,
    make_snapshot,
    new_event_store,
)


@pytest.fixture(autouse=True)
def _reset_miss_counts() -> None:
    reset_snapshot_miss_counts()


@pytest.fixture
def event_store() -> InMemoryEventStore:
    return new_event_store()


class TestSnapshotIdentityMismatch:
    @pytest.mark.asyncio
    async def test_mismatched_aggregate_type_is_a_miss(
        self, event_store: InMemoryEventStore
    ) -> None:
        aggregate_id = uuid4()
        await append_events(event_store, aggregate_id, 2)
        row = make_snapshot(aggregate_id, 1, aggregate_type="Other")
        snapshots = StoreReturning(row)

        loaded = await build_repo(event_store, snapshots).load(aggregate_id)

        assert loaded.version == 2
        assert loaded.state is not None
        assert loaded.state.value == "event_1"
        assert snapshot_miss_counts()[SnapshotMissReason.IDENTITY_MISMATCH] == 1

    @pytest.mark.asyncio
    async def test_mismatched_aggregate_id_is_a_miss(self, event_store: InMemoryEventStore) -> None:
        aggregate_id = uuid4()
        await append_events(event_store, aggregate_id, 2)
        row = make_snapshot(uuid4(), 1)
        snapshots = StoreReturning(row)

        loaded = await build_repo(event_store, snapshots).load(aggregate_id)

        assert loaded.version == 2
        assert loaded.state is not None
        assert loaded.state.value == "event_1"
        assert snapshot_miss_counts()[SnapshotMissReason.IDENTITY_MISMATCH] == 1

    @pytest.mark.asyncio
    async def test_matching_identity_is_not_a_miss(self, event_store: InMemoryEventStore) -> None:
        aggregate_id = uuid4()
        await append_events(event_store, aggregate_id, 2)
        row = replace(make_snapshot(aggregate_id, 1), state={"value": "x", "count": 1, "items": []})
        snapshots = StoreReturning(row)

        loaded = await build_repo(event_store, snapshots).load(aggregate_id)

        assert (loaded.version, loaded.state.value) == (2, "event_1")  # type: ignore[union-attr]
        assert SnapshotMissReason.IDENTITY_MISMATCH not in snapshot_miss_counts()


class TestNoEventsInStream:
    @pytest.mark.asyncio
    async def test_snapshot_with_zero_events_raises_not_found(
        self, event_store: InMemoryEventStore
    ) -> None:
        aggregate_id = uuid4()
        snapshots = InMemorySnapshotStore(enable_tracing=False)
        await snapshots.save_snapshot(make_snapshot(aggregate_id, 5))

        with pytest.raises(AggregateNotFoundError):
            await build_repo(event_store, snapshots).load(aggregate_id)

    @pytest.mark.asyncio
    async def test_snapshot_at_version_zero_with_no_events_raises_not_found(
        self, event_store: InMemoryEventStore
    ) -> None:
        aggregate_id = uuid4()
        snapshots = InMemorySnapshotStore(enable_tracing=False)
        await snapshots.save_snapshot(make_snapshot(aggregate_id, 0))

        with pytest.raises(AggregateNotFoundError):
            await build_repo(event_store, snapshots).load(aggregate_id)

    @pytest.mark.asyncio
    async def test_get_or_none_returns_none_for_phantom_snapshot(
        self, event_store: InMemoryEventStore
    ) -> None:
        aggregate_id = uuid4()
        snapshots = InMemorySnapshotStore(enable_tracing=False)
        await snapshots.save_snapshot(make_snapshot(aggregate_id, 5))

        assert await build_repo(event_store, snapshots).get_or_none(aggregate_id) is None
