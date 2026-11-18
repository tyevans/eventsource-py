"""Edge cases for snapshot loading: concurrency, no-op saves, and isolation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
    TestEvent,
)


class TestEdgeCases:
    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_concurrent_loads_with_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate_id = uuid4()
        events = [
            TestEvent(
                aggregate_id=aggregate_id,
                aggregate_type="Test",
                aggregate_version=i + 1,
                value=f"e{i}",
            )  # fmt: skip
            for i in range(10)
        ]
        await event_store.append(
            StreamId(aggregate_id=aggregate_id, category="Test"), events, ExpectedVersion.exact(0)
        )
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            version=10,
            state={"value": "concurrent", "count": 10, "items": []},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        results = await asyncio.gather(
            repo.load(aggregate_id), repo.load(aggregate_id), repo.load(aggregate_id)
        )
        for loaded in results:
            assert loaded.version == 10
            assert loaded.state.value == "concurrent"

    @pytest.mark.asyncio
    async def test_save_with_no_changes_and_snapshot_store(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
        )
        await repo.save(TestAggregate(uuid4()))
        assert snapshot_store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_multiple_aggregates_independent_snapshots(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=2,
            snapshot_mode="sync",
        )
        id1, id2 = uuid4(), uuid4()
        agg1 = TestAggregate(id1)
        agg1.do_something("agg1_v1")
        agg1.do_something("agg1_v2")
        await repo.save(agg1)

        agg2 = TestAggregate(id2)
        agg2.do_something("agg2_v1")
        agg2.do_something("agg2_v2")
        await repo.save(agg2)

        assert snapshot_store.snapshot_count == 2
        snap1 = await snapshot_store.get_snapshot(id1, "Test")
        snap2 = await snapshot_store.get_snapshot(id2, "Test")
        assert snap1.state["value"] == "agg1_v2"
        assert snap2.state["value"] == "agg2_v2"
