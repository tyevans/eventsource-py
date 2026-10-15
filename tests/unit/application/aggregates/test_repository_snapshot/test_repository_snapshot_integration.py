"""Tests for full cycle snapshot integration and edge cases."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
    TestAggregateV2,
)


class TestFullCycleIntegration:
    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_full_cycle_with_auto_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )
        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)
        for i in range(5):
            aggregate.do_something(f"value_{i}")
        await repo.save(aggregate)

        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version == 5

        loaded = await repo.load(aggregate_id)
        assert loaded.version == 5
        assert loaded.state.value == "value_4"
        assert loaded.state.count == 5

        loaded.do_something("value_5")
        await repo.save(loaded)

        final = await repo.load(aggregate_id)
        assert final.version == 6
        assert final.state.value == "value_5"

    @pytest.mark.asyncio
    async def test_full_cycle_with_manual_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_mode="manual",
        )
        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)
        for i in range(10):
            aggregate.do_something(f"value_{i}")
        await repo.save(aggregate)

        snapshot = await repo.create_snapshot(aggregate)
        assert snapshot.version == 10

        loaded = await repo.load(aggregate_id)
        loaded.do_something("after_snapshot")
        await repo.save(loaded)

        final = await repo.load(aggregate_id)
        assert final.version == 11
        assert final.state.value == "after_snapshot"

    @pytest.mark.asyncio
    async def test_snapshot_invalidation_on_schema_change(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo_v1 = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )
        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)
        for i in range(5):
            aggregate.do_something(f"v1_{i}")
        await repo_v1.save(aggregate)

        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot.schema_version == 1

        repo_v2 = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregateV2,
            snapshot_store=snapshot_store,
        )
        loaded = await repo_v2.load(aggregate_id)
        assert loaded.version == 5


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
