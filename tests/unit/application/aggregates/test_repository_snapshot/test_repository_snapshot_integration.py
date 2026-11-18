"""Tests for full cycle snapshot integration (save, load, and schema invalidation)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
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
