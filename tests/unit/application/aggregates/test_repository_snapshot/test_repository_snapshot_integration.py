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
    """Test complete cycle: save events -> create snapshot -> load from snapshot."""

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
        """Complete cycle with automatic snapshot creation."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)

        # Apply 5 events (triggers snapshot)
        for i in range(5):
            aggregate.do_something(f"value_{i}")

        await repo.save(aggregate)

        # Verify snapshot exists
        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version == 5

        # Load from snapshot
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 5
        assert loaded.state.value == "value_4"
        assert loaded.state.count == 5

        # Apply more events
        loaded.do_something("value_5")
        await repo.save(loaded)

        # Load again (should use snapshot + 1 event)
        final = await repo.load(aggregate_id)
        assert final.version == 6
        assert final.state.value == "value_5"

    @pytest.mark.asyncio
    async def test_full_cycle_with_manual_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Complete cycle with manual snapshot creation."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_mode="manual",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)

        # Apply events and save
        for i in range(10):
            aggregate.do_something(f"value_{i}")
        await repo.save(aggregate)

        # Manually create snapshot
        snapshot = await repo.create_snapshot(aggregate)
        assert snapshot.version == 10

        # Apply more events
        loaded = await repo.load(aggregate_id)
        loaded.do_something("after_snapshot")
        await repo.save(loaded)

        # Load again (should use snapshot + new event)
        final = await repo.load(aggregate_id)
        assert final.version == 11
        assert final.state.value == "after_snapshot"

    @pytest.mark.asyncio
    async def test_snapshot_invalidation_on_schema_change(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Snapshot is invalidated when schema version changes."""
        # Create repo with schema_version=1
        repo_v1 = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,  # schema_version=1
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)
        for i in range(5):
            aggregate.do_something(f"v1_{i}")
        await repo_v1.save(aggregate)

        # Snapshot exists with schema_version=1
        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot.schema_version == 1

        # Create repo with schema_version=2 (simulating schema upgrade)
        repo_v2 = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregateV2,  # schema_version=2
            snapshot_store=snapshot_store,
        )

        # Load should fall back to full replay
        loaded = await repo_v2.load(aggregate_id)
        assert loaded.version == 5  # Full replay from events


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

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
        """Multiple concurrent loads should work with snapshot."""
        aggregate_id = uuid4()

        # Create snapshot
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

        # Load concurrently
        results = await asyncio.gather(
            repo.load(aggregate_id),
            repo.load(aggregate_id),
            repo.load(aggregate_id),
        )

        for loaded in results:
            assert loaded.version == 10
            assert loaded.state.value == "concurrent"

    @pytest.mark.asyncio
    async def test_save_with_no_changes_and_snapshot_store(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Saving with no changes should not create snapshot."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
        )

        aggregate = TestAggregate(uuid4())
        # No changes, just save

        await repo.save(aggregate)

        assert snapshot_store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_multiple_aggregates_independent_snapshots(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Each aggregate should have independent snapshots."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=2,
            snapshot_mode="sync",
        )

        id1 = uuid4()
        id2 = uuid4()

        # Create first aggregate with 2 events (triggers snapshot)
        agg1 = TestAggregate(id1)
        agg1.do_something("agg1_v1")
        agg1.do_something("agg1_v2")
        await repo.save(agg1)

        # Create second aggregate with 2 events (triggers snapshot)
        agg2 = TestAggregate(id2)
        agg2.do_something("agg2_v1")
        agg2.do_something("agg2_v2")
        await repo.save(agg2)

        # Should have 2 independent snapshots
        assert snapshot_store.snapshot_count == 2

        snap1 = await snapshot_store.get_snapshot(id1, "Test")
        snap2 = await snapshot_store.get_snapshot(id2, "Test")

        assert snap1.state["value"] == "agg1_v2"
        assert snap2.state["value"] == "agg2_v2"
