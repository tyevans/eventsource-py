"""Tests for automatic and manual snapshot creation."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
)


class TestRepositoryAutoSnapshot:
    """Test automatic snapshot creation at threshold."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_auto_snapshot_at_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Snapshot created when version reaches threshold."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=3,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()

        # Create and save events until threshold
        for i in range(3):
            if i == 0:
                aggregate = TestAggregate(aggregate_id)
            else:
                aggregate = await repo.load(aggregate_id)
            aggregate.do_something(f"event_{i}")
            await repo.save(aggregate)

        # Should have created snapshot at version 3
        assert snapshot_store.snapshot_count == 1

        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version == 3

    @pytest.mark.asyncio
    async def test_no_snapshot_below_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """No snapshot created below threshold."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=10,
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        assert snapshot_store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_no_auto_snapshot_in_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """No automatic snapshot in manual mode."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=1,  # Would trigger immediately
            snapshot_mode="manual",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        assert snapshot_store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_snapshot_at_multiples_of_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Snapshots created at threshold multiples (e.g., 100, 200, 300)."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)

        # Apply 10 events (should trigger snapshot at 5 and 10)
        for _ in range(10):
            aggregate.increment()

        await repo.save(aggregate)

        # Should have snapshot at version 10 (latest threshold multiple)
        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version == 10

    @pytest.mark.asyncio
    async def test_snapshots_happen_when_saves_never_land_on_a_multiple(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """A stream saving a constant number of events per save must still snapshot.

        Every other test here saves a batch whose size divides the threshold,
        so the version lands on a multiple. Real aggregates emit several events
        per command: with three events per save and a threshold of five, the
        versions are 3, 6, 9, 12 -- no multiple of five among them, and under a
        land-on-the-boundary rule this stream snapshots exactly never.
        """
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()
        for save in range(4):
            aggregate = TestAggregate(aggregate_id) if save == 0 else await repo.load(aggregate_id)
            for _ in range(3):
                aggregate.increment()
            await repo.save(aggregate)

        assert snapshot_store.snapshot_count > 0, (
            "twelve events at a threshold of five produced no snapshot"
        )
        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version >= 5

    @pytest.mark.asyncio
    async def test_snapshot_failure_doesnt_fail_save(self, event_store: InMemoryEventStore) -> None:
        """Snapshot failure doesn't fail the save operation."""
        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(return_value=None)
        mock_snapshot_store.save_snapshot = AsyncMock(side_effect=Exception("Snapshot error"))

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=mock_snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="sync",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")

        # Should not raise
        await repo.save(aggregate)

        # Events should be committed
        assert not aggregate.has_uncommitted_events


class TestRepositoryManualSnapshot:
    """Test manual snapshot creation."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_create_snapshot_returns_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """create_snapshot returns the created Snapshot object."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()  # Simulate already saved

        snapshot = await repo.create_snapshot(aggregate)

        assert snapshot is not None
        assert snapshot.aggregate_id == aggregate.aggregate_id
        assert snapshot.aggregate_type == "Test"
        assert snapshot.version == 1
        assert snapshot.schema_version == 1
        assert snapshot.state["value"] == "test"

    @pytest.mark.asyncio
    async def test_create_snapshot_raises_without_store(
        self, event_store: InMemoryEventStore
    ) -> None:
        """create_snapshot raises RuntimeError without snapshot store."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
        )

        aggregate = TestAggregate(uuid4())

        with pytest.raises(RuntimeError, match="snapshot_store"):
            await repo.create_snapshot(aggregate)

    @pytest.mark.asyncio
    async def test_create_snapshot_works_in_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Manual snapshot works even in manual mode."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_mode="manual",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()

        await repo.create_snapshot(aggregate)

        assert snapshot_store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_create_snapshot_overwrites_existing(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Creating snapshot overwrites existing snapshot (upsert)."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("first")
        aggregate._uncommitted_events.clear()

        await repo.create_snapshot(aggregate)

        # Apply more events
        aggregate.do_something("second")
        aggregate._uncommitted_events.clear()

        await repo.create_snapshot(aggregate)

        # Should still have only 1 snapshot
        assert snapshot_store.snapshot_count == 1

        # Snapshot should be at newer version
        snapshot = await snapshot_store.get_snapshot(aggregate.aggregate_id, "Test")
        assert snapshot.version == 2
        assert snapshot.state["value"] == "second"
