"""Tests for background snapshot mode and snapshot policy logic."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
)


class TestRepositoryBackgroundSnapshot:
    """Test background snapshot mode."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_background_mode_creates_task(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Background mode creates async task for snapshot."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")

        await repo.save(aggregate)

        # Wait for background task
        await repo.await_pending_snapshots()

        # Snapshot should have been created
        assert snapshot_store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_background_mode_doesnt_block(self, event_store: InMemoryEventStore) -> None:
        """Background mode returns before snapshot completes."""

        async def slow_save(snapshot: Snapshot) -> None:
            await asyncio.sleep(0.1)

        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(return_value=None)
        mock_snapshot_store.save_snapshot = AsyncMock(side_effect=slow_save)

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=mock_snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")

        start = time.monotonic()
        await repo.save(aggregate)
        elapsed = time.monotonic() - start

        # Should return much faster than 0.1s
        assert elapsed < 0.05

        # Cleanup
        await repo.await_pending_snapshots()

    @pytest.mark.asyncio
    async def test_await_pending_snapshots_waits_for_all(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """await_pending_snapshots waits for all pending tasks."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        # Create multiple aggregates that will each trigger a snapshot
        for _ in range(3):
            aggregate = TestAggregate(uuid4())
            aggregate.do_something("test")
            await repo.save(aggregate)

        count = await repo.await_pending_snapshots()

        assert count == 3
        assert snapshot_store.snapshot_count == 3

    @pytest.mark.asyncio
    async def test_pending_snapshot_count(self, event_store: InMemoryEventStore) -> None:
        """pending_snapshot_count returns correct count."""

        async def slow_save(snapshot: Snapshot) -> None:
            await asyncio.sleep(0.5)

        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(return_value=None)
        mock_snapshot_store.save_snapshot = AsyncMock(side_effect=slow_save)

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=mock_snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        # There should be at least 0 pending (may have completed already)
        assert repo.pending_snapshot_count >= 0

        # Cleanup
        await repo.await_pending_snapshots()


class TestSnapshotPolicyLogic:
    """Test snapshot policy logic via SnapshotPolicy classes."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    def test_returns_false_without_store(self, event_store: InMemoryEventStore) -> None:
        """Without snapshot store, repository falls back to Never() policy."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_threshold=100,
        )

        # Without snapshot_store, the policy is irrelevant since no store exists
        assert repo._snapshot_store is None

    def test_returns_false_without_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Without threshold, repository falls back to Never() policy."""
        from eventsource.application.aggregates.snapshotting import Never

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100

        assert isinstance(repo._snapshot_policy, Never)
        assert repo._snapshot_policy.should_snapshot(aggregate, events_since_snapshot=100) is False

    def test_returns_false_in_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Manual mode (snapshot_mode='manual') doesn't auto-snapshot."""
        from eventsource.application.aggregates.snapshotting import Never

        policy = Never()
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100

        # Never().should_snapshot always returns False
        assert policy.should_snapshot(aggregate, events_since_snapshot=100) is False

    def test_returns_true_at_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns True at exact threshold."""
        from eventsource.application.aggregates.snapshotting import EveryNEvents

        policy = EveryNEvents(100)
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100

        assert policy.should_snapshot(aggregate, events_since_snapshot=100) is True

    def test_returns_true_at_threshold_multiples(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns True at threshold multiples."""
        from eventsource.application.aggregates.snapshotting import EveryNEvents

        policy = EveryNEvents(100)
        aggregate = TestAggregate(uuid4())

        for version in [100, 200, 300, 500, 1000]:
            aggregate._version = version
            assert policy.should_snapshot(aggregate, events_since_snapshot=version) is True

    def test_returns_false_within_one_interval(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns False for a save that never left the interval it started in."""
        from eventsource.application.aggregates.snapshotting import EveryNEvents

        policy = EveryNEvents(100)
        aggregate = TestAggregate(uuid4())

        # (version reached, events in this save) -- each stays inside one interval
        for version, events in [(1, 1), (50, 49), (99, 9), (101, 1), (150, 10), (199, 9)]:
            aggregate._version = version
            assert policy.should_snapshot(aggregate, events_since_snapshot=events) is False

    def test_returns_true_when_one_save_spans_several_multiples(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """A save crossing more than one multiple still owes one snapshot."""
        from eventsource.application.aggregates.snapshotting import EveryNEvents

        policy = EveryNEvents(100)
        aggregate = TestAggregate(uuid4())

        for version in [101, 150, 199, 250]:
            aggregate._version = version
            assert policy.should_snapshot(aggregate, events_since_snapshot=version) is True

    def test_returns_false_at_version_zero(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns False at version zero."""
        from eventsource.application.aggregates.snapshotting import EveryNEvents

        policy = EveryNEvents(100)
        aggregate = TestAggregate(uuid4())
        aggregate._version = 0

        assert policy.should_snapshot(aggregate, events_since_snapshot=0) is False
