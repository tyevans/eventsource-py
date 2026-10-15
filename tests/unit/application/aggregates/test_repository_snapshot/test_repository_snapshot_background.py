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
from eventsource.application.aggregates.snapshotting import EveryNEvents, Never
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
)


class TestRepositoryBackgroundSnapshot:
    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    def _make_repo(self, event_store: InMemoryEventStore, snapshot_store) -> AggregateRepository:
        return AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

    @pytest.mark.asyncio
    async def test_background_mode_creates_task(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = self._make_repo(event_store, snapshot_store)
        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)
        await repo.await_pending_snapshots()
        assert snapshot_store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_background_mode_doesnt_block(self, event_store: InMemoryEventStore) -> None:
        async def slow_save(snapshot: Snapshot) -> None:
            await asyncio.sleep(0.1)

        mock_store = MagicMock(
            get_snapshot=AsyncMock(return_value=None),
            save_snapshot=AsyncMock(side_effect=slow_save),
        )
        repo = self._make_repo(event_store, mock_store)
        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        start = time.monotonic()
        await repo.save(aggregate)
        assert time.monotonic() - start < 0.05
        await repo.await_pending_snapshots()

    @pytest.mark.asyncio
    async def test_await_pending_snapshots_waits_for_all(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = self._make_repo(event_store, snapshot_store)
        for _ in range(3):
            aggregate = TestAggregate(uuid4())
            aggregate.do_something("test")
            await repo.save(aggregate)
        assert await repo.await_pending_snapshots() == 3
        assert snapshot_store.snapshot_count == 3

    @pytest.mark.asyncio
    async def test_pending_snapshot_count(self, event_store: InMemoryEventStore) -> None:
        async def slow_save(snapshot: Snapshot) -> None:
            await asyncio.sleep(0.5)

        mock_store = MagicMock(
            get_snapshot=AsyncMock(return_value=None),
            save_snapshot=AsyncMock(side_effect=slow_save),
        )
        repo = self._make_repo(event_store, mock_store)
        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)
        assert repo.pending_snapshot_count >= 0
        await repo.await_pending_snapshots()


class TestSnapshotPolicyLogic:
    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    def test_returns_false_without_store(self, event_store: InMemoryEventStore) -> None:
        repo = AggregateRepository(
            event_store=event_store, aggregate_factory=TestAggregate, snapshot_threshold=100
        )
        assert repo._snapshot_store is None

    def test_returns_false_without_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store, aggregate_factory=TestAggregate, snapshot_store=snapshot_store
        )
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100
        assert isinstance(repo._snapshot_policy, Never)
        assert repo._snapshot_policy.should_snapshot(aggregate, events_since_snapshot=100) is False

    def test_returns_false_in_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100
        assert Never().should_snapshot(aggregate, events_since_snapshot=100) is False

    def test_returns_true_at_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100
        assert EveryNEvents(100).should_snapshot(aggregate, events_since_snapshot=100) is True

    def test_returns_true_at_threshold_multiples(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        policy, aggregate = EveryNEvents(100), TestAggregate(uuid4())
        for version in [100, 200, 300, 500, 1000]:
            aggregate._version = version
            assert policy.should_snapshot(aggregate, events_since_snapshot=version) is True

    def test_returns_false_within_one_interval(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        policy, aggregate = EveryNEvents(100), TestAggregate(uuid4())
        for version, events in [(1, 1), (50, 49), (99, 9), (101, 1), (150, 10), (199, 9)]:
            aggregate._version = version
            assert policy.should_snapshot(aggregate, events_since_snapshot=events) is False

    def test_returns_true_when_one_save_spans_several_multiples(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        policy, aggregate = EveryNEvents(100), TestAggregate(uuid4())
        for version in [101, 150, 199, 250]:
            aggregate._version = version
            assert policy.should_snapshot(aggregate, events_since_snapshot=version) is True

    def test_returns_false_at_version_zero(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate = TestAggregate(uuid4())
        aggregate._version = 0
        assert EveryNEvents(100).should_snapshot(aggregate, events_since_snapshot=0) is False
