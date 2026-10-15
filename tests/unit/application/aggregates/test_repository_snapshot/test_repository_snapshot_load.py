"""Tests for repository constructor and snapshot-aware load functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.domain.exceptions import AggregateNotFoundError
from eventsource.ports import ExpectedVersion
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
    TestEvent,
)


def _make_snapshot(
    aggregate_id: UUID, state: dict[str, Any], version: int = 5, schema_version: int = 1
) -> Snapshot:
    return Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Test",
        version=version,
        state=state,
        schema_version=schema_version,
        created_at=datetime.now(UTC),
    )


def _make_events(aggregate_id: UUID, count: int) -> list[TestEvent]:
    return [
        TestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            aggregate_version=i + 1,
            value=f"event_{i}",
        )
        for i in range(count)
    ]


class TestRepositoryConstructor:
    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    def test_constructor_without_snapshot_params(self, event_store: InMemoryEventStore) -> None:
        repo = AggregateRepository(event_store=event_store, aggregate_factory=TestAggregate)
        assert repo.snapshot_store is None
        assert repo.snapshot_threshold is None
        assert repo.snapshot_mode == "sync"
        assert repo.has_snapshot_support is False

    def test_constructor_with_snapshot_store(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        assert repo.snapshot_store is snapshot_store
        assert repo.has_snapshot_support is True

    def test_constructor_with_all_params(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=100,
            snapshot_mode="background",
        )
        assert repo.snapshot_threshold == 100
        assert repo.snapshot_mode == "background"

    def test_constructor_with_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=10,
            snapshot_mode="manual",
        )
        assert repo.snapshot_mode == "manual"


class TestRepositorySnapshotLoad:
    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_load_without_snapshot_store(self, event_store: InMemoryEventStore) -> None:
        repo = AggregateRepository(event_store=event_store, aggregate_factory=TestAggregate)
        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test-value")
        await repo.save(aggregate)
        loaded = await repo.load(aggregate.aggregate_id)
        assert loaded.version == 1
        assert loaded.state.value == "test-value"

    @pytest.mark.asyncio
    async def test_load_uses_snapshot_when_available(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Test")
        await event_store.append(stream, _make_events(aggregate_id, 5), ExpectedVersion.exact(0))
        await snapshot_store.save_snapshot(
            _make_snapshot(aggregate_id, {"value": "from_snapshot", "count": 5, "items": []})
        )
        event = TestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            aggregate_version=6,
            value="after_snapshot",
        )
        await event_store.append(stream, [event], ExpectedVersion.exact(5))
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 6
        assert loaded.state.value == "after_snapshot"
        assert loaded.state.count == 6

    @pytest.mark.asyncio
    async def test_load_falls_back_on_schema_mismatch(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate_id = uuid4()
        await snapshot_store.save_snapshot(
            _make_snapshot(
                aggregate_id,
                {"value": "old_snapshot", "count": 5, "items": []},
                schema_version=99,
            )
        )
        await event_store.append(
            StreamId(aggregate_id=aggregate_id, category="Test"),
            _make_events(aggregate_id, 3),
            ExpectedVersion.exact(0),
        )
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 3
        assert loaded.state.count == 3
        assert loaded.state.value == "event_2"

    @pytest.mark.asyncio
    async def test_load_falls_back_on_snapshot_error(self, event_store: InMemoryEventStore) -> None:
        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(side_effect=Exception("Store error"))
        aggregate_id = uuid4()
        event = TestEvent(
            aggregate_id=aggregate_id, aggregate_type="Test", aggregate_version=1, value="test"
        )
        await event_store.append(
            StreamId(aggregate_id=aggregate_id, category="Test"),
            [event],
            ExpectedVersion.exact(0),
        )
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=mock_snapshot_store,
        )
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 1

    @pytest.mark.asyncio
    async def test_load_falls_back_on_deserialization_error(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate_id = uuid4()
        await snapshot_store.save_snapshot(
            _make_snapshot(
                aggregate_id, {"value": "test", "count": {"invalid": "type"}, "items": []}
            )
        )
        await event_store.append(
            StreamId(aggregate_id=aggregate_id, category="Test"),
            _make_events(aggregate_id, 2),
            ExpectedVersion.exact(0),
        )
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 2

    @pytest.mark.asyncio
    async def test_load_raises_not_found_when_no_events(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        with pytest.raises(AggregateNotFoundError):
            await repo.load(uuid4())

    @pytest.mark.asyncio
    async def test_load_no_events_after_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        aggregate_id = uuid4()
        await snapshot_store.save_snapshot(
            _make_snapshot(aggregate_id, {"value": "snapshot_only", "count": 5, "items": []})
        )
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            snapshot_store=snapshot_store,
        )
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 5
        assert loaded.state.value == "snapshot_only"
