"""Tests for span creation in AggregateRepository operations."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_VERSION,
    MockTracer,
)
from eventsource.ports import ExpectedVersion
from tests.unit.application.aggregates.test_repository_tracing._fixtures import (
    _REGISTRY,
    TracingTestAggregate,
    TracingTestEvent,
)


class TestAggregateRepositorySpanCreation:
    """Tests for span creation in AggregateRepository operations."""

    @pytest.fixture
    def mock_tracer(self):
        return MockTracer()

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.fixture
    def traced_repo(self, event_store, mock_tracer):
        return AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=mock_tracer,
        )

    @pytest.fixture
    def traced_repo_with_snapshots(self, event_store, snapshot_store, mock_tracer):
        return AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            snapshot_store=snapshot_store,
            tracer=mock_tracer,
        )

    @pytest.mark.asyncio
    async def test_save_creates_span(self, traced_repo, mock_tracer):
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        await traced_repo.save(aggregate)
        assert "eventsource.repository.save" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_save_span_attributes(self, traced_repo, mock_tracer):
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")
        await traced_repo.save(aggregate)

        save_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.save"
        ]
        assert len(save_spans) > 0
        _, attributes = save_spans[0]
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"
        assert attributes[ATTR_EVENT_COUNT] == 1
        assert attributes[ATTR_VERSION] == 1

    @pytest.mark.asyncio
    async def test_load_creates_span(self, traced_repo, mock_tracer, event_store):
        aggregate_id = uuid4()
        event = TracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            aggregate_version=1,
            value="test",
        )
        await event_store.append(
            StreamId(aggregate_id=aggregate_id, category="TracingTest"),
            [event],
            ExpectedVersion.exact(0),
        )
        await traced_repo.load(aggregate_id)
        assert "eventsource.repository.load" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_load_span_attributes(self, traced_repo, mock_tracer, event_store):
        aggregate_id = uuid4()
        event = TracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            aggregate_version=1,
            value="test",
        )
        await event_store.append(
            StreamId(aggregate_id=aggregate_id, category="TracingTest"),
            [event],
            ExpectedVersion.exact(0),
        )
        await traced_repo.load(aggregate_id)

        load_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.load"
        ]
        assert len(load_spans) > 0
        _, attributes = load_spans[0]
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"

    @pytest.mark.asyncio
    async def test_exists_creates_span(self, traced_repo, mock_tracer):
        await traced_repo.exists(uuid4())
        assert "eventsource.repository.exists" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_exists_span_attributes(self, traced_repo, mock_tracer):
        aggregate_id = uuid4()
        await traced_repo.exists(aggregate_id)

        exists_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.exists"
        ]
        assert len(exists_spans) > 0
        _, attributes = exists_spans[0]
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"

    @pytest.mark.asyncio
    async def test_create_snapshot_creates_span(self, traced_repo_with_snapshots, mock_tracer):
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()
        await traced_repo_with_snapshots.create_snapshot(aggregate)
        assert "eventsource.repository.create_snapshot" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_create_snapshot_span_attributes(self, traced_repo_with_snapshots, mock_tracer):
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()
        await traced_repo_with_snapshots.create_snapshot(aggregate)

        snapshot_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.create_snapshot"
        ]
        assert len(snapshot_spans) > 0
        _, attributes = snapshot_spans[0]
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"
        assert attributes[ATTR_VERSION] == 1


__all__ = ["TestAggregateRepositorySpanCreation"]
