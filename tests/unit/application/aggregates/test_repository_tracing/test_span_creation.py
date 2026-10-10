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
        """Create a MockTracer for testing span creation."""
        return MockTracer()

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.fixture
    def traced_repo(self, event_store, mock_tracer):
        """Create a repository with injected mock tracer."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=mock_tracer,
        )
        return repo

    @pytest.fixture
    def traced_repo_with_snapshots(self, event_store, snapshot_store, mock_tracer):
        """Create a repository with snapshots and injected mock tracer."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            snapshot_store=snapshot_store,
            tracer=mock_tracer,
        )
        return repo

    @pytest.mark.asyncio
    async def test_save_creates_span(self, traced_repo, mock_tracer):
        """save creates a span with correct name."""
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")

        await traced_repo.save(aggregate)

        # Verify span was created with correct name
        assert "eventsource.repository.save" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_save_span_attributes(self, traced_repo, mock_tracer):
        """save span includes correct standard attributes."""
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")

        await traced_repo.save(aggregate)

        # Find the save span and verify attributes
        save_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.save"
        ]
        assert len(save_spans) > 0
        name, attributes = save_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"
        assert ATTR_EVENT_COUNT in attributes
        assert attributes[ATTR_EVENT_COUNT] == 1
        assert ATTR_VERSION in attributes
        assert attributes[ATTR_VERSION] == 1

    @pytest.mark.asyncio
    async def test_load_creates_span(self, traced_repo, mock_tracer, event_store):
        """load creates a span with correct name."""
        # First create an aggregate
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

        # Verify span was created with correct name
        assert "eventsource.repository.load" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_load_span_attributes(self, traced_repo, mock_tracer, event_store):
        """load span includes correct standard attributes."""
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

        # Find the load span and verify attributes
        load_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.load"
        ]
        assert len(load_spans) > 0
        name, attributes = load_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"

    @pytest.mark.asyncio
    async def test_exists_creates_span(self, traced_repo, mock_tracer):
        """exists creates a span with correct name."""
        aggregate_id = uuid4()

        await traced_repo.exists(aggregate_id)

        # Verify span was created with correct name
        assert "eventsource.repository.exists" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_exists_span_attributes(self, traced_repo, mock_tracer):
        """exists span includes correct standard attributes."""
        aggregate_id = uuid4()

        await traced_repo.exists(aggregate_id)

        # Find the exists span and verify attributes
        exists_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.exists"
        ]
        assert len(exists_spans) > 0
        name, attributes = exists_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"

    @pytest.mark.asyncio
    async def test_create_snapshot_creates_span(self, traced_repo_with_snapshots, mock_tracer):
        """create_snapshot creates a span with correct name."""
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()  # Simulate already saved

        await traced_repo_with_snapshots.create_snapshot(aggregate)

        # Verify span was created with correct name
        assert "eventsource.repository.create_snapshot" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_create_snapshot_span_attributes(self, traced_repo_with_snapshots, mock_tracer):
        """create_snapshot span includes correct standard attributes."""
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()

        await traced_repo_with_snapshots.create_snapshot(aggregate)

        # Find the create_snapshot span and verify attributes
        snapshot_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.create_snapshot"
        ]
        assert len(snapshot_spans) > 0
        name, attributes = snapshot_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"
        assert ATTR_VERSION in attributes
        assert attributes[ATTR_VERSION] == 1


__all__ = [
    "TestAggregateRepositorySpanCreation",
]
