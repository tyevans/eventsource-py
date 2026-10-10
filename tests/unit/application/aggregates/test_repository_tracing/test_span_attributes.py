"""Tests for dynamic span attributes and multiple events in AggregateRepository."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.observability import (
    ATTR_EVENT_COUNT,
    MockTracer,
)
from eventsource.ports import ExpectedVersion
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_tracing._fixtures import (
    _REGISTRY,
    TracingTestAggregate,
    TracingTestEvent,
)


class TestAggregateRepositorySpanDynamicAttributes:
    """Tests for dynamic span attributes set during operation.

    Note: These tests verify that spans are created properly. Dynamic attribute
    testing (set_attribute calls) requires OpenTelemetry integration and is
    covered by integration tests.
    """

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

    @pytest.mark.asyncio
    async def test_load_creates_span_with_multiple_events(self, event_store, mock_tracer):
        """load creates a span when loading multiple events."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=mock_tracer,
        )

        # Create events
        aggregate_id = uuid4()
        events = [
            TracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_type="TracingTest",
                aggregate_version=i + 1,
                value=f"test_{i}",
            )
            for i in range(3)
        ]
        await event_store.append(
            StreamId(aggregate_id=aggregate_id, category="TracingTest"),
            events,
            ExpectedVersion.exact(0),
        )

        await repo.load(aggregate_id)

        # Verify span was created
        assert "eventsource.repository.load" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_save_creates_span(self, event_store, mock_tracer):
        """save creates a span with events."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=mock_tracer,
        )

        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")

        await repo.save(aggregate)

        # Verify span was created
        assert "eventsource.repository.save" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_exists_creates_span(self, event_store, mock_tracer):
        """exists creates a span."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=mock_tracer,
        )

        aggregate_id = uuid4()
        await repo.exists(aggregate_id)

        # Verify span was created
        assert "eventsource.repository.exists" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_load_with_snapshot_creates_span(self, event_store, snapshot_store, mock_tracer):
        """load creates a span when using snapshot."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            snapshot_store=snapshot_store,
            tracer=mock_tracer,
        )

        aggregate_id = uuid4()

        # Create a snapshot
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            version=5,
            state={"value": "from_snapshot", "count": 5, "items": []},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)

        await repo.load(aggregate_id)

        # Verify span was created
        assert "eventsource.repository.load" in mock_tracer.span_names


class TestAggregateRepositoryTracingMultipleEvents:
    """Tests for tracing with multiple events."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a MockTracer for testing span creation."""
        return MockTracer()

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.mark.asyncio
    async def test_multiple_events_count_attribute(self, event_store, mock_tracer):
        """Event count attribute reflects actual number of events."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=mock_tracer,
        )

        aggregate = TracingTestAggregate(uuid4())
        for i in range(5):
            aggregate.do_something(f"test_{i}")

        await repo.save(aggregate)

        # Find the save span and verify event count attribute
        save_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.repository.save"
        ]
        assert len(save_spans) > 0
        name, attributes = save_spans[0]

        assert attributes[ATTR_EVENT_COUNT] == 5

    @pytest.mark.asyncio
    async def test_no_span_when_no_events_to_save(self, event_store, mock_tracer):
        """No span created when there are no uncommitted events."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=mock_tracer,
        )

        aggregate = TracingTestAggregate(uuid4())
        # No events applied

        await repo.save(aggregate)

        # No span should be created for empty events
        save_spans = [
            name for name in mock_tracer.span_names if name == "eventsource.repository.save"
        ]
        assert len(save_spans) == 0


__all__ = [
    "TestAggregateRepositorySpanDynamicAttributes",
    "TestAggregateRepositoryTracingMultipleEvents",
]
