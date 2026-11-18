"""Tests for tracing event-count attributes across multiple events."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.observability import (
    ATTR_EVENT_COUNT,
    MockTracer,
)
from tests.unit.application.aggregates.test_repository_tracing._fixtures import (
    _REGISTRY,
    TracingTestAggregate,
)


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
    "TestAggregateRepositoryTracingMultipleEvents",
]
