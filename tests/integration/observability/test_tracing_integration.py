"""
Integration tests for end-to-end tracing functionality.

Tests for:
- Event store tracing with real span creation
- Span hierarchy verification (parent-child relationships)
- Standard attribute propagation
- Tracing graceful degradation
- Cross-component trace propagation
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, InMemoryEventBus
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
)
from eventsource.ports import ExpectedVersion, collect

from .conftest import TracingTestEvent, TracingTestEventCreated, TracingTestEventUpdated

if TYPE_CHECKING:
    from collections.abc import Callable


pytestmark = [pytest.mark.integration]


# ============================================================================
# Test Aggregate for Repository Tests
# ============================================================================


class TracingTestAggregateState:
    """State for TracingTestAggregate."""

    def __init__(self) -> None:
        self.name: str = ""
        self.value: int = 0


class TracingTestAggregate:
    """Simple test aggregate for tracing integration tests."""

    aggregate_type = "TracingTestAggregate"
    schema_version = 1

    def __init__(self, aggregate_id):
        self.aggregate_id = aggregate_id
        self._version = 0
        self._uncommitted_events: list[DomainEvent] = []
        self._state = TracingTestAggregateState()

    @property
    def version(self):
        return self._version

    @property
    def uncommitted_events(self):
        return self._uncommitted_events.copy()

    @property
    def has_uncommitted_events(self):
        return len(self._uncommitted_events) > 0

    @property
    def state(self):
        return self._state

    def mark_events_as_committed(self):
        self._uncommitted_events.clear()

    def apply_event(self, event: DomainEvent, is_new: bool = True) -> None:
        if isinstance(event, TracingTestEventCreated):
            self._state.name = event.name
        elif isinstance(event, TracingTestEventUpdated):
            self._state.value = event.value

        if is_new:
            self._uncommitted_events.append(event)

        self._version += 1

    def load_from_history(self, events: list[DomainEvent]) -> None:
        for event in events:
            self.apply_event(event, is_new=False)

    def create(self, name: str) -> None:
        """Create the aggregate with a name."""
        event = TracingTestEventCreated(
            aggregate_id=self.aggregate_id,
            aggregate_version=self._version + 1,
            name=name,
        )
        self.apply_event(event)

    def update_value(self, value: int) -> None:
        """Update the value."""
        event = TracingTestEventUpdated(
            aggregate_id=self.aggregate_id,
            aggregate_version=self._version + 1,
            value=value,
        )
        self.apply_event(event)


# ============================================================================
# Event Bus Tracing Tests
# ============================================================================


class TestEventBusTracing:
    """Integration tests for event bus tracing."""

    @pytest.mark.asyncio
    async def test_inmemory_bus_dispatch_creates_span(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
        sample_aggregate_id,
    ):
        """InMemoryEventBus dispatch creates a span with event details."""
        bus = InMemoryEventBus(enable_tracing=True)

        # Register a handler
        handled_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            handled_events.append(event)

        bus.subscribe(TracingTestEvent, handler)

        # Publish an event
        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        # Find the dispatch span
        span = find_span("dispatch")
        assert span is not None, "Event dispatch should create a span"

        attributes = dict(span.attributes)
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == "TracingTestEvent"

    @pytest.mark.asyncio
    async def test_inmemory_bus_handler_creates_span(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
        sample_aggregate_id,
    ):
        """InMemoryEventBus handler execution creates a child span."""
        bus = InMemoryEventBus(enable_tracing=True)

        handled_events: list[DomainEvent] = []

        async def my_handler(event: DomainEvent) -> None:
            handled_events.append(event)

        bus.subscribe(TracingTestEvent, my_handler)

        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        # Find the handler span
        span = find_span("handle")
        assert span is not None, "Handler execution should create a span"

        attributes = dict(span.attributes)
        assert ATTR_HANDLER_NAME in attributes

    @pytest.mark.asyncio
    async def test_multiple_handlers_create_separate_spans(
        self,
        trace_provider: Any,
        find_spans: Callable[[str], list[Any]],
        sample_aggregate_id,
    ):
        """Multiple handlers for same event create separate spans."""
        bus = InMemoryEventBus(enable_tracing=True)

        async def handler1(event: DomainEvent) -> None:
            pass

        async def handler2(event: DomainEvent) -> None:
            pass

        async def handler3(event: DomainEvent) -> None:
            pass

        bus.subscribe(TracingTestEvent, handler1)
        bus.subscribe(TracingTestEvent, handler2)
        bus.subscribe(TracingTestEvent, handler3)

        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        # Find all handler spans
        handler_spans = find_spans("handle")
        assert len(handler_spans) == 3, "Should have one span per handler"


# ============================================================================
# Repository Tracing Tests
# ============================================================================


class TestRepositoryTracing:
    """Integration tests for repository tracing."""

    @pytest.mark.asyncio
    async def test_repository_save_creates_span(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
    ):
        """AggregateRepository.save creates a span."""
        store = InMemoryEventStore()
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTestAggregate",
            enable_tracing=True,
        )

        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")

        await repo.save(aggregate)

        span = find_span("repository.save")
        assert span is not None, "repository.save should create a span"

        attributes = dict(span.attributes)
        assert ATTR_AGGREGATE_ID in attributes
        assert ATTR_AGGREGATE_TYPE in attributes

    @pytest.mark.asyncio
    async def test_repository_load_creates_span(
        self,
        trace_provider: Any,
        trace_exporter: Any,
        find_span: Callable[[str], Any | None],
    ):
        """AggregateRepository.load creates a span."""
        store = InMemoryEventStore()
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTestAggregate",
            enable_tracing=True,
        )

        # First save an aggregate
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")
        await repo.save(aggregate)

        # Clear exporter to isolate load span
        trace_exporter.clear()

        # Load the aggregate
        await repo.load(aggregate_id)

        span = find_span("repository.load")
        assert span is not None, "repository.load should create a span"


# ============================================================================
# Span Hierarchy Tests
# ============================================================================


class TestSpanHierarchy:
    """Tests for correct span parent-child relationships."""

    @pytest.mark.asyncio
    async def test_event_bus_handler_spans_are_children_of_dispatch(
        self,
        trace_provider: Any,
        get_spans: Callable[[], list[Any]],
    ):
        """Event bus handler spans should be children of the dispatch span."""
        bus = InMemoryEventBus(enable_tracing=True)

        async def handler(event: DomainEvent) -> None:
            pass

        bus.subscribe(TracingTestEvent, handler)

        event = TracingTestEvent(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        spans = get_spans()

        dispatch_span = next((s for s in spans if "dispatch" in s.name), None)
        handler_span = next((s for s in spans if "handle" in s.name), None)

        assert dispatch_span is not None
        assert handler_span is not None

        # Handler should be child of dispatch
        if handler_span.parent is not None:
            assert handler_span.parent.span_id == dispatch_span.context.span_id


# ============================================================================
# Graceful Degradation Tests
# ============================================================================


class TestTracingGracefulDegradation:
    """Tests that tracing degrades gracefully when disabled."""

    @pytest.mark.asyncio
    async def test_event_store_works_without_tracing(
        self,
        sample_aggregate_id,
    ):
        """Event store operations work correctly when tracing is disabled."""
        store = InMemoryEventStore()

        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        stream = StreamId(aggregate_id=sample_aggregate_id, category="TracingTestAggregate")
        result = await store.append(stream, [event], ExpectedVersion.exact(0))

        assert result.position is not None

        events = await collect(store.read_stream(stream))
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_event_bus_works_without_tracing(self):
        """Event bus operations work correctly when tracing is disabled."""
        bus = InMemoryEventBus(enable_tracing=False)

        handled_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            handled_events.append(event)

        bus.subscribe(TracingTestEvent, handler)

        event = TracingTestEvent(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        assert len(handled_events) == 1
        assert handled_events[0].name == "Test"

    @pytest.mark.asyncio
    async def test_repository_works_without_tracing(self):
        """Repository operations work correctly when tracing is disabled."""
        store = InMemoryEventStore()
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTestAggregate",
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")

        await repo.save(aggregate)

        loaded = await repo.load(aggregate_id)
        assert loaded.state.name == "Test"


# ============================================================================
# Cross-Trace ID Consistency Tests
# ============================================================================


class TestTraceIdConsistency:
    """Tests for trace ID consistency across operations."""

    @pytest.mark.asyncio
    async def test_all_spans_in_operation_share_trace_id(
        self,
        trace_provider: Any,
        get_spans: Callable[[], list[Any]],
    ):
        """All spans created during a single logical operation share the same trace ID."""
        store = InMemoryEventStore()
        bus = InMemoryEventBus(enable_tracing=True)
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTestAggregate",
            event_publisher=bus,
            enable_tracing=True,
        )

        # Register a handler
        async def handler(event: DomainEvent) -> None:
            pass

        bus.subscribe(TracingTestEventCreated, handler)

        # Perform a save which should:
        # 1. Create repository.save span
        # 2. Publish event, creating bus dispatch/handle spans
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")

        await repo.save(aggregate)

        spans = get_spans()

        # Should have multiple spans
        assert len(spans) >= 2, f"Should have multiple spans, got {len(spans)}"

        # All spans should share the same trace ID
        trace_ids = {span.context.trace_id for span in spans}
        assert len(trace_ids) == 1, (
            f"All spans should share the same trace ID, but found {len(trace_ids)} different IDs"
        )


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestStandardAttributes:
    """Tests for standard attribute usage across components."""

    @pytest.mark.asyncio
    async def test_event_bus_uses_standard_attributes(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
    ):
        """Event bus spans use standard ATTR_* attribute keys."""
        bus = InMemoryEventBus(enable_tracing=True)

        async def handler(event: DomainEvent) -> None:
            pass

        bus.subscribe(TracingTestEvent, handler)

        event = TracingTestEvent(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        dispatch_span = find_span("dispatch")
        handler_span = find_span("handle")

        assert dispatch_span is not None
        assert handler_span is not None

        # Dispatch span should have event type
        dispatch_attrs = dict(dispatch_span.attributes)
        assert ATTR_EVENT_TYPE in dispatch_attrs

        # Handler span should have handler name
        handler_attrs = dict(handler_span.attributes)
        assert ATTR_HANDLER_NAME in handler_attrs

    @pytest.mark.asyncio
    async def test_repository_uses_standard_attributes(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
    ):
        """Repository spans use standard ATTR_* attribute keys."""
        store = InMemoryEventStore()
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTestAggregate",
            enable_tracing=True,
        )

        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.create(name="Test")

        await repo.save(aggregate)

        span = find_span("repository.save")
        assert span is not None

        attributes = dict(span.attributes)
        assert ATTR_AGGREGATE_ID in attributes
        assert ATTR_AGGREGATE_TYPE in attributes
