"""
Unit tests for InMemoryEventStore tracing functionality.

Tests for:
- TracingMixin integration
- Span creation for append_events, get_events, read_stream, read_all
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest

from eventsource import DomainEvent, register_event
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
    ATTR_POSITION,
    ATTR_STREAM_ID,
    TracingMixin,
)
from eventsource.stores.in_memory import InMemoryEventStore

# ============================================================================
# Test Events for tracing tests
# ============================================================================


@register_event
class MemoryTracingTestEvent(DomainEvent):
    """Test event for tracing tests."""

    event_type: str = "MemoryTracingTestEvent"
    aggregate_type: str = "MemoryTracingTestAggregate"
    name: str


# ============================================================================
# TracingMixin Integration Tests
# ============================================================================


class TestInMemoryEventStoreTracingMixin:
    """Tests for InMemoryEventStore TracingMixin integration."""

    def test_inherits_from_tracing_mixin(self):
        """InMemoryEventStore inherits from TracingMixin."""
        assert issubclass(InMemoryEventStore, TracingMixin)

    def test_tracing_enabled_by_default(self):
        """Tracing is enabled by default when OTEL is available."""
        store = InMemoryEventStore()

        # Check that tracing was initialized
        assert hasattr(store, "_enable_tracing")
        assert hasattr(store, "_tracer")

    def test_tracing_disabled_when_requested(self):
        """Tracing can be disabled via constructor parameter."""
        store = InMemoryEventStore(enable_tracing=False)

        assert store._enable_tracing is False
        assert store._tracer is None

    def test_has_tracing_enabled_property(self):
        """Store exposes tracing_enabled property from mixin."""
        store = InMemoryEventStore(enable_tracing=True)

        # tracing_enabled is a property from TracingMixin
        assert hasattr(store, "tracing_enabled")
        assert isinstance(store.tracing_enabled, bool)

    def test_backward_compatible_constructor(self):
        """Constructor without enable_tracing should work (default True)."""
        store = InMemoryEventStore()
        # Should not raise, tracing defaults to enabled
        assert hasattr(store, "_enable_tracing")


# ============================================================================
# Span Creation Tests
# ============================================================================


class TestInMemoryEventStoreSpanCreation:
    """Tests for span creation in InMemoryEventStore operations."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer

    @pytest.fixture
    def traced_store(self, mock_tracer):
        """Create a store with injected mock tracer."""
        store = InMemoryEventStore(enable_tracing=True)
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        return store

    @pytest.mark.asyncio
    async def test_append_events_creates_span(self, traced_store, mock_tracer):
        """append_events creates a span with correct name."""
        aggregate_id = uuid4()
        event = MemoryTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        await traced_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "inmemory_event_store.append_events"

    @pytest.mark.asyncio
    async def test_append_events_span_attributes(self, traced_store, mock_tracer):
        """append_events span includes correct standard attributes."""
        aggregate_id = uuid4()
        event = MemoryTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        await traced_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "MemoryTracingTestAggregate"
        assert ATTR_EVENT_COUNT in attributes
        assert attributes[ATTR_EVENT_COUNT] == 1
        assert ATTR_EXPECTED_VERSION in attributes
        assert attributes[ATTR_EXPECTED_VERSION] == 0

    @pytest.mark.asyncio
    async def test_get_events_creates_span(self, traced_store, mock_tracer):
        """get_events creates a span with correct name."""
        aggregate_id = uuid4()
        await traced_store.get_events(
            aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
        )

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "inmemory_event_store.get_events"

    @pytest.mark.asyncio
    async def test_get_events_span_attributes(self, traced_store, mock_tracer):
        """get_events span includes correct standard attributes."""
        aggregate_id = uuid4()
        await traced_store.get_events(
            aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            from_version=5,
        )

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "MemoryTracingTestAggregate"
        assert ATTR_FROM_VERSION in attributes
        assert attributes[ATTR_FROM_VERSION] == 5

    @pytest.mark.asyncio
    async def test_read_stream_creates_span(self, traced_store, mock_tracer):
        """read_stream creates a span with correct name."""
        aggregate_id = uuid4()
        stream_id = f"{aggregate_id}:MemoryTracingTestAggregate"

        # Consume the async iterator
        async for _ in traced_store.read_stream(stream_id):
            pass

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "inmemory_event_store.read_stream"

    @pytest.mark.asyncio
    async def test_read_stream_span_attributes(self, traced_store, mock_tracer):
        """read_stream span includes correct standard attributes."""
        aggregate_id = uuid4()
        stream_id = f"{aggregate_id}:MemoryTracingTestAggregate"

        # Consume the async iterator
        async for _ in traced_store.read_stream(stream_id):
            pass

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_STREAM_ID in attributes
        assert attributes[ATTR_STREAM_ID] == stream_id
        assert ATTR_POSITION in attributes
        assert attributes[ATTR_POSITION] == 0

    @pytest.mark.asyncio
    async def test_read_all_creates_span(self, traced_store, mock_tracer):
        """read_all creates a span with correct name."""
        # Consume the async iterator
        async for _ in traced_store.read_all():
            pass

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "inmemory_event_store.read_all"

    @pytest.mark.asyncio
    async def test_read_all_span_attributes(self, traced_store, mock_tracer):
        """read_all span includes correct standard attributes."""
        # Consume the async iterator
        async for _ in traced_store.read_all():
            pass

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_POSITION in attributes
        assert attributes[ATTR_POSITION] == 0


# ============================================================================
# Tracing Disabled Tests
# ============================================================================


class TestInMemoryEventStoreTracingDisabled:
    """Tests for InMemoryEventStore behavior when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_append_events_works_without_tracing(self):
        """append_events works correctly when tracing is disabled."""
        store = InMemoryEventStore(enable_tracing=False)

        aggregate_id = uuid4()
        event = MemoryTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1

    @pytest.mark.asyncio
    async def test_get_events_works_without_tracing(self):
        """get_events works correctly when tracing is disabled."""
        store = InMemoryEventStore(enable_tracing=False)

        aggregate_id = uuid4()
        event = MemoryTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        stream = await store.get_events(
            aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
        )

        assert len(stream.events) == 1
        assert stream.version == 1

    @pytest.mark.asyncio
    async def test_read_stream_works_without_tracing(self):
        """read_stream works correctly when tracing is disabled."""
        store = InMemoryEventStore(enable_tracing=False)

        aggregate_id = uuid4()
        event = MemoryTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        stream_id = f"{aggregate_id}:MemoryTracingTestAggregate"
        events = []
        async for stored_event in store.read_stream(stream_id):
            events.append(stored_event)

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_read_all_works_without_tracing(self):
        """read_all works correctly when tracing is disabled."""
        store = InMemoryEventStore(enable_tracing=False)

        aggregate_id = uuid4()
        event = MemoryTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        events = []
        async for stored_event in store.read_all():
            events.append(stored_event)

        assert len(events) == 1


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestInMemoryEventStoreStandardAttributes:
    """Tests for standard attribute usage in InMemoryEventStore."""

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in in_memory.py."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/stores/in_memory.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be 0 - no local definition
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in in_memory.py"
        )

    def test_imports_from_observability_module(self):
        """Verify InMemoryEventStore imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/stores/in_memory.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "InMemoryEventStore should import from eventsource.observability"

    def test_uses_standard_attribute_constants(self):
        """Verify InMemoryEventStore uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/stores/in_memory.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 5, f"Expected at least 5 ATTR_* usages, found {count}"


# ============================================================================
# Multiple Events Tests
# ============================================================================


class TestInMemoryEventStoreTracingMultipleEvents:
    """Tests for tracing with multiple events."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer

    @pytest.mark.asyncio
    async def test_multiple_events_count_attribute(self, mock_tracer):
        """Event count attribute reflects actual number of events."""
        store = InMemoryEventStore(enable_tracing=True)
        store._tracer = mock_tracer
        store._enable_tracing = True

        aggregate_id = uuid4()
        events = [
            MemoryTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Event {i}",
            )
            for i in range(3)
        ]

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=events,
            expected_version=0,
        )

        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert attributes[ATTR_EVENT_COUNT] == 3

    @pytest.mark.asyncio
    async def test_empty_events_no_span(self, mock_tracer):
        """No span created when events list is empty."""
        store = InMemoryEventStore(enable_tracing=True)
        store._tracer = mock_tracer
        store._enable_tracing = True

        aggregate_id = uuid4()
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=[],  # Empty list
            expected_version=0,
        )

        assert result.success is True

        # No span should be created for empty events
        mock_tracer.start_as_current_span.assert_not_called()


# ============================================================================
# Operations with Events Tests
# ============================================================================


class TestInMemoryEventStoreTracingWithEvents:
    """Tests for tracing with actual events in the store."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer

    @pytest.mark.asyncio
    async def test_read_stream_with_events(self, mock_tracer):
        """read_stream correctly yields events with tracing enabled."""
        store = InMemoryEventStore(enable_tracing=True)

        # First append some events without mock tracer
        aggregate_id = uuid4()
        events = [
            MemoryTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Event {i}",
            )
            for i in range(3)
        ]

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=events,
            expected_version=0,
        )

        # Now inject mock tracer and read
        store._tracer = mock_tracer
        store._enable_tracing = True

        stream_id = f"{aggregate_id}:MemoryTracingTestAggregate"
        read_events = []
        async for stored_event in store.read_stream(stream_id):
            read_events.append(stored_event)

        assert len(read_events) == 3
        mock_tracer.start_as_current_span.assert_called()

    @pytest.mark.asyncio
    async def test_read_all_with_events(self, mock_tracer):
        """read_all correctly yields events with tracing enabled."""
        store = InMemoryEventStore(enable_tracing=True)

        # First append some events without mock tracer
        aggregate_id = uuid4()
        events = [
            MemoryTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Event {i}",
            )
            for i in range(3)
        ]

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="MemoryTracingTestAggregate",
            events=events,
            expected_version=0,
        )

        # Now inject mock tracer and read
        store._tracer = mock_tracer
        store._enable_tracing = True

        read_events = []
        async for stored_event in store.read_all():
            read_events.append(stored_event)

        assert len(read_events) == 3
        mock_tracer.start_as_current_span.assert_called()
