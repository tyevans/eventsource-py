"""
Unit tests for PostgreSQLEventStore tracing functionality.

Tests for:
- Composition-based Tracer integration
- Span creation for append_events and get_events
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest

from eventsource import DomainEvent, EventRegistry, register_event
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_DB_NAME,
    ATTR_DB_SYSTEM,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_TYPE,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
)
from eventsource.stores.postgresql import PostgreSQLEventStore

# ============================================================================
# Test Events for tracing tests
# ============================================================================


@register_event
class PostgreSQLTracingTestEvent(DomainEvent):
    """Test event for PostgreSQL tracing tests."""

    event_type: str = "PostgreSQLTracingTestEvent"
    aggregate_type: str = "PostgreSQLTracingTestAggregate"
    name: str


# ============================================================================
# Composition-based Tracer Integration Tests
# ============================================================================


class TestPostgreSQLEventStoreTracingMixin:
    """Tests for PostgreSQLEventStore composition-based Tracer integration."""

    @pytest.fixture
    def mock_session_factory(self):
        """Create a mock session factory for testing."""
        return MagicMock()

    def test_uses_composition_based_tracer(self, mock_session_factory):
        """PostgreSQLEventStore uses composition-based Tracer (not inheritance)."""
        registry = EventRegistry()
        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=True,
        )
        # Tracer is always set (either NullTracer or OpenTelemetryTracer)
        assert store._tracer is not None
        assert hasattr(store._tracer, "span")
        assert hasattr(store._tracer, "enabled")

    def test_tracing_enabled_by_default(self, mock_session_factory):
        """Tracing is enabled by default when OTEL is available."""
        registry = EventRegistry()
        store = PostgreSQLEventStore(mock_session_factory, event_registry=registry)

        # Check that tracing was initialized
        assert hasattr(store, "_enable_tracing")
        assert hasattr(store, "_tracer")

    def test_tracing_disabled_when_requested(self, mock_session_factory):
        """Tracing can be disabled via constructor parameter."""
        registry = EventRegistry()
        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=False,
        )

        assert store._enable_tracing is False
        # With composition-based tracing, _tracer is always set but disabled
        assert store._tracer is not None
        assert store._tracer.enabled is False

    def test_tracer_has_span_method(self, mock_session_factory):
        """Store's tracer exposes span() context manager method."""
        registry = EventRegistry()
        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=True,
        )

        # Tracer uses span() method for creating spans
        assert hasattr(store._tracer, "span")
        assert callable(store._tracer.span)


# ============================================================================
# Span Creation Tests
# ============================================================================


class TestPostgreSQLEventStoreSpanCreation:
    """Tests for span creation in PostgreSQLEventStore operations."""

    @pytest.fixture
    def mock_session_factory(self):
        """Create a mock session factory for testing."""
        return MagicMock()

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span_cm = MagicMock()
        span_cm.__enter__ = Mock(return_value=span)
        span_cm.__exit__ = Mock(return_value=None)
        tracer.span.return_value = span_cm
        tracer.enabled = True
        return tracer

    @pytest.fixture
    def traced_store(self, mock_session_factory, mock_tracer):
        """Create a store with injected mock tracer."""
        registry = EventRegistry()
        registry.register(PostgreSQLTracingTestEvent)

        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=True,
        )
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        return store

    @pytest.mark.asyncio
    async def test_append_events_creates_span(self, traced_store, mock_tracer):
        """append_events creates a span with correct name."""
        from uuid import uuid4

        aggregate_id = uuid4()
        event = PostgreSQLTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        # Mock the _do_append_events to avoid database interaction
        traced_store._do_append_events = MagicMock(
            return_value=MagicMock(success=True, new_version=1)
        )

        # Make it awaitable
        async def mock_append(*args, **kwargs):
            return MagicMock(success=True, new_version=1)

        traced_store._do_append_events = mock_append

        await traced_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        # Verify span was created with correct name
        mock_tracer.span.assert_called()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "postgresql_event_store.append_events"

    @pytest.mark.asyncio
    async def test_append_events_span_attributes(self, traced_store, mock_tracer):
        """append_events span includes correct standard attributes."""
        from uuid import uuid4

        aggregate_id = uuid4()
        event = PostgreSQLTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        # Mock the _do_append_events to avoid database interaction
        async def mock_append(*args, **kwargs):
            return MagicMock(success=True, new_version=1)

        traced_store._do_append_events = mock_append

        await traced_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        # Verify correct attributes using standard constants
        call_args = mock_tracer.span.call_args
        attributes = call_args[0][1]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "PostgreSQLTracingTestAggregate"
        assert ATTR_EVENT_COUNT in attributes
        assert attributes[ATTR_EVENT_COUNT] == 1
        assert ATTR_EXPECTED_VERSION in attributes
        assert attributes[ATTR_EXPECTED_VERSION] == 0
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == "PostgreSQLTracingTestEvent"
        assert ATTR_DB_SYSTEM in attributes
        assert attributes[ATTR_DB_SYSTEM] == "postgresql"
        assert ATTR_DB_NAME in attributes

    @pytest.mark.asyncio
    async def test_get_events_creates_span(self, traced_store, mock_tracer):
        """get_events creates a span with correct name."""
        from uuid import uuid4

        aggregate_id = uuid4()

        # Mock the _do_get_events to avoid database interaction
        async def mock_get(*args, **kwargs):
            return MagicMock(events=[], version=0)

        traced_store._do_get_events = mock_get

        await traced_store.get_events(
            aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
        )

        # Verify span was created with correct name
        mock_tracer.span.assert_called()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "postgresql_event_store.get_events"

    @pytest.mark.asyncio
    async def test_get_events_span_attributes(self, traced_store, mock_tracer):
        """get_events span includes correct standard attributes."""
        from uuid import uuid4

        aggregate_id = uuid4()

        # Mock the _do_get_events to avoid database interaction
        async def mock_get(*args, **kwargs):
            return MagicMock(events=[], version=0)

        traced_store._do_get_events = mock_get

        await traced_store.get_events(
            aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
            from_version=5,
        )

        # Verify correct attributes using standard constants
        call_args = mock_tracer.span.call_args
        attributes = call_args[0][1]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "PostgreSQLTracingTestAggregate"
        assert ATTR_FROM_VERSION in attributes
        assert attributes[ATTR_FROM_VERSION] == 5
        assert ATTR_DB_SYSTEM in attributes
        assert attributes[ATTR_DB_SYSTEM] == "postgresql"
        assert ATTR_DB_NAME in attributes


# ============================================================================
# Tracing Disabled Tests
# ============================================================================


class TestPostgreSQLEventStoreTracingDisabled:
    """Tests for PostgreSQLEventStore behavior when tracing is disabled."""

    @pytest.fixture
    def mock_session_factory(self):
        """Create a mock session factory for testing."""
        return MagicMock()

    @pytest.mark.asyncio
    async def test_append_events_works_without_tracing(self, mock_session_factory):
        """append_events works correctly when tracing is disabled."""
        registry = EventRegistry()
        registry.register(PostgreSQLTracingTestEvent)

        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=False,
        )

        from uuid import uuid4

        aggregate_id = uuid4()
        event = PostgreSQLTracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test",
        )

        # Mock the _do_append_events to avoid database interaction
        async def mock_append(*args, **kwargs):
            from eventsource.stores.interface import AppendResult

            return AppendResult.successful(1)

        store._do_append_events = mock_append

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1

    @pytest.mark.asyncio
    async def test_get_events_works_without_tracing(self, mock_session_factory):
        """get_events works correctly when tracing is disabled."""
        registry = EventRegistry()
        registry.register(PostgreSQLTracingTestEvent)

        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=False,
        )

        from uuid import uuid4

        aggregate_id = uuid4()

        # Mock the _do_get_events to avoid database interaction
        async def mock_get(*args, **kwargs):
            from eventsource.stores.interface import EventStream

            return EventStream(
                aggregate_id=aggregate_id,
                aggregate_type="PostgreSQLTracingTestAggregate",
                events=[],
                version=0,
            )

        store._do_get_events = mock_get

        stream = await store.get_events(
            aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
        )

        assert len(stream.events) == 0
        assert stream.version == 0


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestPostgreSQLEventStoreStandardAttributes:
    """Tests for standard attribute usage in PostgreSQLEventStore."""

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in postgresql.py."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/stores/postgresql.py"],
            capture_output=True,
            text=True,
        )
        # Should be 0 - no local definition
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in postgresql.py"
        )

    def test_imports_from_observability_module(self):
        """Verify PostgreSQLEventStore imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/stores/postgresql.py",
            ],
            capture_output=True,
            text=True,
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "PostgreSQLEventStore should import from eventsource.observability"

    def test_uses_standard_attribute_constants(self):
        """Verify PostgreSQLEventStore uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/stores/postgresql.py"],
            capture_output=True,
            text=True,
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 5, f"Expected at least 5 ATTR_* usages, found {count}"

    def test_uses_tracer_span_method(self):
        """Verify PostgreSQLEventStore uses self._tracer.span() method."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "self._tracer.span", "src/eventsource/stores/postgresql.py"],
            capture_output=True,
            text=True,
        )
        # Should find at least 2 usages (append_events and get_events)
        count = int(result.stdout.strip())
        assert count >= 2, f"Expected at least 2 self._tracer.span usages, found {count}"


# ============================================================================
# Multiple Events Tests
# ============================================================================


class TestPostgreSQLEventStoreTracingMultipleEvents:
    """Tests for tracing with multiple events."""

    @pytest.fixture
    def mock_session_factory(self):
        """Create a mock session factory for testing."""
        return MagicMock()

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span_cm = MagicMock()
        span_cm.__enter__ = Mock(return_value=span)
        span_cm.__exit__ = Mock(return_value=None)
        tracer.span.return_value = span_cm
        tracer.enabled = True
        return tracer

    @pytest.mark.asyncio
    async def test_multiple_events_count_attribute(self, mock_session_factory, mock_tracer):
        """Event count attribute reflects actual number of events."""
        from uuid import uuid4

        registry = EventRegistry()
        registry.register(PostgreSQLTracingTestEvent)

        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=True,
        )
        store._tracer = mock_tracer
        store._enable_tracing = True

        aggregate_id = uuid4()
        events = [
            PostgreSQLTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Event {i}",
            )
            for i in range(3)
        ]

        # Mock the _do_append_events to avoid database interaction
        async def mock_append(*args, **kwargs):
            from eventsource.stores.interface import AppendResult

            return AppendResult.successful(3)

        store._do_append_events = mock_append

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
            events=events,
            expected_version=0,
        )

        call_args = mock_tracer.span.call_args
        attributes = call_args[0][1]

        assert attributes[ATTR_EVENT_COUNT] == 3
        assert "PostgreSQLTracingTestEvent" in attributes[ATTR_EVENT_TYPE]

    @pytest.mark.asyncio
    async def test_empty_events_no_span(self, mock_session_factory, mock_tracer):
        """No span created when events list is empty."""
        from uuid import uuid4

        registry = EventRegistry()

        store = PostgreSQLEventStore(
            mock_session_factory,
            event_registry=registry,
            enable_tracing=True,
        )
        store._tracer = mock_tracer
        store._enable_tracing = True

        aggregate_id = uuid4()
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="PostgreSQLTracingTestAggregate",
            events=[],  # Empty list
            expected_version=0,
        )

        assert result.success is True

        # No span should be created for empty events
        mock_tracer.span.assert_not_called()
