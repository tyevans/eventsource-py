"""
Unit tests for SQLiteEventStore tracing functionality.

Tests for:
- Composition-based Tracer integration
- Span creation for append_events and get_events
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

# ============================================================================
# Skip condition for all tests in this module
# ============================================================================

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


# ============================================================================
# Test Events for tracing tests
# ============================================================================

if AIOSQLITE_AVAILABLE:
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
    from eventsource.stores.sqlite import SQLiteEventStore

    @register_event
    class SQLiteTracingTestEvent(DomainEvent):
        """Test event for SQLite tracing tests."""

        event_type: str = "SQLiteTracingTestEvent"
        aggregate_type: str = "SQLiteTracingTestAggregate"
        name: str


# ============================================================================
# Composition-based Tracer Integration Tests
# ============================================================================


class TestSQLiteEventStoreTracingMixin:
    """Tests for SQLiteEventStore composition-based Tracer integration."""

    def test_uses_composition_based_tracer(self):
        """SQLiteEventStore uses composition-based Tracer (not inheritance)."""
        registry = EventRegistry()
        store = SQLiteEventStore(":memory:", registry, enable_tracing=True)
        # Tracer is always set (either NullTracer or OpenTelemetryTracer)
        assert store._tracer is not None
        assert hasattr(store._tracer, "span")
        assert hasattr(store._tracer, "enabled")

    def test_tracing_enabled_by_default(self):
        """Tracing is enabled by default when OTEL is available."""
        registry = EventRegistry()
        store = SQLiteEventStore(":memory:", registry)

        # Check that tracing was initialized
        assert hasattr(store, "_enable_tracing")
        assert hasattr(store, "_tracer")

    def test_tracing_disabled_when_requested(self):
        """Tracing can be disabled via constructor parameter."""
        registry = EventRegistry()
        store = SQLiteEventStore(":memory:", registry, enable_tracing=False)

        assert store._enable_tracing is False
        # With composition-based tracing, _tracer is always set but disabled
        assert store._tracer is not None
        assert store._tracer.enabled is False

    def test_tracer_has_span_method(self):
        """Store's tracer exposes span() context manager method."""
        registry = EventRegistry()
        store = SQLiteEventStore(":memory:", registry, enable_tracing=True)

        # Tracer uses span() method for creating spans
        assert hasattr(store._tracer, "span")
        assert callable(store._tracer.span)


# ============================================================================
# Span Creation Tests
# ============================================================================


class TestSQLiteEventStoreSpanCreation:
    """Tests for span creation in SQLiteEventStore operations."""

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
    def traced_store(self, mock_tracer):
        """Create a store with injected mock tracer."""
        registry = EventRegistry()
        registry.register(SQLiteTracingTestEvent)

        store = SQLiteEventStore(":memory:", registry, enable_tracing=True)
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        return store

    @pytest.mark.asyncio
    async def test_append_events_creates_span(self, traced_store, mock_tracer):
        """append_events creates a span with correct name."""
        async with traced_store:
            await traced_store.initialize()

            aggregate_id = uuid4()
            event = SQLiteTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Test",
            )

            await traced_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
                events=[event],
                expected_version=0,
            )

        # Verify span was created with correct name
        mock_tracer.span.assert_called()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "sqlite_event_store.append_events"

    @pytest.mark.asyncio
    async def test_append_events_span_attributes(self, traced_store, mock_tracer):
        """append_events span includes correct standard attributes."""
        async with traced_store:
            await traced_store.initialize()

            aggregate_id = uuid4()
            event = SQLiteTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Test",
            )

            await traced_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
                events=[event],
                expected_version=0,
            )

        # Verify correct attributes using standard constants
        call_args = mock_tracer.span.call_args
        attributes = call_args[0][1]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "SQLiteTracingTestAggregate"
        assert ATTR_EVENT_COUNT in attributes
        assert attributes[ATTR_EVENT_COUNT] == 1
        assert ATTR_EXPECTED_VERSION in attributes
        assert attributes[ATTR_EXPECTED_VERSION] == 0
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == "SQLiteTracingTestEvent"
        assert ATTR_DB_SYSTEM in attributes
        assert attributes[ATTR_DB_SYSTEM] == "sqlite"
        assert ATTR_DB_NAME in attributes
        assert attributes[ATTR_DB_NAME] == ":memory:"

    @pytest.mark.asyncio
    async def test_get_events_creates_span(self, traced_store, mock_tracer):
        """get_events creates a span with correct name."""
        async with traced_store:
            await traced_store.initialize()

            aggregate_id = uuid4()
            await traced_store.get_events(
                aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
            )

        # Verify span was created with correct name
        mock_tracer.span.assert_called()
        call_args = mock_tracer.span.call_args
        assert call_args[0][0] == "sqlite_event_store.get_events"

    @pytest.mark.asyncio
    async def test_get_events_span_attributes(self, traced_store, mock_tracer):
        """get_events span includes correct standard attributes."""
        async with traced_store:
            await traced_store.initialize()

            aggregate_id = uuid4()
            await traced_store.get_events(
                aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
                from_version=5,
            )

        # Verify correct attributes using standard constants
        call_args = mock_tracer.span.call_args
        attributes = call_args[0][1]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "SQLiteTracingTestAggregate"
        assert ATTR_FROM_VERSION in attributes
        assert attributes[ATTR_FROM_VERSION] == 5
        assert ATTR_DB_SYSTEM in attributes
        assert attributes[ATTR_DB_SYSTEM] == "sqlite"
        assert ATTR_DB_NAME in attributes
        assert attributes[ATTR_DB_NAME] == ":memory:"


# ============================================================================
# Tracing Disabled Tests
# ============================================================================


class TestSQLiteEventStoreTracingDisabled:
    """Tests for SQLiteEventStore behavior when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_append_events_works_without_tracing(self):
        """append_events works correctly when tracing is disabled."""
        registry = EventRegistry()
        registry.register(SQLiteTracingTestEvent)

        store = SQLiteEventStore(":memory:", registry, enable_tracing=False)

        async with store:
            await store.initialize()

            aggregate_id = uuid4()
            event = SQLiteTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Test",
            )

            result = await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
                events=[event],
                expected_version=0,
            )

            assert result.success is True
            assert result.new_version == 1

    @pytest.mark.asyncio
    async def test_get_events_works_without_tracing(self):
        """get_events works correctly when tracing is disabled."""
        registry = EventRegistry()
        registry.register(SQLiteTracingTestEvent)

        store = SQLiteEventStore(":memory:", registry, enable_tracing=False)

        async with store:
            await store.initialize()

            aggregate_id = uuid4()
            event = SQLiteTracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Test",
            )

            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
                events=[event],
                expected_version=0,
            )

            stream = await store.get_events(
                aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
            )

            assert len(stream.events) == 1
            assert stream.version == 1


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestSQLiteEventStoreStandardAttributes:
    """Tests for standard attribute usage in SQLiteEventStore."""

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in sqlite.py."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/stores/sqlite.py"],
            capture_output=True,
            text=True,
        )
        # Should be 0 - no local definition
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in sqlite.py"
        )

    def test_imports_from_observability_module(self):
        """Verify SQLiteEventStore imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/stores/sqlite.py",
            ],
            capture_output=True,
            text=True,
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "SQLiteEventStore should import from eventsource.observability"

    def test_uses_standard_attribute_constants(self):
        """Verify SQLiteEventStore uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/stores/sqlite.py"],
            capture_output=True,
            text=True,
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 5, f"Expected at least 5 ATTR_* usages, found {count}"


# ============================================================================
# Multiple Events Tests
# ============================================================================


class TestSQLiteEventStoreTracingMultipleEvents:
    """Tests for tracing with multiple events."""

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
    async def test_multiple_events_count_attribute(self, mock_tracer):
        """Event count attribute reflects actual number of events."""
        registry = EventRegistry()
        registry.register(SQLiteTracingTestEvent)

        store = SQLiteEventStore(":memory:", registry, enable_tracing=True)
        store._tracer = mock_tracer
        store._enable_tracing = True

        async with store:
            await store.initialize()

            aggregate_id = uuid4()
            events = [
                SQLiteTracingTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=i + 1,
                    name=f"Event {i}",
                )
                for i in range(3)
            ]

            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
                events=events,
                expected_version=0,
            )

        call_args = mock_tracer.span.call_args
        attributes = call_args[0][1]

        assert attributes[ATTR_EVENT_COUNT] == 3
        assert "SQLiteTracingTestEvent" in attributes[ATTR_EVENT_TYPE]

    @pytest.mark.asyncio
    async def test_empty_events_no_span(self, mock_tracer):
        """No span created when events list is empty."""
        registry = EventRegistry()

        store = SQLiteEventStore(":memory:", registry, enable_tracing=True)
        store._tracer = mock_tracer
        store._enable_tracing = True

        async with store:
            await store.initialize()

            aggregate_id = uuid4()
            result = await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTracingTestAggregate",
                events=[],  # Empty list
                expected_version=0,
            )

            assert result.success is True

        # No span should be created for empty events
        mock_tracer.span.assert_not_called()
