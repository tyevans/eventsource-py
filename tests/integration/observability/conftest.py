"""
Shared pytest fixtures for observability integration tests.

This module provides fixtures for OpenTelemetry testing infrastructure
using an in-memory span exporter for span inspection.
"""

from __future__ import annotations

from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest

if TYPE_CHECKING:
    pass

# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for observability tests."""
    config.addinivalue_line("markers", "benchmark: marks tests as performance benchmarks")


# ============================================================================
# OpenTelemetry Fixtures
# ============================================================================

# Module-level storage for the global test provider
_test_provider = None
_test_processor = None


@pytest.fixture(scope="session", autouse=True)
def setup_test_tracing() -> Generator[Any, None, None]:
    """
    Set up a global TracerProvider for all tests at session scope.

    This sets up OpenTelemetry once at the start of the test session.
    Individual tests use the trace_exporter fixture to capture spans.
    """
    global _test_provider, _test_processor

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    # Only set up if not already configured (avoid re-configuration errors)
    current_provider = trace.get_tracer_provider()

    # Check if current provider is a proxy (not yet configured)
    if current_provider.__class__.__name__ == "ProxyTracerProvider":
        _test_provider = TracerProvider()
        trace.set_tracer_provider(_test_provider)
    else:
        # Already configured, use existing provider if it's a TracerProvider
        if isinstance(current_provider, TracerProvider):
            _test_provider = current_provider
        else:
            # Use whatever is there
            _test_provider = current_provider

    yield _test_provider


@pytest.fixture(scope="function")
def trace_exporter(setup_test_tracing: Any) -> Generator[Any, None, None]:
    """
    Create an in-memory span exporter for testing.

    This exporter captures all finished spans in memory, allowing
    tests to inspect span creation, attributes, and relationships.

    The exporter is dynamically added to the provider and removed after the test.

    Yields:
        InMemorySpanExporter instance with captured spans

    Example:
        >>> def test_span_created(trace_exporter, trace_provider):
        ...     # Perform operations that should create spans
        ...     spans = trace_exporter.get_finished_spans()
        ...     assert len(spans) > 0
    """
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()

    # Add processor to the existing provider if possible
    if isinstance(_test_provider, TracerProvider):
        processor = SimpleSpanProcessor(exporter)
        _test_provider.add_span_processor(processor)

    yield exporter

    exporter.clear()
    # Note: We can't easily remove processors, but we clear the exporter


@pytest.fixture(scope="function")
def trace_provider(trace_exporter: Any) -> Generator[Any, None, None]:
    """
    Provide the TracerProvider for testing.

    This fixture ensures the trace_exporter is set up and provides
    access to the provider.

    Args:
        trace_exporter: The in-memory exporter fixture

    Yields:
        Configured TracerProvider

    Example:
        >>> def test_with_provider(trace_provider, trace_exporter):
        ...     from opentelemetry import trace
        ...     tracer = trace.get_tracer("test")
        ...     with tracer.start_as_current_span("test-span"):
        ...         pass
        ...     spans = trace_exporter.get_finished_spans()
        ...     assert len(spans) == 1
    """
    yield _test_provider


@pytest.fixture(scope="function")
def get_spans(trace_exporter: Any) -> Callable[[], list[Any]]:
    """
    Helper fixture to retrieve finished spans from the exporter.

    Returns:
        Callable that returns list of finished spans

    Example:
        >>> def test_spans(trace_provider, get_spans):
        ...     # ... operations that create spans ...
        ...     spans = get_spans()
        ...     assert len(spans) > 0
    """

    def _get_spans() -> list[Any]:
        return trace_exporter.get_finished_spans()

    return _get_spans


@pytest.fixture(scope="function")
def find_span(get_spans: Callable[[], list[Any]]) -> Callable[[str], Any | None]:
    """
    Helper fixture to find a span by name substring.

    Args:
        get_spans: The get_spans fixture

    Returns:
        Callable that finds a span by name substring, or None if not found

    Example:
        >>> def test_find_span(trace_provider, find_span):
        ...     # ... operations that create spans ...
        ...     span = find_span("event_store.append")
        ...     assert span is not None
    """

    def _find_span(name_contains: str) -> Any | None:
        spans = get_spans()
        return next((s for s in spans if name_contains in s.name), None)

    return _find_span


@pytest.fixture(scope="function")
def find_spans(get_spans: Callable[[], list[Any]]) -> Callable[[str], list[Any]]:
    """
    Helper fixture to find all spans matching a name substring.

    Args:
        get_spans: The get_spans fixture

    Returns:
        Callable that returns all spans matching the name substring

    Example:
        >>> def test_find_spans(trace_provider, find_spans):
        ...     # ... operations that create multiple handler spans ...
        ...     handler_spans = find_spans("handle")
        ...     assert len(handler_spans) == 3
    """

    def _find_spans(name_contains: str) -> list[Any]:
        spans = get_spans()
        return [s for s in spans if name_contains in s.name]

    return _find_spans


# ============================================================================
# Test Events and Aggregates
# ============================================================================


from eventsource import DomainEvent, register_event  # noqa: E402


@register_event
class TracingTestEvent(DomainEvent):
    """Test event for tracing integration tests."""

    event_type: str = "TracingTestEvent"
    aggregate_type: str = "TracingTestAggregate"
    name: str
    value: int = 0


@register_event
class TracingTestEventCreated(DomainEvent):
    """Test event for aggregate creation in tracing tests."""

    event_type: str = "TracingTestEventCreated"
    aggregate_type: str = "TracingTestAggregate"
    name: str


@register_event
class TracingTestEventUpdated(DomainEvent):
    """Test event for aggregate updates in tracing tests."""

    event_type: str = "TracingTestEventUpdated"
    aggregate_type: str = "TracingTestAggregate"
    value: int


# ============================================================================
# Sample Data Fixtures
# ============================================================================


@pytest.fixture
def sample_aggregate_id() -> UUID:
    """Provide a sample aggregate ID."""
    return uuid4()


@pytest.fixture
def sample_event(sample_aggregate_id: UUID) -> TracingTestEvent:
    """Provide a sample test event."""
    return TracingTestEvent(
        aggregate_id=sample_aggregate_id,
        aggregate_version=1,
        name="Test Event",
        value=42,
    )


@pytest.fixture
def sample_events(sample_aggregate_id: UUID) -> list[TracingTestEvent]:
    """Provide multiple sample test events."""
    return [
        TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=i + 1,
            name=f"Test Event {i}",
            value=i * 10,
        )
        for i in range(5)
    ]
