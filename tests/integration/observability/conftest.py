"""Shared pytest fixtures for observability integration tests."""

from __future__ import annotations

from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest

if TYPE_CHECKING:
    pass


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for observability tests."""
    config.addinivalue_line("markers", "benchmark: marks tests as performance benchmarks")


_test_provider = None
_test_processor = None


@pytest.fixture(scope="session", autouse=True)
def setup_test_tracing() -> Generator[Any]:
    """Set up a global TracerProvider for all tests at session scope."""
    global _test_provider, _test_processor

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    current_provider = trace.get_tracer_provider()
    if current_provider.__class__.__name__ == "ProxyTracerProvider":
        _test_provider = TracerProvider()
        trace.set_tracer_provider(_test_provider)
    else:
        _test_provider = current_provider

    yield _test_provider


@pytest.fixture(scope="function")
def trace_exporter(setup_test_tracing: Any) -> Generator[Any]:
    """Create an in-memory span exporter for testing."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    if isinstance(_test_provider, TracerProvider):
        processor = SimpleSpanProcessor(exporter)
        _test_provider.add_span_processor(processor)

    yield exporter
    exporter.clear()


@pytest.fixture(scope="function")
def trace_provider(trace_exporter: Any) -> Generator[Any]:
    """Provide the TracerProvider for testing."""
    yield _test_provider


@pytest.fixture(scope="function")
def get_spans(trace_exporter: Any) -> Callable[[], list[Any]]:
    """Helper fixture to retrieve finished spans from the exporter."""

    def _get_spans() -> list[Any]:
        return trace_exporter.get_finished_spans()

    return _get_spans


@pytest.fixture(scope="function")
def find_span(get_spans: Callable[[], list[Any]]) -> Callable[[str], Any | None]:
    """Helper fixture to find a span by name substring."""

    def _find_span(name_contains: str) -> Any | None:
        return next((s for s in get_spans() if name_contains in s.name), None)

    return _find_span


@pytest.fixture(scope="function")
def find_spans(get_spans: Callable[[], list[Any]]) -> Callable[[str], list[Any]]:
    """Helper fixture to find all spans matching a name substring."""

    def _find_spans(name_contains: str) -> list[Any]:
        return [s for s in get_spans() if name_contains in s.name]

    return _find_spans


from eventsource import DomainEvent, register_event  # noqa: E402


@register_event
class TracingTestEvent(DomainEvent):
    """Test event for tracing integration tests."""

    aggregate_type: str = "TracingTestAggregate"
    name: str
    value: int = 0


@register_event
class TracingTestEventCreated(DomainEvent):
    """Test event for aggregate creation in tracing tests."""

    aggregate_type: str = "TracingTestAggregate"
    name: str


@register_event
class TracingTestEventUpdated(DomainEvent):
    """Test event for aggregate updates in tracing tests."""

    aggregate_type: str = "TracingTestAggregate"
    value: int


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
