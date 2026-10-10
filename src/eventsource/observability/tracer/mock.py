"""Mock tracer for testing."""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import Any

from eventsource.observability.tracer.protocol import SpanKindEnum


class MockTracer:
    """
    Mock tracer for testing that records span information.

    This tracer is useful for testing components that use tracing,
    allowing you to verify that spans are created with the expected
    names and attributes.

    Example:
        >>> tracer = MockTracer()
        >>> with tracer.span("operation", {"key": "value"}):
        ...     pass
        >>> assert tracer.spans == [("operation", {"key": "value"})]
        >>> assert tracer.span_names == ["operation"]
    """

    def __init__(self) -> None:
        """Initialize MockTracer with empty span list."""
        self.spans: list[tuple[str, dict[str, Any] | None]] = []

    @contextlib.contextmanager
    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> Generator[None]:
        """Record span and yield None."""
        self.spans.append((name, attributes))
        yield None

    @property
    def enabled(self) -> bool:
        """Returns True to enable attribute computation in tests."""
        return True

    @property
    def span_names(self) -> list[str]:
        """Get just the span names for easy assertions."""
        return [name for name, _ in self.spans]

    def clear(self) -> None:
        """Clear recorded spans."""
        self.spans.clear()

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> None:
        """Record span and return None (mock spans don't need to be ended)."""
        self.spans.append((name, attributes))
        return None

    @contextlib.contextmanager
    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Generator[None]:
        """Record span with kind and yield None."""
        self.spans.append((name, attributes))
        yield None


__all__ = ["MockTracer"]
