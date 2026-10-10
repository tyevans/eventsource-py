"""No-op tracer implementation for when tracing is disabled."""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import Any

from eventsource.observability.tracer.protocol import SpanKindEnum


class NullTracer:
    """
    No-op tracer implementation for when tracing is disabled.

    This tracer creates no spans and has minimal overhead. Use it:
    - When tracing is explicitly disabled
    - In unit tests to avoid tracing side effects
    - As a default when no tracer is provided

    Example:
        >>> tracer = NullTracer()
        >>> with tracer.span("operation"):  # Does nothing
        ...     do_work()
        >>> tracer.enabled  # False
    """

    @contextlib.contextmanager
    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> Generator[None]:
        """Create a no-op span context (yields None)."""
        yield None

    @property
    def enabled(self) -> bool:
        """Always returns False for NullTracer."""
        return False

    def start_span(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> None:
        """Return None (no-op for disabled tracing)."""
        return None

    @contextlib.contextmanager
    def span_with_kind(
        self,
        name: str,
        kind: SpanKindEnum = SpanKindEnum.INTERNAL,
        attributes: dict[str, Any] | None = None,
        context: Any | None = None,
    ) -> Generator[None]:
        """Create a no-op span context with kind (yields None)."""
        yield None


__all__ = ["NullTracer"]
