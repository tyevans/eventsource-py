"""Record an exception on a span without writing raw exception text into it.

``span.record_exception`` writes ``str(exc)`` and the full traceback into the
span event. Pydantic and SQLAlchemy messages can carry event payload values, so
``record_error`` writes only the ``error_summary`` text and no stacktrace.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from eventsource.observability.error_summary import error_summary

if TYPE_CHECKING:
    from opentelemetry.trace import Span


def record_error(span: Span | None, error: BaseException) -> None:
    """Add a log-safe ``exception`` event and an ERROR status to ``span``.

    The event carries ``exception.type`` (the class qualname) and
    ``exception.message`` (the ``error_summary``). It has no stacktrace.
    A ``None`` span, as the no-op tracer yields, does nothing. The OpenTelemetry
    API is imported only when a span exists, so this module does not need it.

    Args:
        span: The span to record on, or ``None`` when tracing is disabled.
        error: The exception to record.
    """
    if span is None:
        return
    from opentelemetry.trace import Status, StatusCode

    summary = error_summary(error)
    span.add_event(
        "exception",
        {"exception.type": type(error).__qualname__, "exception.message": summary},
    )
    span.set_status(Status(StatusCode.ERROR, summary))


__all__ = ["record_error"]
