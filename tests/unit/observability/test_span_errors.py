"""record_error keeps payload values and stacktraces out of exported span events."""

from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from pydantic import BaseModel, ValidationError

from eventsource.observability.span_errors import record_error

MARKER = "victim-secret@example.com"


class _Account(BaseModel):
    owner_id: UUID


def _validation_error() -> ValidationError:
    try:
        _Account.model_validate({"owner_id": MARKER})
    except ValidationError as exc:
        return exc
    raise AssertionError("expected a ValidationError")


@pytest.fixture
def exporter() -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    """A private TracerProvider, so the process-global provider is untouched."""
    provider = TracerProvider()
    spans = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    yield provider, spans
    provider.shutdown()


def test_record_error_writes_summary_event_and_error_status(
    exporter: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    provider, spans = exporter
    span = provider.get_tracer("test").start_span("op")

    record_error(span, _validation_error())
    span.end()

    [finished] = spans.get_finished_spans()
    [event] = finished.events
    assert event.name == "exception"
    assert event.attributes is not None
    assert event.attributes["exception.type"] == "ValidationError"
    summary = str(event.attributes["exception.message"])
    assert summary.startswith("ValidationError(")
    assert "('owner_id',)" in summary
    assert MARKER not in str(event.attributes)
    assert "exception.stacktrace" not in event.attributes
    assert finished.status.status_code is StatusCode.ERROR
    assert finished.status.description == summary


def test_record_error_with_no_span_does_nothing() -> None:
    record_error(None, _validation_error())
