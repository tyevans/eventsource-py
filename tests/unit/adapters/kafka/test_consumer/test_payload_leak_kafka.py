"""Payload values stay out of Kafka consumer logs and DLQ headers.

The message value has the marker in ``aggregate_id``, which fails UUID
validation. The consumer must dead-letter it without logging or header-copying
pydantic's message, which would echo the marker.
"""

from __future__ import annotations

import json
import logging

import pytest

from ._fixtures import _make_event, _make_loop, _make_message

pytest.importorskip("aiokafka", reason="aiokafka not installed")

MARKER = "victim-secret@example.com"


def _bad_body() -> bytes:
    return json.dumps(
        {"aggregate_type": "SampleAggregate", "aggregate_id": MARKER, "payload": "x"}
    ).encode()


def _assert_marker_absent_from_logs(caplog: pytest.LogCaptureFixture) -> None:
    assert caplog.records, "expected log records from the failure path"
    for record in caplog.records:
        assert MARKER not in record.getMessage(), record.getMessage()
        assert MARKER not in repr(record.args)
        assert MARKER not in repr(record.__dict__)


@pytest.mark.asyncio
async def test_undecodable_message_keeps_marker_out_of_logs_and_dlq_headers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    loop, producer, _consumer = _make_loop(max_retries=0)
    message = _make_message(_make_event())
    message.value = _bad_body()

    await loop._process_message(message)

    assert any("sending directly to DLQ" in r.getMessage() for r in caplog.records)
    _assert_marker_absent_from_logs(caplog)
    producer.send.assert_awaited_once()
    headers = dict(producer.send.await_args.kwargs["headers"])
    assert MARKER not in repr(headers)
    assert headers["dlq_error_type"] == b"DeserializationError"
    assert headers["dlq_error_message"].startswith(b"DeserializationError: ")
    assert b"('aggregate_id',)" in headers["dlq_error_message"]


@pytest.mark.asyncio
async def test_unknown_event_type_header_is_not_logged_as_message_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    loop, _producer, _consumer = _make_loop(event_class=None, max_retries=0)
    message = _make_message(_make_event())
    forged = "Forged\nFAKE-LEVEL victim-secret@example.com"
    message.headers = [("event_type", forged.encode())]

    await loop._process_message(message)

    for record in caplog.records:
        assert forged not in record.getMessage()
        assert "\nFAKE-LEVEL" not in record.getMessage()
