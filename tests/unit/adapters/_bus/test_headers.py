"""Decode-safe header readers shared by the RabbitMQ and Kafka consumers."""

from __future__ import annotations

import pytest

from eventsource.adapters._bus.headers import read_header_int, read_header_str


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (b"order.created", "order.created"),
        (b"\xff\xfe", "��"),
        ("text", "text"),
        (42, "42"),
    ],
)
def test_read_header_str_decodes_without_raising(value: object, expected: str | None) -> None:
    assert read_header_str(value) == expected


def test_read_header_str_returns_default_only_for_missing_values() -> None:
    assert read_header_str(None, "unknown") == "unknown"
    assert read_header_str(b"\xff", "unknown") == "�"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, 0),
        (b"3", 3),
        (b" 7 ", 7),
        (5, 5),
        (b"abc", 0),
        (b"\xff", 0),
        (b"-1", 0),
        (-9, 0),
        (b"1.5", 0),
        (b"9" * 5000, 0),
    ],
)
def test_read_header_int_parses_clamps_and_falls_back(value: object, expected: int) -> None:
    assert read_header_int(value) == expected


def test_read_header_int_uses_caller_default_for_unparseable_values() -> None:
    assert read_header_int(b"abc", 9) == 9
    assert read_header_int(None, 9) == 9
