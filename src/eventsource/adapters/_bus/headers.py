"""Decode-safe readers for message header values.

Header values are set by whoever publishes to the broker, so they are
untrusted input. Both the RabbitMQ and Kafka consumers read them before their
per-message error boundary. A value that raises on read would stop the consume
loop, so these readers never raise on bad data:

- Bytes are decoded as UTF-8 with ``errors="replace"``.
- Integers that do not parse return the caller's default.
- Counts are clamped to zero or more.
"""

from __future__ import annotations

from typing import Any


def read_header_str(value: Any, default: str | None = None) -> str | None:
    """Return a header value as text.

    Args:
        value: Raw header value. Usually ``bytes`` (Kafka) or ``str``/``int``
            (RabbitMQ). ``None`` means the header is absent.
        default: Returned when ``value`` is ``None``.

    Returns:
        The decoded text. Undecodable bytes become U+FFFD replacement characters.
    """
    if value is None:
        return default
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def read_header_int(value: Any, default: int = 0) -> int:
    """Return a header value as a non-negative integer.

    Args:
        value: Raw header value. Missing, non-integer, and non-UTF-8 values
            return ``default``.
        default: Fallback for missing or unparseable values.

    Returns:
        The parsed integer, clamped to zero or more.
    """
    text = read_header_str(value)
    if text is None:
        return default
    try:
        return max(0, int(text.strip()))
    except ValueError:
        return default


__all__ = ["read_header_int", "read_header_str"]
