"""Helper utilities for Kafka consumer processing and DLQ header creation.

Extracted from ``KafkaConsumerLoop`` to keep module line count within limits
and isolate stateless header/logging helpers.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any


def get_header_value(
    headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None,
    key: str,
) -> str | None:
    """Get a string header value by key from Kafka headers."""
    if not headers:
        return None
    for k, v in headers:
        if k == key:
            if v is None:
                return None
            return v.decode("utf-8")
    return None


def get_retry_delay_remaining(
    headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None,
) -> float:
    """Calculate remaining retry delay from retry_after header, if any."""
    val = get_header_value(headers, "retry_after")
    if not val:
        return 0.0
    try:
        remaining = float(val) - datetime.now(UTC).timestamp()
        return max(0.0, remaining)
    except ValueError:
        return 0.0


def create_dlq_headers(
    message: Any,
    error: Exception,
    retry_count: int,
    reason: str,
    consumer_group: str,
) -> list[tuple[str, bytes]]:
    """Create DLQ-specific headers with failure metadata.

    Args:
        message: The failed Kafka message.
        error: The exception that caused the failure.
        retry_count: Number of retry attempts.
        reason: Reason for DLQ routing.
        consumer_group: Name of the consumer group.

    Returns:
        List of DLQ header tuples.
    """
    error_message = str(error)[:1000]
    return [
        ("dlq_reason", reason.encode("utf-8")),
        ("dlq_error_type", type(error).__name__.encode("utf-8")),
        ("dlq_error_message", error_message.encode("utf-8")),
        ("dlq_retry_count", str(retry_count).encode("utf-8")),
        ("dlq_timestamp", datetime.now(UTC).isoformat().encode("utf-8")),
        ("dlq_original_topic", message.topic.encode("utf-8")),
        ("dlq_original_partition", str(message.partition).encode("utf-8")),
        ("dlq_original_offset", str(message.offset).encode("utf-8")),
        ("dlq_consumer_group", consumer_group.encode("utf-8")),
    ]


def warn_uncommitted(
    logger: logging.Logger,
    message: Any,
    stage: str,
    event_id: str | None,
) -> None:
    """Log that a Kafka offset was deliberately left uncommitted."""
    logger.critical(
        "Offset left uncommitted: message was neither retried nor sent to the DLQ. "
        "It will be redelivered.",
        extra={
            "stage": stage,
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "event_id": event_id,
        },
    )
