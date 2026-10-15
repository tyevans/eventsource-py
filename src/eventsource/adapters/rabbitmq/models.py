"""Data classes and exceptions used by the RabbitMQ event bus."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from eventsource.domain.exceptions import EventSourceError


class RabbitMQNotAvailableError(EventSourceError, ImportError):
    """Raised when aio-pika package is not installed."""

    def __init__(self) -> None:
        """Initialize the error with a helpful installation message."""
        super().__init__(
            "aio-pika package is not installed. Install it with: pip install eventsource[rabbitmq]"
        )


@dataclass
class DLQMessage:
    """Dataclass representing a message from the dead letter queue.

    Holds original message content and metadata about why and when it was dead-lettered.
    """

    message_id: str | None
    routing_key: str | None
    body: str
    headers: dict[str, Any]
    event_type: str | None = None
    dlq_reason: str | None = None
    dlq_error_type: str | None = None
    dlq_retry_count: int | None = None
    dlq_timestamp: str | None = None
    original_routing_key: str | None = None


@dataclass
class RabbitMQEventBusStats:
    """Statistics for RabbitMQ event bus operations."""

    # Counters
    events_published: int = 0
    events_consumed: int = 0
    events_processed_success: int = 0
    events_processed_failed: int = 0
    messages_sent_to_dlq: int = 0
    handler_errors: int = 0
    reconnections: int = 0
    publish_confirms: int = 0
    publish_returns: int = 0

    # Batch publishing counters
    batch_publishes: int = 0
    batch_events_published: int = 0
    batch_partial_failures: int = 0

    # Timing
    last_publish_at: datetime | None = None
    last_consume_at: datetime | None = None
    last_error_at: datetime | None = None
    connected_at: datetime | None = None


@dataclass
class QueueInfo:
    """Operational information about a RabbitMQ queue."""

    name: str
    message_count: int
    consumer_count: int
    state: str = "running"
    error: str | None = None


@dataclass
class HealthCheckResult:
    """Result of a health check on the RabbitMQ event bus."""

    healthy: bool
    connection_status: str
    channel_status: str
    queue_status: str
    dlq_status: str | None = None
    error: str | None = None
    details: dict[str, Any] | None = None


class ShutdownError(EventSourceError):
    """Raised when an operation is attempted after shutdown."""

    def __init__(self, message: str = "Event bus has been shut down") -> None:
        """Initialize the error with a message."""
        super().__init__(message)


class BatchPublishError(EventSourceError):
    """Raised when one or more events in a batch publish operation fail."""

    def __init__(
        self,
        message: str,
        results: dict[str, int] | None = None,
        errors: list[Exception] | None = None,
    ) -> None:
        """Initialize the error with message, batch statistics, and exceptions."""
        super().__init__(message)
        self.results = results or {"total": 0, "published": 0, "failed": 0, "chunks": 0}
        self.errors = errors or []
