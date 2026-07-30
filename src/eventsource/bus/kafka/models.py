"""Data classes and exceptions used by the Kafka event bus."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


class KafkaNotAvailableError(ImportError):
    """Raised when aiokafka package is not installed.

    This exception is raised when attempting to use Kafka functionality
    without having the aiokafka package installed. The error message includes
    the installation command to help users resolve the issue.

    Example:
        >>> from eventsource.bus.kafka import KafkaEventBus
        >>> bus = KafkaEventBus()  # Raises if aiokafka not installed
        KafkaNotAvailableError: aiokafka package is not installed. ...
    """

    def __init__(self) -> None:
        """Initialize the error with a helpful installation message."""
        super().__init__(
            "aiokafka package is not installed. Install it with: pip install eventsource[kafka]"
        )


class DeserializationError(ValueError):
    """Raised when a message cannot be deserialized.

    This exception is raised when a Kafka message cannot be deserialized
    into a valid DomainEvent. These errors are unrecoverable and the message
    should be sent directly to the DLQ without retry attempts.

    Attributes:
        message: Description of the deserialization failure.
    """

    pass


@dataclass
class KafkaEventBusStats:
    """Statistics for Kafka event bus operations.

    Tracks operational metrics for monitoring and debugging. Statistics are
    updated atomically during publish and consume operations.

    Attributes:
        events_published: Total number of events successfully published.
        events_consumed: Total number of events consumed from Kafka.
        events_processed_success: Events successfully processed by handlers.
        events_processed_failed: Events that failed handler processing.
        messages_sent_to_dlq: Events sent to dead letter queue.
        handler_errors: Total handler exceptions caught.
        reconnections: Number of reconnection attempts.
        rebalance_count: Number of consumer group rebalances.
        last_publish_at: Timestamp of last successful publish.
        last_consume_at: Timestamp of last successful consume.
        last_error_at: Timestamp of last error.
        connected_at: Timestamp when connection was established.
    """

    # Counters
    events_published: int = 0
    events_consumed: int = 0
    events_processed_success: int = 0
    events_processed_failed: int = 0
    messages_sent_to_dlq: int = 0
    handler_errors: int = 0
    reconnections: int = 0

    # Kafka-specific
    rebalance_count: int = 0

    # Timing
    last_publish_at: datetime | None = None
    last_consume_at: datetime | None = None
    last_error_at: datetime | None = None
    connected_at: datetime | None = None

    def get_stats_dict(self) -> dict[str, Any]:
        """Return statistics as a JSON-serializable dictionary.

        Returns:
            Dictionary with all statistics, datetimes converted to ISO format.
        """
        return {
            "events_published": self.events_published,
            "events_consumed": self.events_consumed,
            "events_processed_success": self.events_processed_success,
            "events_processed_failed": self.events_processed_failed,
            "messages_sent_to_dlq": self.messages_sent_to_dlq,
            "handler_errors": self.handler_errors,
            "reconnections": self.reconnections,
            "rebalance_count": self.rebalance_count,
            "last_publish_at": (self.last_publish_at.isoformat() if self.last_publish_at else None),
            "last_consume_at": (self.last_consume_at.isoformat() if self.last_consume_at else None),
            "last_error_at": (self.last_error_at.isoformat() if self.last_error_at else None),
            "connected_at": (self.connected_at.isoformat() if self.connected_at else None),
        }
