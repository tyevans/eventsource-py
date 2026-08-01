"""Data classes and exceptions used by the RabbitMQ event bus."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


class RabbitMQNotAvailableError(ImportError):
    """Raised when aio-pika package is not installed.

    This exception is raised when attempting to use RabbitMQ functionality
    without having the aio-pika package installed. The error message includes
    the installation command to help users resolve the issue.

    Example:
        >>> from eventsource.adapters.rabbitmq import RabbitMQEventBus
        >>> bus = RabbitMQEventBus()  # Raises if aio-pika not installed
        RabbitMQNotAvailableError: aio-pika package is not installed. ...
    """

    def __init__(self) -> None:
        """Initialize the error with a helpful installation message."""
        super().__init__(
            "aio-pika package is not installed. Install it with: pip install eventsource[rabbitmq]"
        )


@dataclass
class DLQMessage:
    """Dataclass representing a message from the dead letter queue.

    This class holds information about messages that have been moved to the
    dead letter queue after failing processing. It includes both the original
    message content and metadata about why and when it was dead-lettered.

    Attributes:
        message_id: Unique identifier for the message (from RabbitMQ).
        routing_key: The routing key used when the message was originally published.
        body: The message body as a string (typically JSON).
        headers: All message headers as a dictionary.
        event_type: The type of the event (extracted from headers).
        dlq_reason: The reason the message was sent to DLQ (from x-dlq-reason header).
        dlq_error_type: The type of error that caused the failure (from x-dlq-error-type).
        dlq_retry_count: Number of retries before being sent to DLQ (from x-dlq-retry-count).
        dlq_timestamp: When the message was sent to DLQ (from x-dlq-timestamp).
        original_routing_key: The original routing key before dead-lettering
            (from x-original-routing-key).

    Example:
        >>> dlq_messages = await bus.get_dlq_messages(limit=10)
        >>> for msg in dlq_messages:
        ...     print(f"Message {msg.message_id}: {msg.event_type}")
        ...     print(f"  Failed due to: {msg.dlq_reason}")
        ...     print(f"  Retries: {msg.dlq_retry_count}")
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
    """Statistics for RabbitMQ event bus operations.

    This dataclass tracks operational metrics for monitoring and observability
    of the RabbitMQ event bus. It follows the same patterns as RedisEventBusStats
    for consistency across the eventsource library.

    Attributes:
        events_published: Total number of events successfully published to the exchange.
            Incremented after each successful publish operation.
        events_consumed: Total number of events consumed from the queue.
            Incremented when a message is received from RabbitMQ.
        events_processed_success: Total number of events that were processed
            successfully by their handlers without errors.
        events_processed_failed: Total number of events that failed during
            handler processing (handler raised an exception).
        messages_sent_to_dlq: Total number of messages moved to the dead letter
            queue after exceeding max retries.
        handler_errors: Total number of handler execution errors. This may differ
            from events_processed_failed if a single event is retried multiple times.
        reconnections: Number of reconnection attempts made after connection loss.
        publish_confirms: Number of publisher confirms received from RabbitMQ.
            Only applicable when publisher confirms are enabled.
        publish_returns: Number of unroutable messages returned by RabbitMQ.
            Occurs when a message cannot be routed to any queue.
        batch_publishes: Number of batch publish operations performed.
            Each call to publish_batch() or publish() with multiple events counts as one.
        batch_events_published: Total events published through batch operations.
            This is a subset of events_published that tracks batch-specific throughput.
        batch_partial_failures: Number of batch operations that had some failures
            but were not complete failures. Useful for tracking partial success scenarios.
        last_publish_at: Timestamp of the last successful publish operation.
            None if no events have been published yet.
        last_consume_at: Timestamp of the last successful consume operation.
            None if no events have been consumed yet.
        last_error_at: Timestamp of the last error (handler error or failed processing).
            None if no errors have occurred.
        connected_at: Timestamp when the connection was established.
            None if not connected. Used for uptime calculation.

    Example:
        >>> stats = RabbitMQEventBusStats()
        >>> stats.events_published = 100
        >>> stats.events_consumed = 95
        >>> print(f"Published: {stats.events_published}, Consumed: {stats.events_consumed}")
        Published: 100, Consumed: 95

    Note:
        Thread-safety of stats updates is handled by the event bus implementation,
        not this dataclass. This is a simple data container.
    """

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
    """Information about a RabbitMQ queue.

    This dataclass provides operational information about a queue including
    message count, consumer count, and queue state. Used for monitoring
    and health checking.

    Attributes:
        name: Name of the queue.
        message_count: Number of messages currently in the queue.
        consumer_count: Number of consumers currently attached to the queue.
        state: Current state of the queue. Possible values:
            - "running": Queue is operational
            - "idle": Queue exists but has no consumers
            - "unknown": Queue state cannot be determined
            - "error": An error occurred while querying queue state
        error: Error message if state is "error", None otherwise.

    Example:
        >>> info = await bus.get_queue_info()
        >>> print(f"Queue {info.name}: {info.message_count} messages")
        >>> if info.consumer_count == 0:
        ...     print("Warning: No consumers attached")
    """

    name: str
    message_count: int
    consumer_count: int
    state: str = "running"
    error: str | None = None


@dataclass
class HealthCheckResult:
    """Result of a health check on the RabbitMQ event bus.

    This dataclass provides comprehensive health status information
    for monitoring and alerting purposes.

    Attributes:
        healthy: Overall health status. True if all components are operational,
            False if any component is unhealthy.
        connection_status: Status of the RabbitMQ connection.
            - "connected": Connection is established and open
            - "disconnected": Not connected to RabbitMQ
            - "closed": Connection was closed
        channel_status: Status of the AMQP channel.
            - "open": Channel is open and operational
            - "closed": Channel is closed
            - "not_initialized": Channel was never created
        queue_status: Status of the consumer queue.
            - "accessible": Queue can be accessed and declared
            - "inaccessible": Queue cannot be accessed
            - "not_initialized": Queue was never declared
            - "error: <message>": An error occurred checking the queue
        dlq_status: Status of the dead letter queue (if DLQ is enabled).
            - "accessible": DLQ can be accessed
            - "inaccessible": DLQ cannot be accessed
            - "disabled": DLQ is not enabled
            - "error: <message>": An error occurred checking the DLQ
            - None if DLQ check was not performed
        error: Error message if health check failed, None otherwise.
        details: Additional details about the health check including
            configuration information and consuming state.

    Example:
        >>> result = await bus.health_check()
        >>> if not result.healthy:
        ...     print(f"Unhealthy: {result.error}")
        ...     print(f"Connection: {result.connection_status}")
        ...     print(f"Channel: {result.channel_status}")
    """

    healthy: bool
    connection_status: str
    channel_status: str
    queue_status: str
    dlq_status: str | None = None
    error: str | None = None
    details: dict[str, Any] | None = None


class ShutdownError(Exception):
    """Raised when an operation is attempted after shutdown.

    This exception is raised when attempting to publish events or start
    consuming after the event bus has been shut down. The event bus cannot
    be reused after shutdown - a new instance must be created.

    Example:
        >>> await bus.shutdown()
        >>> await bus.publish([event])  # Raises ShutdownError
    """

    def __init__(self, message: str = "Event bus has been shut down") -> None:
        """Initialize the error with a message.

        Args:
            message: The error message describing the situation.
        """
        super().__init__(message)


class BatchPublishError(Exception):
    """Raised when a batch publish operation has failures.

    This exception is raised when one or more events in a batch fail to publish.
    It contains information about both successful and failed publishes, allowing
    the caller to handle partial failures appropriately.

    Attributes:
        results: Dictionary containing batch statistics:
            - total: Total number of events in the batch
            - published: Number of events successfully published
            - failed: Number of events that failed to publish
            - chunks: Number of chunks the batch was split into
        errors: List of exceptions from failed publish operations

    Example:
        >>> try:
        ...     result = await bus.publish_batch(events)
        ... except BatchPublishError as e:
        ...     print(f"Partial failure: {e.results['published']}/{e.results['total']} published")
        ...     for err in e.errors:
        ...         print(f"  Error: {err}")
    """

    def __init__(
        self,
        message: str,
        results: dict[str, int] | None = None,
        errors: list[Exception] | None = None,
    ) -> None:
        """Initialize the error with message and details.

        Args:
            message: The error message describing the failure
            results: Dictionary with batch statistics
            errors: List of exceptions from failed operations
        """
        super().__init__(message)
        self.results = results or {"total": 0, "published": 0, "failed": 0, "chunks": 0}
        self.errors = errors or []
