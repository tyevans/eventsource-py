"""Configuration for the RabbitMQ event bus."""

from __future__ import annotations

import socket
import ssl
import uuid
from dataclasses import dataclass
from typing import Any


@dataclass
class RabbitMQEventBusConfig:
    """Configuration for RabbitMQ event bus.

    This configuration class provides all settings needed for connecting to
    RabbitMQ and managing message exchange and queue behavior. It follows
    the same patterns as RedisEventBusConfig for consistency across the
    eventsource library.

    Attributes:
        rabbitmq_url: RabbitMQ connection URL (amqp:// or amqps://).
            Format: amqp://user:password@host:port/vhost
        exchange_name: Name of the event exchange. Events are published to this
            exchange and routed to bound queues based on exchange type.
        exchange_type: Type of exchange. Supported types:
            - 'topic': Route messages based on routing key patterns (default)
            - 'direct': Route messages to queues with matching routing key
            - 'fanout': Broadcast messages to all bound queues
            - 'headers': Route based on message header attributes
        consumer_group: Name of the consumer group. Used in queue naming to
            allow multiple consumer groups to receive the same events.
        consumer_name: Unique name for this consumer instance. Auto-generated
            from hostname and UUID if not provided. Used for consumer tagging
            and debugging.
        prefetch_count: Maximum number of unacknowledged messages per consumer.
            Controls flow and prevents overwhelming slow consumers.
        max_retries: Maximum number of retry attempts before sending to DLQ.
            After this many failures, the message is moved to the dead letter queue.
        enable_dlq: Whether to enable dead letter queue. When True, messages that
            fail after max_retries are sent to a DLQ for manual inspection.
        dlq_exchange_suffix: Suffix appended to exchange_name for DLQ exchange name.
        dlq_message_ttl: Time-to-live in milliseconds for messages in the DLQ.
            If set, messages will be automatically removed from the DLQ after this time.
            If None (default), messages persist until manually removed.
        dlq_max_length: Maximum number of messages the DLQ can hold.
            If set, oldest messages are dropped when the limit is exceeded.
            If None (default), no limit is applied.
        durable: Whether exchanges and queues survive broker restarts.
            Should be True for production to ensure message durability.
        auto_delete: Whether to delete queues when all consumers disconnect.
            Should be False for production to prevent message loss.
        reconnect_delay: Fixed delay in seconds between reconnection attempts.
            Passed to aio-pika's ``connect_robust()`` as ``reconnect_interval``,
            which retries at this constant interval -- aio-pika does not back
            off, so there is no separate maximum-delay setting.
        heartbeat: Heartbeat interval in seconds. Used by RabbitMQ to detect
            dead connections and prevent firewall timeouts.
        enable_tracing: Enable OpenTelemetry tracing if available. When True,
            publishes and consumes are traced for observability.
        ssl_options: Additional SSL/TLS options passed to aio-pika connect.
            Used for advanced SSL configurations beyond the convenience fields.
        ssl_context: Pre-configured SSLContext to use for TLS connections.
            If provided, takes precedence over ssl_options and convenience fields.
            Allows full control over SSL/TLS configuration.
        verify_ssl: Whether to verify the server's SSL certificate. Default True.
            Setting to False disables certificate verification (NOT recommended
            for production). A warning is logged when verification is disabled.
        ca_file: Path to CA certificate file for verifying server certificates.
            Used when connecting to RabbitMQ with custom CA certificates.
        cert_file: Path to client certificate file for mutual TLS (mTLS).
            Must be used together with key_file for client certificate auth.
        key_file: Path to client private key file for mutual TLS (mTLS).
            Must be used together with cert_file for client certificate auth.
        retry_base_delay: Base delay in seconds for exponential backoff between
            retries. Actual delay is: retry_base_delay * (2 ** retry_count).
            Default is 1.0 second.
        retry_max_delay: Maximum delay in seconds between retries. Caps the
            exponential backoff to prevent excessively long waits.
            Default is 60.0 seconds.
        retry_jitter: Add random jitter to retry delays to prevent thundering
            herd. Value is 0.0 to 1.0 representing the fraction of delay to
            randomize. Default is 0.1 (10% jitter).
        shutdown_timeout: Default timeout in seconds for graceful shutdown.
            Used by the context manager and shutdown() method when no explicit
            timeout is provided. Default is 30.0 seconds.
        routing_key_pattern: Pattern for binding queues to exchanges. Behavior
            varies by exchange type:
            - topic: Supports wildcards (* for single word, # for zero or more words).
              Default is "#" (receive all messages).
            - direct: Requires exact match. Default is the queue_name.
            - fanout: Ignored (all bound queues receive all messages).
            - headers: Ignored (routing uses header matching).
            When None (default), the binding pattern is automatically chosen
            based on exchange type. Set explicitly to override.
        publish_chunk_size: Maximum number of events per chunk when publishing a
            batch. Large batches are split into chunks of this size to prevent
            overwhelming the broker; `max_concurrent_publishes` then bounds how
            many publishes within a chunk run concurrently. Default is 100.
            Unrelated to Kafka's `producer_max_batch_bytes` (a byte threshold)
            or Redis's `stream_read_count` (a consume-side read count) --
            this one counts events on the publish side.
        max_concurrent_publishes: Maximum number of concurrent publish operations
            within a batch chunk. Controls the level of parallelism when publishing
            batches. Default is 10.

    Example:
        >>> config = RabbitMQEventBusConfig(
        ...     rabbitmq_url="amqp://user:pass@rabbitmq.example.com:5672/",
        ...     exchange_name="myapp.events",
        ...     consumer_group="order-projection",
        ...     prefetch_count=20,
        ...     enable_dlq=True,
        ... )
        >>> print(config.queue_name)
        'myapp.events.order-projection'
        >>> print(config.dlq_exchange_name)
        'myapp.events_dlq'
    """

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    exchange_name: str = "events"
    exchange_type: str = "topic"  # topic, direct, fanout, headers
    consumer_group: str = "default"
    consumer_name: str | None = None
    prefetch_count: int = 10
    max_retries: int = 3
    enable_dlq: bool = True
    dlq_exchange_suffix: str = "_dlq"
    dlq_message_ttl: int | None = None
    dlq_max_length: int | None = None
    durable: bool = True
    auto_delete: bool = False
    reconnect_delay: float = 1.0
    heartbeat: int = 60
    enable_tracing: bool = True
    ssl_options: dict[str, Any] | None = None
    ssl_context: ssl.SSLContext | None = None
    verify_ssl: bool = True
    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    retry_jitter: float = 0.1
    shutdown_timeout: float = 30.0
    routing_key_pattern: str | None = None
    publish_chunk_size: int = 100
    max_concurrent_publishes: int = 10

    def __post_init__(self) -> None:
        """Generate consumer name if not provided.

        Creates a unique consumer name by combining the hostname with
        a truncated UUID to ensure uniqueness across distributed deployments.
        Format: {hostname}-{uuid[:8]}
        """
        if self.consumer_name is None:
            hostname = socket.gethostname()
            unique_id = str(uuid.uuid4())[:8]
            self.consumer_name = f"{hostname}-{unique_id}"

    @property
    def queue_name(self) -> str:
        """Get the consumer queue name.

        The queue name is derived from the exchange name and consumer group,
        following the pattern: {exchange_name}.{consumer_group}

        Returns:
            The queue name for this consumer group.

        Example:
            >>> config = RabbitMQEventBusConfig(
            ...     exchange_name="orders",
            ...     consumer_group="analytics"
            ... )
            >>> config.queue_name
            'orders.analytics'
        """
        return f"{self.exchange_name}.{self.consumer_group}"

    @property
    def dlq_exchange_name(self) -> str:
        """Get the dead letter exchange name.

        The DLQ exchange name is derived from the main exchange name
        with a configurable suffix appended.

        Returns:
            The dead letter exchange name.

        Example:
            >>> config = RabbitMQEventBusConfig(exchange_name="orders")
            >>> config.dlq_exchange_name
            'orders_dlq'
        """
        return f"{self.exchange_name}{self.dlq_exchange_suffix}"

    @property
    def dlq_queue_name(self) -> str:
        """Get the dead letter queue name.

        The DLQ queue name is derived from the main queue name
        with '.dlq' appended.

        Returns:
            The dead letter queue name.

        Example:
            >>> config = RabbitMQEventBusConfig(
            ...     exchange_name="orders",
            ...     consumer_group="analytics"
            ... )
            >>> config.dlq_queue_name
            'orders.analytics.dlq'
        """
        return f"{self.queue_name}.dlq"

    def get_effective_routing_key(self) -> str:
        """Get the effective routing key pattern for queue binding.

        If routing_key_pattern is explicitly set, use that. Otherwise,
        automatically determine the appropriate routing key based on
        exchange type:
        - topic: "#" (matches all routing keys)
        - direct: queue_name (exact match for work queue pattern)
        - fanout: "" (routing key is ignored for fanout)
        - headers: "" (routing uses header matching, not routing key)

        Returns:
            The routing key pattern to use for queue binding.

        Example:
            >>> config = RabbitMQEventBusConfig(
            ...     exchange_name="orders",
            ...     exchange_type="direct",
            ...     consumer_group="workers"
            ... )
            >>> config.get_effective_routing_key()
            'orders.workers'
        """
        # If explicitly set, use that value
        if self.routing_key_pattern is not None:
            return self.routing_key_pattern

        # Auto-determine based on exchange type
        exchange_type_lower = self.exchange_type.lower()

        if exchange_type_lower == "topic":
            # Topic exchange: use "#" to receive all messages
            return "#"
        elif exchange_type_lower == "direct":
            # Direct exchange: use queue_name for work queue pattern
            # This allows multiple consumers on the same queue to compete
            return self.queue_name
        elif exchange_type_lower in ("fanout", "headers"):
            # Fanout and headers: routing key is ignored
            return ""
        else:
            # Unknown type: default to topic behavior
            return "#"
