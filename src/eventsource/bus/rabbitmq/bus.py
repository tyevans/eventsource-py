"""RabbitMQ event bus implementation using aio-pika.

This module provides a distributed event bus implementation using RabbitMQ
for durable event distribution across multiple processes and servers.

Features:
- Durable event storage (events survive restarts)
- Consumer groups via queue bindings
- At-least-once delivery guarantees
- Topic-based routing with exchange types
- Horizontal scaling support
- Dead letter queue for unrecoverable failures
- Configurable retry policies
- Optional OpenTelemetry tracing

Example:
    >>> from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
    >>>
    >>> config = RabbitMQEventBusConfig(
    ...     rabbitmq_url="amqp://guest:guest@localhost:5672/",
    ...     exchange_name="events",
    ...     consumer_group="projections",
    ... )
    >>> bus = RabbitMQEventBus(config=config, event_registry=my_registry)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])
    >>> await bus.start_consuming()
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.bus.base import BaseEventBus
from eventsource.bus.rabbitmq import death_headers, serialization
from eventsource.bus.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.bus.rabbitmq.connection import RabbitMQConnectionManager
from eventsource.bus.rabbitmq.consumer import RabbitMQConsumer
from eventsource.bus.rabbitmq.models import (
    BatchPublishError,
    DLQMessage,
    HealthCheckResult,
    QueueInfo,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
    ShutdownError,
)
from eventsource.bus.rabbitmq.publisher import RabbitMQPublisher
from eventsource.bus.rabbitmq.topology import RabbitMQTopology
from eventsource.bus.retry import RetryPolicy
from eventsource.events.base import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.events.registry import EventRegistry

# Optional aio-pika import - fail gracefully if not installed
try:
    import aio_pika
    from aio_pika import DeliveryMode, Message
    from aio_pika.abc import (
        AbstractChannel,
        AbstractConnection,
        AbstractExchange,
        AbstractIncomingMessage,
        AbstractQueue,
        AbstractRobustChannel,
        AbstractRobustConnection,
    )

    RABBITMQ_AVAILABLE = True
except ImportError:
    RABBITMQ_AVAILABLE = False
    aio_pika = None  # type: ignore[assignment]
    Message = None  # type: ignore[assignment, misc]
    DeliveryMode = None  # type: ignore[assignment, misc]
    AbstractChannel = None  # type: ignore[assignment, misc]
    AbstractConnection = None  # type: ignore[assignment, misc]
    AbstractExchange = None  # type: ignore[assignment, misc]
    AbstractIncomingMessage = None  # type: ignore[assignment, misc]
    AbstractQueue = None  # type: ignore[assignment, misc]
    AbstractRobustChannel = None  # type: ignore[assignment, misc]
    AbstractRobustConnection = None  # type: ignore[assignment, misc]

# OpenTelemetry propagation imports - kept separate for distributed tracing context
# These are NOT in TracingMixin and must be imported directly for inject/extract
try:
    from opentelemetry.propagate import extract, inject
    from opentelemetry.trace import SpanKind, Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = None  # type: ignore[assignment]
    inject = None  # type: ignore[assignment]
    SpanKind = None  # type: ignore[assignment, misc]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False


# Named explicitly (not via __name__) so the logger name is stable across the
# rabbitmq.py -> rabbitmq/bus.py package move -- callers that configure
# logging by name ("eventsource.bus.rabbitmq") keep working unchanged.
logger = logging.getLogger("eventsource.bus.rabbitmq")


class RabbitMQEventBus(BaseEventBus):
    """Event bus implementation using RabbitMQ.

    This implementation provides distributed event delivery with:
    - At-least-once delivery guarantees via message acknowledgments
    - Horizontal scaling via consumer groups
    - Automatic reconnection via aio-pika's RobustConnection
    - Dead letter queue for failed messages
    - Topic-based routing with configurable exchange types

    Thread Safety:
        - Subscription methods are thread-safe
        - Publishing and consuming should only be called from async context

    Example:
        >>> from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
        >>> from eventsource.events.registry import EventRegistry
        >>>
        >>> config = RabbitMQEventBusConfig(rabbitmq_url="amqp://localhost:5672")
        >>> registry = EventRegistry()
        >>> bus = RabbitMQEventBus(config=config, event_registry=registry)
        >>>
        >>> async with bus:
        ...     bus.subscribe(OrderCreated, order_handler)
        ...     await bus.publish([OrderCreated(...)])
        ...     await bus.start_consuming()
    """

    def __init__(
        self,
        config: RabbitMQEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        """Initialize the RabbitMQ event bus.

        Args:
            config: Configuration for the RabbitMQ event bus.
                   Defaults to RabbitMQEventBusConfig() with default values.
            event_registry: Event registry for deserializing events.
                          If None, uses the default registry.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on config.enable_tracing setting.

        Raises:
            RabbitMQNotAvailableError: If aio-pika package is not installed
        """
        if not RABBITMQ_AVAILABLE:
            raise RabbitMQNotAvailableError()

        super().__init__(event_registry=event_registry)

        self._config = config or RabbitMQEventBusConfig()

        # Statistics
        self._stats = RabbitMQEventBusStats()

        # Connection lifecycle (connection/channel/connect-lock/reconnect
        # and close callbacks) is owned by RabbitMQConnectionManager.
        self._connection_manager = RabbitMQConnectionManager(config=self._config, stats=self._stats)
        self._connection_manager._is_consuming = lambda: self._consuming

        # Exchange/queue declaration and bindings are owned by
        # RabbitMQTopology. Re-declaration after a reconnect is wired
        # through the connection manager's reconnect hook.
        self._topology = RabbitMQTopology(config=self._config, connection=self._connection_manager)
        self._connection_manager.on_reconnect(self._topology.redeclare)

        self._retry_policy = RetryPolicy(
            base_delay=self._config.retry_base_delay,
            max_delay=self._config.retry_max_delay,
            jitter=self._config.retry_jitter,
            max_retries=self._config.max_retries,
        )

        # Shutdown state tracking
        self._shutdown_initiated: bool = False

        # Logger (named explicitly -- see module-level `logger` for rationale)
        self._logger = logging.getLogger("eventsource.bus.rabbitmq")

        # Initialize tracing via composition (replaces TracingMixin)
        tracer_instance = tracer or create_tracer(__name__, self._config.enable_tracing)

        # Publish-path (single/batch, sequential/concurrent strategies) is
        # owned by RabbitMQPublisher, which is the source of truth for
        # _tracer/_enable_tracing (see the proxying properties below) so
        # that tests/facade code reassigning bus._tracer after construction
        # (e.g. to a mock) still affect publish-path spans.
        self._publisher = RabbitMQPublisher(
            config=self._config,
            connection=self._connection_manager,
            topology=self._topology,
            stats=self._stats,
            tracer=tracer_instance,
            enable_tracing=tracer_instance.enabled,
        )

        # Consume path (consume loop, dispatch, retry/DLQ write path,
        # graceful stop/drain) is owned by RabbitMQConsumer. It receives
        # handler lookup / event resolution as callables so it never touches
        # the subscription registry directly.
        self._consumer = RabbitMQConsumer(
            config=self._config,
            connection=self._connection_manager,
            topology=self._topology,
            stats=self._stats,
            retry_policy=self._retry_policy,
            handlers_for=self._handlers_for,
            resolve_event_class=self._resolve_event_class,
            tracer=tracer_instance,
            enable_tracing=tracer_instance.enabled,
        )

    @property
    def config(self) -> RabbitMQEventBusConfig:
        """Get the configuration."""
        return self._config

    # -------------------------------------------------------------------
    # Backward-compatible internal accessors -- these proxy the tracer
    # fields now owned by RabbitMQPublisher, so that facade code (and
    # tests) that read/write them directly keep working unchanged.
    # -------------------------------------------------------------------

    @property
    def _tracer(self) -> Tracer | None:
        return self._publisher._tracer

    @_tracer.setter
    def _tracer(self, value: Tracer | None) -> None:
        # Kept in sync across both collaborators so tests/callers that swap in
        # a mock tracer after construction still affect publish *and* consume
        # spans, exactly as when a single field lived on the facade.
        self._publisher._tracer = value
        self._consumer._tracer = value

    @property
    def _enable_tracing(self) -> bool:
        return self._publisher._enable_tracing

    @_enable_tracing.setter
    def _enable_tracing(self, value: bool) -> None:
        self._publisher._enable_tracing = value
        self._consumer._enable_tracing = value

    @property
    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ.

        Returns True only if the connection is established and not closed.
        """
        return self._connection_manager.is_connected

    # -------------------------------------------------------------------
    # Backward-compatible internal accessors -- these proxy the private
    # connection-state fields now owned by RabbitMQConnectionManager, so
    # that facade code (and tests) that read/write them directly keep
    # working unchanged.
    # -------------------------------------------------------------------

    @property
    def _connection(self) -> AbstractRobustConnection | None:
        return self._connection_manager._connection

    @_connection.setter
    def _connection(self, value: AbstractRobustConnection | None) -> None:
        self._connection_manager._connection = value

    @property
    def _channel(self) -> AbstractRobustChannel | None:
        return self._connection_manager._channel  # type: ignore[return-value]

    @_channel.setter
    def _channel(self, value: AbstractRobustChannel | None) -> None:
        self._connection_manager._channel = value

    @property
    def _connected(self) -> bool:
        return self._connection_manager._connected

    @_connected.setter
    def _connected(self, value: bool) -> None:
        self._connection_manager._connected = value

    @property
    def _reconnecting(self) -> bool:
        return self._connection_manager._reconnecting

    @_reconnecting.setter
    def _reconnecting(self, value: bool) -> None:
        self._connection_manager._reconnecting = value

    @property
    def _was_consuming(self) -> bool:
        return self._connection_manager._was_consuming

    @_was_consuming.setter
    def _was_consuming(self, value: bool) -> None:
        self._connection_manager._was_consuming = value

    @property
    def _lock(self) -> asyncio.Lock:
        return self._connection_manager._lock

    # -------------------------------------------------------------------
    # Backward-compatible internal accessors -- these proxy the private
    # exchange/queue-reference fields now owned by RabbitMQTopology, so
    # that facade code (and tests) that read/write them directly keep
    # working unchanged.
    # -------------------------------------------------------------------

    @property
    def _exchange(self) -> AbstractExchange | None:
        return self._topology.exchange

    @_exchange.setter
    def _exchange(self, value: AbstractExchange | None) -> None:
        self._topology._exchange = value

    @property
    def _dlq_exchange(self) -> AbstractExchange | None:
        return self._topology.dlq_exchange

    @_dlq_exchange.setter
    def _dlq_exchange(self, value: AbstractExchange | None) -> None:
        self._topology._dlq_exchange = value

    @property
    def _consumer_queue(self) -> AbstractQueue | None:
        return self._topology.consumer_queue

    @_consumer_queue.setter
    def _consumer_queue(self, value: AbstractQueue | None) -> None:
        self._topology._consumer_queue = value

    @property
    def _dlq_queue(self) -> AbstractQueue | None:
        return self._topology.dlq_queue

    @_dlq_queue.setter
    def _dlq_queue(self, value: AbstractQueue | None) -> None:
        self._topology._dlq_queue = value

    # -------------------------------------------------------------------
    # Backward-compatible internal accessors -- these proxy the consumer
    # state now owned by RabbitMQConsumer, so that facade code (and tests)
    # that read/write them directly keep working unchanged.
    # -------------------------------------------------------------------

    @property
    def _consuming(self) -> bool:
        return self._consumer._consuming

    @_consuming.setter
    def _consuming(self, value: bool) -> None:
        self._consumer._consuming = value

    @property
    def _consumer_task(self) -> asyncio.Task[None] | None:
        return self._consumer._consumer_task

    @_consumer_task.setter
    def _consumer_task(self, value: asyncio.Task[None] | None) -> None:
        self._consumer._consumer_task = value

    @property
    def is_consuming(self) -> bool:
        """Check if currently consuming events."""
        return self._consumer.is_consuming

    @property
    def stats(self) -> RabbitMQEventBusStats:
        """Get current statistics."""
        return self._stats

    def get_stats(self) -> RabbitMQEventBusStats:
        """Get current statistics (method form).

        This method provides an alternative way to access statistics,
        useful for consistency with other interfaces that expect a method
        rather than a property.

        Returns:
            RabbitMQEventBusStats with current values.
        """
        return self._stats

    def get_stats_dict(self) -> dict[str, Any]:
        """Get statistics as a dictionary.

        Converts all statistics to a dictionary format suitable for
        JSON serialization and logging. Includes counters, timing
        information, connection state, and uptime calculation.

        Returns:
            Dictionary with all statistics including:
            - Counter fields (events_published, events_consumed, etc.)
            - Timing fields as ISO format strings (last_publish_at, etc.)
            - Connection state (is_connected, is_consuming)
            - Uptime in seconds (None if not connected)
            - Queue depth if available

        Example:
            >>> bus = RabbitMQEventBus(config=config)
            >>> await bus.connect()
            >>> stats_dict = bus.get_stats_dict()
            >>> import json
            >>> print(json.dumps(stats_dict, indent=2))
        """
        # Calculate uptime if connected
        uptime_seconds: float | None = None
        if self._stats.connected_at is not None:
            uptime_delta = datetime.now(UTC) - self._stats.connected_at
            uptime_seconds = uptime_delta.total_seconds()

        return {
            # Counters
            "events_published": self._stats.events_published,
            "events_consumed": self._stats.events_consumed,
            "events_processed_success": self._stats.events_processed_success,
            "events_processed_failed": self._stats.events_processed_failed,
            "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
            "handler_errors": self._stats.handler_errors,
            "reconnections": self._stats.reconnections,
            "publish_confirms": self._stats.publish_confirms,
            "publish_returns": self._stats.publish_returns,
            # Batch publishing counters
            "batch_publishes": self._stats.batch_publishes,
            "batch_events_published": self._stats.batch_events_published,
            "batch_partial_failures": self._stats.batch_partial_failures,
            # Timing (ISO format strings for JSON serialization)
            "last_publish_at": (
                self._stats.last_publish_at.isoformat() if self._stats.last_publish_at else None
            ),
            "last_consume_at": (
                self._stats.last_consume_at.isoformat() if self._stats.last_consume_at else None
            ),
            "last_error_at": (
                self._stats.last_error_at.isoformat() if self._stats.last_error_at else None
            ),
            "connected_at": (
                self._stats.connected_at.isoformat() if self._stats.connected_at else None
            ),
            # Connection state
            "is_connected": self.is_connected,
            "is_consuming": self.is_consuming,
            # Uptime in seconds
            "uptime_seconds": uptime_seconds,
        }

    def reset_stats(self) -> None:
        """Reset all statistics to initial values.

        Creates a new RabbitMQEventBusStats instance with default values,
        effectively resetting all counters to zero and all timestamps to None.

        Note:
            This does not affect connection state or other bus state,
            only the statistics tracking. The connected_at timestamp
            is also reset, which will affect uptime calculation until
            the next connection is established.

        Example:
            >>> bus = RabbitMQEventBus(config=config)
            >>> await bus.connect()
            >>> await bus.publish([event])
            >>> print(bus.stats.events_published)  # 1
            >>> bus.reset_stats()
            >>> print(bus.stats.events_published)  # 0
        """
        # Preserve connected_at if still connected to maintain accurate uptime
        connected_at = self._stats.connected_at if self._connected else None
        self._stats = RabbitMQEventBusStats(connected_at=connected_at)
        # Keep the connection manager's stats reference in sync -- it was
        # handed the original instance at construction time.
        self._connection_manager._stats = self._stats
        self._logger.info("Statistics reset")

    def _create_ssl_context(self) -> ssl.SSLContext | None:
        """Create SSL context from configuration.

        Delegates to :class:`RabbitMQConnectionManager`.

        Example:
            >>> config = RabbitMQEventBusConfig(
            ...     rabbitmq_url="amqps://host:5671/",
            ...     ca_file="/path/to/ca.crt",
            ... )
            >>> bus = RabbitMQEventBus(config=config)
            >>> ctx = bus._create_ssl_context()
            >>> assert ctx is not None
        """
        return self._connection_manager._create_ssl_context()

    async def connect(self) -> None:
        """Connect to RabbitMQ and set up exchanges/queues.

        Establishes connection, creates channel, declares exchanges
        and queues, and sets up bindings.

        Uses aio-pika's RobustConnection for automatic reconnection support.
        Sets up the channel with configured prefetch count for flow control.
        Supports TLS/SSL connections via amqps:// URLs and ssl_context configuration.

        Raises:
            Exception: If connection or setup fails
            ssl.SSLError: If SSL/TLS configuration or handshake fails
        """
        await self._connection_manager.connect()

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ.

        Closes channel and connection cleanly. Cancels any running consumer
        task before closing the connection.
        """
        # Stop consumer if running
        if self._consumer_task:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None

        await self._connection_manager.disconnect()

        self._consuming = False

        # Clear exchange/queue references
        self._exchange = None
        self._dlq_exchange = None
        self._consumer_queue = None
        self._dlq_queue = None

    async def __aenter__(self) -> RabbitMQEventBus:
        """Async context manager entry.

        Connects to RabbitMQ when entering the context.

        Returns:
            The connected event bus instance.
        """
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit with graceful shutdown.

        Performs graceful shutdown when exiting the context, regardless
        of whether an exception occurred. Uses the configured shutdown_timeout.

        Args:
            exc_type: The exception type if an exception was raised
            exc_val: The exception instance if an exception was raised
            exc_tb: The traceback if an exception was raised
        """
        await self.shutdown(timeout=self._config.shutdown_timeout)

    def _sanitize_url(self, url: str) -> str:
        """Remove credentials from URL for logging.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            url: The RabbitMQ connection URL

        Returns:
            URL with credentials replaced by ***
        """
        return self._connection_manager._sanitize_url(url)

    # =========================================================================
    # Reconnection Callback Methods (P2-004)
    #
    # Channel re-establishment and the aio-pika close/reconnect callbacks
    # themselves now live on RabbitMQConnectionManager. These facade methods
    # are thin delegations kept for backward compatibility (they're invoked
    # directly, and monkeypatched, by existing tests and callers).
    # =========================================================================

    async def _on_reconnect(self, connection: AbstractRobustConnection) -> None:
        """Handle connection restoration after disconnection.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            connection: The restored RobustConnection instance
        """
        await self._connection_manager._on_reconnect(connection)

    def _on_connection_close(
        self,
        connection: AbstractRobustConnection | None,
        exception: BaseException | None,
    ) -> None:
        """Handle connection closure.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            connection: The closed connection instance (may be None)
            exception: The exception that caused the closure, or None
                      if closed gracefully
        """
        self._connection_manager._on_connection_close(connection, exception)

    def _on_channel_close(
        self,
        channel: AbstractChannel | None,
        exception: BaseException | None,
    ) -> None:
        """Handle channel closure.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            channel: The closed channel instance (may be None)
            exception: The exception that caused the closure, or None
                      if closed gracefully
        """
        self._connection_manager._on_channel_close(channel, exception)

    # =========================================================================
    # Exchange and Queue Declaration Methods (P1-006)
    #
    # Declaration bodies now live on RabbitMQTopology. These facade methods
    # are thin delegations kept for backward compatibility (they're invoked
    # directly, and monkeypatched, by existing tests and callers).
    # =========================================================================

    async def _declare_exchange(self) -> None:
        """Declare the main event exchange.

        Delegates to :class:`RabbitMQTopology`.
        """
        await self._topology._declare_exchange()

    async def _declare_queue(self) -> None:
        """Declare the consumer queue with optional DLQ configuration.

        Delegates to :class:`RabbitMQTopology`.
        """
        await self._topology._declare_queue()

    async def _bind_queue(self) -> None:
        """Bind consumer queue to exchange based on exchange type.

        Delegates to :class:`RabbitMQTopology`.
        """
        await self._topology._bind_queue()

    async def bind_event_type(self, event_type: type[DomainEvent]) -> None:
        """Bind queue to receive messages for a specific event type.

        This method creates an additional binding for the queue to receive
        messages published with a routing key matching the event type pattern.
        Useful for direct exchanges when you want to selectively receive
        specific event types rather than all messages.

        For direct exchanges, this creates an exact-match binding for the
        event type's routing key (format: "{aggregate_type}.{event_type_name}").

        For topic exchanges, this is usually not needed since the default "#"
        binding already receives all messages. However, it can be useful if
        you've configured a more restrictive routing_key_pattern.

        Args:
            event_type: The DomainEvent subclass to bind for.

        Raises:
            RuntimeError: If not connected or queue/exchange not initialized.

        Example:
            >>> # For direct exchange - only receive OrderCreated events
            >>> config = RabbitMQEventBusConfig(
            ...     exchange_type="direct",
            ...     routing_key_pattern="",  # No default binding
            ... )
            >>> bus = RabbitMQEventBus(config=config)
            >>> await bus.connect()
            >>> await bus.bind_event_type(OrderCreated)
            >>> # Now queue will receive OrderCreated events
        """
        if not self._connected:
            raise RuntimeError("Not connected or queue/exchange not initialized")
        await self._topology.bind_event_type(event_type)

    async def bind_routing_key(self, routing_key: str) -> None:
        """Bind queue to receive messages with a specific routing key.

        Creates an additional binding for the queue to receive messages
        matching the specified routing key. This is a lower-level method
        than bind_event_type, useful when you need precise control over
        routing key patterns.

        Args:
            routing_key: The routing key pattern to bind. For topic exchanges,
                this can include wildcards (* for one word, # for zero or more).
                For direct exchanges, this must be an exact match.

        Raises:
            RuntimeError: If not connected or queue/exchange not initialized.

        Example:
            >>> # Bind to all Order events on topic exchange
            >>> await bus.bind_routing_key("Order.*")
            >>> # Bind to specific routing key on direct exchange
            >>> await bus.bind_routing_key("Order.OrderCreated")
        """
        if not self._connected:
            raise RuntimeError("Not connected or queue/exchange not initialized")
        await self._topology.bind_routing_key(routing_key)

    async def _declare_dlq(self) -> None:
        """Declare dead letter exchange and queue.

        Delegates to :class:`RabbitMQTopology`.
        """
        await self._topology._declare_dlq()

    # =========================================================================
    # DLQ Helper Methods (P2-001)
    # =========================================================================

    # Permanent public aliases: the pure implementations live in death_headers.py.
    get_death_count = staticmethod(death_headers.get_death_count)
    get_first_death_queue = staticmethod(death_headers.get_first_death_queue)
    get_first_death_reason = staticmethod(death_headers.get_first_death_reason)
    get_first_death_exchange = staticmethod(death_headers.get_first_death_exchange)
    get_original_routing_key = staticmethod(death_headers.get_original_routing_key)
    is_from_dlq = staticmethod(death_headers.is_from_dlq)
    get_death_info = staticmethod(death_headers.get_death_info)

    # =========================================================================
    # Retry Logic Methods (P2-002)
    # =========================================================================

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate the delay before the next retry.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            retry_count: Zero-based retry attempt number.

        Returns:
            Delay in seconds, with symmetric jitter applied.
        """
        return self._consumer._calculate_retry_delay(retry_count)

    async def _handle_failed_message(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a failed message - retry with backoff or route to DLQ.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Current retry count (from x-retry-count header)
        """
        await self._consumer._handle_failed_message(message, error, retry_count)

    async def _republish_for_retry(
        self,
        original_message: AbstractIncomingMessage,
        new_retry_count: int,
    ) -> None:
        """Republish a message with incremented retry count.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            original_message: The original message to retry
            new_retry_count: The new retry count value
        """
        await self._consumer._republish_for_retry(original_message, new_retry_count)

    async def _send_to_dlq(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Send a failed message to the dead letter queue.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Final retry count before DLQ
        """
        await self._consumer._send_to_dlq(message, error, retry_count)

    # =========================================================================
    # Event Serialization Methods (P1-007)
    # =========================================================================

    @staticmethod
    def _get_event_field_default(
        event_type: type[DomainEvent], field_name: str, default: str
    ) -> str:
        """Get the default value for a field from a DomainEvent subclass.

        Thin wrapper -- see `serialization.get_event_field_default`.
        """
        return serialization.get_event_field_default(event_type, field_name, default)

    def _get_routing_key(self, event: DomainEvent) -> str:
        """Generate routing key for an event.

        Thin wrapper -- see `serialization.get_routing_key`.
        """
        return serialization.get_routing_key(event)

    def _serialize_event(self, event: DomainEvent) -> tuple[bytes, dict[str, Any]]:
        """Serialize a domain event to JSON bytes and message headers.

        Thin wrapper -- see `serialization.serialize_event`.
        """
        return serialization.serialize_event(event)

    def _create_message(self, event: DomainEvent) -> Message:
        """Create an AMQP message from a domain event.

        Thin wrapper -- see `serialization.create_message`.
        """
        return serialization.create_message(event)

    def _create_message_with_tracing(
        self,
        event: DomainEvent,
        span: Any = None,
    ) -> Message:
        """Create an AMQP message with optional trace context injection.

        Thin wrapper -- see `serialization.create_message_with_tracing`.
        """
        return serialization.create_message_with_tracing(event, span)

    def _deserialize_event(
        self,
        message: AbstractIncomingMessage,
    ) -> DomainEvent | None:
        """Deserialize an AMQP message to a domain event.

        Thin wrapper -- see `serialization.deserialize_event`.
        """
        return serialization.deserialize_event(message, self._resolve_event_class, self._logger)

    # =========================================================================
    # Publish Methods (P1-008, P3-005)
    # =========================================================================

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to RabbitMQ exchange.

        Events are serialized to JSON and published with routing keys
        based on aggregate type and event type.

        For single events, publishes directly. For multiple events, uses
        batch optimization with concurrent publishing via asyncio.gather()
        for improved performance.

        Args:
            events: List of events to publish
            background: If True, publish without waiting for confirms.
                       Default is False (wait for confirms).
                       Note: Unlike InMemoryEventBus which uses asyncio tasks
                       for background, RabbitMQ is inherently async. The background
                       parameter controls confirmation waiting rather than task spawning.

        Raises:
            RuntimeError: If not connected and connection fails, or if exchange
                         not initialized after connection
            Exception: If publishing fails (exceptions are logged and re-raised)

        Example:
            >>> await bus.publish([OrderCreated(...), OrderShipped(...)])
        """
        if not events:
            return

        # Auto-connect if needed
        if not self._connected:
            await self.connect()

        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        if len(events) == 1:
            # Single event - no batch optimization needed
            await self._publish_single(events[0], wait_for_confirm=not background)
        else:
            # Multiple events - use batch optimization
            await self._publisher.publish_many(events, wait_for_confirm=not background)

    async def publish_batch(
        self,
        events: list[DomainEvent],
        preserve_order: bool = False,
    ) -> dict[str, int]:
        """Publish multiple events with batch optimization.

        This method provides optimized batch publishing using concurrent
        asyncio.gather() to publish multiple events in parallel. Large batches
        are automatically chunked based on config.batch_size to prevent
        overwhelming the broker.

        Args:
            events: List of events to publish
            preserve_order: If True, publishes events sequentially to maintain
                          order guarantees. Default is False (concurrent publishing).
                          Use True when event ordering within the batch is critical.

        Returns:
            Dictionary with batch publishing statistics:
            - total: Total number of events in the batch
            - published: Number of events successfully published
            - failed: Number of events that failed to publish
            - chunks: Number of chunks the batch was split into

        Raises:
            RuntimeError: If not connected and connection fails, or if exchange
                         not initialized after connection
            BatchPublishError: If any events failed to publish (contains partial results)

        Example:
            >>> events = [OrderCreated(...) for _ in range(1000)]
            >>> result = await bus.publish_batch(events)
            >>> print(f"Published {result['published']}/{result['total']} events")
        """
        if not events:
            return {"total": 0, "published": 0, "failed": 0, "chunks": 0}

        # Auto-connect if needed
        if not self._connected:
            await self.connect()

        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        return await self._publisher.publish_batch(events, preserve_order=preserve_order)

    async def _publish_single(
        self,
        event: DomainEvent,
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish a single event to the exchange.

        Thin wrapper -- see `RabbitMQPublisher.publish_one`. Kept as a
        facade method because tests exercise it directly.
        """
        await self._publisher.publish_one(event, wait_for_confirm=wait_for_confirm)

    # =========================================================================
    # Consumer Loop Methods (P1-010)
    # =========================================================================

    async def start_consuming(self) -> None:
        """Start consuming events from the RabbitMQ queue.

        Connects if necessary, then delegates the consume loop to
        :class:`RabbitMQConsumer`.

        Raises:
            RuntimeError: If not connected and connection fails, or if
                         consumer queue not initialized
        """
        if not self._connected:
            await self.connect()

        await self._consumer.start()

    async def stop_consuming(self) -> None:
        """Stop the consumer loop gracefully.

        Delegates to :class:`RabbitMQConsumer`.
        """
        await self._consumer.stop()

    def start_consuming_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task.

        The background task runs :meth:`start_consuming` (so it auto-connects
        exactly as a direct call would); the task handle itself is owned by
        :class:`RabbitMQConsumer`.

        Returns:
            The background task running the consumer

        Raises:
            RuntimeError: If consumer is already running in background
        """
        return self._consumer.start_in_background(self.start_consuming)

    async def _process_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        """Process a single message from the queue.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            message: The incoming AMQP message
        """
        await self._consumer._process_message(message)

    async def _dispatch_event(
        self,
        event: DomainEvent,
        message: AbstractIncomingMessage,
        parent_span: Any = None,
    ) -> None:
        """Dispatch an event to all matching handlers.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            event: The deserialized domain event
            message: Original AMQP message for context
            parent_span: Optional parent span for tracing

        Raises:
            HandlerDispatchError: If one or more handlers raise.
        """
        await self._consumer._dispatch_event(event, message, parent_span)

    # =========================================================================
    # DLQ Operations Methods (P2-003)
    # =========================================================================

    async def get_dlq_messages(
        self,
        limit: int = 100,
    ) -> list[DLQMessage]:
        """Get messages from the dead letter queue for inspection.

        Retrieves messages from the DLQ without removing them. Messages are
        retrieved using basic.get and then rejected with requeue=True to
        preserve them in the queue.

        Note: This operation is not atomic. If another consumer is reading
        from the DLQ concurrently, some messages may be missed or duplicated.
        For production use, consider using a dedicated DLQ consumer.

        Args:
            limit: Maximum number of messages to retrieve (default: 100)

        Returns:
            List of DLQMessage objects containing message content and metadata.
            Returns empty list if:
            - Not connected
            - DLQ is not enabled
            - Channel is not initialized
            - An error occurs during retrieval

        Example:
            >>> messages = await bus.get_dlq_messages(limit=10)
            >>> for msg in messages:
            ...     print(f"{msg.message_id}: {msg.event_type} - {msg.dlq_reason}")
        """
        if not self._connected or not self._config.enable_dlq:
            return []

        if not self._channel:
            self._logger.warning(
                "Cannot get DLQ messages: channel not initialized",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return []

        messages: list[DLQMessage] = []

        try:
            # Get queue reference - declare passively to ensure it exists
            dlq_queue = await self._channel.get_queue(
                self._config.dlq_queue_name,
            )

            for _ in range(limit):
                # Get message without auto-ack
                message = await dlq_queue.get(no_ack=False)
                if message is None:
                    # No more messages in queue
                    break

                headers = dict(message.headers or {})
                body = message.body.decode("utf-8")

                # Extract retry count with type safety
                dlq_retry_count_value = headers.get("x-dlq-retry-count")
                if dlq_retry_count_value is None:
                    dlq_retry_count = None
                elif isinstance(dlq_retry_count_value, int):
                    dlq_retry_count = dlq_retry_count_value
                else:
                    dlq_retry_count = int(str(dlq_retry_count_value))

                dlq_message = DLQMessage(
                    message_id=message.message_id,
                    routing_key=message.routing_key,
                    body=body,
                    headers=headers,
                    event_type=str(headers.get("event_type"))
                    if headers.get("event_type")
                    else None,
                    dlq_reason=str(headers.get("x-dlq-reason"))
                    if headers.get("x-dlq-reason")
                    else None,
                    dlq_error_type=str(headers.get("x-dlq-error-type"))
                    if headers.get("x-dlq-error-type")
                    else None,
                    dlq_retry_count=dlq_retry_count,
                    dlq_timestamp=str(headers.get("x-dlq-timestamp"))
                    if headers.get("x-dlq-timestamp")
                    else None,
                    original_routing_key=str(headers.get("x-original-routing-key"))
                    if headers.get("x-original-routing-key")
                    else None,
                )
                messages.append(dlq_message)

                # Reject with requeue to put message back in queue (non-destructive read)
                await message.reject(requeue=True)

            self._logger.info(
                f"Retrieved {len(messages)} messages from DLQ",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "message_count": len(messages),
                    "limit": limit,
                },
            )

        except Exception as e:
            self._logger.error(
                f"Failed to get DLQ messages: {e}",
                exc_info=True,
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )

        return messages

    async def get_dlq_message_count(self) -> int:
        """Get the number of messages in the dead letter queue.

        Returns the current count of messages waiting in the DLQ.
        Uses passive queue declaration to query the message count
        without modifying the queue.

        Returns:
            Number of messages in the DLQ.
            Returns 0 if:
            - Not connected
            - DLQ is not enabled
            - Channel is not initialized
            - An error occurs during retrieval

        Example:
            >>> count = await bus.get_dlq_message_count()
            >>> if count > 0:
            ...     print(f"Warning: {count} messages in DLQ")
        """
        if not self._connected or not self._config.enable_dlq:
            return 0

        if not self._channel:
            self._logger.warning(
                "Cannot get DLQ count: channel not initialized",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return 0

        try:
            # Declare queue passively to get message count
            # This will fail if queue doesn't exist, which is fine
            queue_info = await self._channel.declare_queue(
                name=self._config.dlq_queue_name,
                passive=True,
            )

            count = queue_info.declaration_result.message_count or 0
            self._logger.debug(
                f"DLQ message count: {count}",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "message_count": count,
                },
            )
            return count

        except Exception as e:
            self._logger.error(
                f"Failed to get DLQ message count: {e}",
                exc_info=True,
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return 0

    async def replay_dlq_message(
        self,
        message_id: str,
    ) -> bool:
        """Replay a specific message from the DLQ back to the main exchange.

        Finds a message in the DLQ by its message_id, removes DLQ-specific
        headers, resets the retry count to 0, and republishes it to the
        main exchange for reprocessing.

        The replayed message includes an 'x-replayed-from-dlq' header with
        the timestamp of when it was replayed, allowing tracking of message
        replay history.

        Note: This operation searches through the DLQ sequentially. For
        queues with many messages, this may be slow. The search is limited
        to 1000 messages to prevent excessive iteration.

        Args:
            message_id: The message_id of the DLQ message to replay

        Returns:
            True if the message was found and replayed successfully,
            False otherwise.

        Example:
            >>> success = await bus.replay_dlq_message("abc-123-def")
            >>> if success:
            ...     print("Message replayed successfully")
        """
        if not self._connected or not self._exchange:
            self._logger.warning(
                "Cannot replay DLQ message: not connected or exchange not initialized",
                extra={
                    "message_id": message_id,
                    "dlq_queue": self._config.dlq_queue_name,
                    "is_connected": self._connected,
                    "exchange_initialized": self._exchange is not None,
                },
            )
            return False

        if not self._channel or not self._config.enable_dlq:
            self._logger.warning(
                "Cannot replay DLQ message: channel not initialized or DLQ disabled",
                extra={
                    "message_id": message_id,
                    "dlq_queue": self._config.dlq_queue_name,
                    "dlq_enabled": self._config.enable_dlq,
                    "channel_initialized": self._channel is not None,
                },
            )
            return False

        try:
            dlq_queue = await self._channel.get_queue(
                self._config.dlq_queue_name,
            )

            # Search for the message (with iteration limit to prevent infinite loops)
            max_search = 1000
            found = False

            for _ in range(max_search):
                message = await dlq_queue.get(no_ack=False)
                if message is None:
                    # Reached end of queue
                    break

                if message.message_id == message_id:
                    # Found the message - replay it
                    await self._replay_message(message)
                    await message.ack()  # Remove from DLQ
                    found = True

                    self._logger.info(
                        f"Replayed DLQ message: {message_id}",
                        extra={
                            "message_id": message_id,
                            "event_type": (message.headers or {}).get("event_type"),
                            "dlq_queue": self._config.dlq_queue_name,
                        },
                    )
                    break
                else:
                    # Not the message we want - put back in queue
                    await message.reject(requeue=True)

            if not found:
                self._logger.warning(
                    f"DLQ message not found for replay: {message_id}",
                    extra={
                        "message_id": message_id,
                        "dlq_queue": self._config.dlq_queue_name,
                        "max_search": max_search,
                    },
                )

            return found

        except Exception as e:
            self._logger.error(
                f"Failed to replay DLQ message {message_id}: {e}",
                exc_info=True,
                extra={
                    "message_id": message_id,
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return False

    async def _replay_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        """Republish a DLQ message to the main exchange.

        Internal helper method that creates a new message from a DLQ message
        with DLQ-specific headers removed and retry count reset.

        Headers removed:
        - x-dlq-reason
        - x-dlq-error-type
        - x-dlq-retry-count
        - x-dlq-timestamp
        - x-original-routing-key
        - x-death (RabbitMQ's built-in death header)

        Headers added/modified:
        - x-retry-count: Reset to 0
        - x-replayed-from-dlq: Timestamp of replay

        Args:
            message: The DLQ message to replay

        Raises:
            RuntimeError: If exchange is not initialized
        """
        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        # Copy headers and remove DLQ-specific ones
        headers = dict(message.headers or {})
        dlq_headers_to_remove = [
            "x-dlq-reason",
            "x-dlq-error-type",
            "x-dlq-retry-count",
            "x-dlq-timestamp",
            "x-original-routing-key",
            "x-death",  # RabbitMQ's built-in death header
        ]
        for key in dlq_headers_to_remove:
            headers.pop(key, None)

        # Reset retry count and add replay marker
        headers["x-retry-count"] = 0
        headers["x-replayed-from-dlq"] = datetime.now(UTC).isoformat()

        # Get original routing key (from our custom header or message routing key)
        original_headers = message.headers or {}
        original_routing_key = original_headers.get(
            "x-original-routing-key", message.routing_key or ""
        )

        # Create replay message
        replay_message = Message(
            body=message.body,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            headers=headers,
        )

        await self._exchange.publish(
            replay_message,
            routing_key=str(original_routing_key),
        )

        self._logger.debug(
            "Republished message to exchange",
            extra={
                "message_id": message.message_id,
                "routing_key": original_routing_key,
                "exchange": self._config.exchange_name,
            },
        )

    async def purge_dlq(self) -> int:
        """Remove all messages from the dead letter queue.

        Purges all messages from the DLQ. This operation is irreversible -
        all messages will be permanently deleted.

        Use with caution in production environments. Consider archiving
        or reviewing DLQ messages before purging.

        Returns:
            Number of messages that were purged.
            Returns 0 if:
            - Not connected
            - DLQ is not enabled
            - Channel is not initialized
            - An error occurs during purge

        Example:
            >>> count = await bus.purge_dlq()
            >>> print(f"Purged {count} messages from DLQ")
        """
        if not self._connected or not self._config.enable_dlq:
            return 0

        if not self._channel:
            self._logger.warning(
                "Cannot purge DLQ: channel not initialized",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return 0

        try:
            # Get queue reference
            dlq_queue = await self._channel.get_queue(
                self._config.dlq_queue_name,
            )

            # Purge the queue - purge() returns PurgeOk with message_count attribute
            purge_result = await dlq_queue.purge()
            purged_count = purge_result.message_count or 0

            self._logger.info(
                f"Purged {purged_count} messages from DLQ",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "purged_count": purged_count,
                },
            )

            return purged_count

        except Exception as e:
            self._logger.error(
                f"Failed to purge DLQ: {e}",
                exc_info=True,
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return 0

    # =========================================================================
    # Graceful Shutdown Methods (P2-005)
    # =========================================================================

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the event bus gracefully.

        Stops consuming new messages, waits for in-flight messages to complete
        processing, and closes all connections. This method is idempotent - calling
        it multiple times is safe.

        The shutdown process follows these steps:
        1. Stop accepting new messages (set _consuming flag to False)
        2. Wait for the consumer task to finish processing current messages
        3. Wait for any in-flight message processing to complete
        4. Disconnect from RabbitMQ (close channel and connection)

        After shutdown is initiated, the event bus cannot be reused without
        creating a new instance. Attempting to publish or start consuming after
        shutdown will raise an error.

        Args:
            timeout: Maximum time to wait for graceful shutdown in seconds.
                    If the timeout is exceeded, a TimeoutError is raised.
                    The timeout is split between the consumer stop and drain phases.
                    Default is 30.0 seconds.

        Raises:
            TimeoutError: If the shutdown process exceeds the timeout.
                         The error message includes details about what phase timed out.
                         Connection and channel are NOT force-closed on timeout.

        Example:
            >>> await bus.shutdown(timeout=10.0)

            # With context manager (uses config.shutdown_timeout):
            >>> async with RabbitMQEventBus(config=config) as bus:
            ...     await bus.publish([event])
            # Graceful shutdown happens automatically on exit
        """
        if self._shutdown_initiated:
            self._logger.debug("Shutdown already initiated, skipping")
            return

        self._shutdown_initiated = True
        shutdown_start = datetime.now(UTC)

        self._logger.info(
            f"Initiating graceful shutdown (timeout={timeout}s)",
            extra={"timeout": timeout},
        )

        try:
            # Step 1: Stop accepting new messages
            await self._stop_consuming_gracefully(timeout)

            # Step 2: Wait for in-flight processing to complete
            await self._drain_in_flight(timeout)

            # Step 2b: Wait for publisher background tasks (a different
            # concern from consumer message processing above)
            await self._drain_background(timeout)

            # Step 3: Disconnect from RabbitMQ
            await self.disconnect()

            shutdown_duration = (datetime.now(UTC) - shutdown_start).total_seconds()
            self._logger.info(
                f"Graceful shutdown completed in {shutdown_duration:.2f}s",
                extra={"duration_seconds": shutdown_duration},
            )

        except TimeoutError:
            shutdown_duration = (datetime.now(UTC) - shutdown_start).total_seconds()
            self._logger.error(
                f"Graceful shutdown timed out after {shutdown_duration:.2f}s",
                extra={
                    "timeout": timeout,
                    "duration_seconds": shutdown_duration,
                },
            )
            # Re-raise TimeoutError as specified in task requirements
            # Connection is NOT force-closed - caller can decide what to do
            raise TimeoutError(
                f"Graceful shutdown timed out after {shutdown_duration:.2f}s. "
                f"In-flight messages may still be processing. "
                f"Call disconnect() or _force_disconnect() to force close."
            ) from None

        except Exception as e:
            shutdown_duration = (datetime.now(UTC) - shutdown_start).total_seconds()
            self._logger.error(
                f"Error during shutdown after {shutdown_duration:.2f}s: {e}",
                exc_info=True,
                extra={
                    "duration_seconds": shutdown_duration,
                    "error": str(e),
                },
            )
            raise

    async def _stop_consuming_gracefully(self, timeout: float) -> None:
        """Stop consuming and wait for consumer task to finish.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            timeout: Maximum time to wait for the consumer to stop.
        """
        await self._consumer.stop_gracefully(timeout)

    async def _drain_in_flight(self, timeout: float) -> None:
        """Wait for any in-flight message processing to complete.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            timeout: Maximum time available for draining.
        """
        await self._consumer.drain_in_flight(timeout)

    # =========================================================================
    # Queue Info and Health Check Methods (P2-007)
    # =========================================================================

    async def get_queue_info(self) -> QueueInfo:
        """Get information about the consumer queue.

        Retrieves queue statistics using passive queue declaration, which
        queries the queue state without modifying it. This is safe to call
        at any time and does not affect the queue or its messages.

        Returns:
            QueueInfo object containing:
            - name: Queue name
            - message_count: Number of messages waiting in the queue
            - consumer_count: Number of active consumers
            - state: "running", "idle", "unknown", or "error"
            - error: Error message if state is "error"

        Note:
            If not connected or channel is not initialized, returns a
            QueueInfo with state="error" and appropriate error message.

        Example:
            >>> info = await bus.get_queue_info()
            >>> print(f"Queue {info.name} has {info.message_count} messages")
            >>> print(f"State: {info.state}, Consumers: {info.consumer_count}")
        """
        # Handle not connected state
        if not self._connected or not self._channel:
            return QueueInfo(
                name=self._config.queue_name,
                message_count=0,
                consumer_count=0,
                state="error",
                error="Not connected to RabbitMQ",
            )

        try:
            # Use passive declaration to get queue info without modifying it
            queue_info = await self._channel.declare_queue(
                name=self._config.queue_name,
                passive=True,
            )

            # Extract message and consumer counts from declaration result
            message_count = queue_info.declaration_result.message_count or 0
            consumer_count = queue_info.declaration_result.consumer_count or 0

            # Determine queue state based on consumer count
            state = "running" if consumer_count > 0 else "idle"

            self._logger.debug(
                f"Queue info retrieved: {self._config.queue_name}",
                extra={
                    "queue_name": self._config.queue_name,
                    "message_count": message_count,
                    "consumer_count": consumer_count,
                    "state": state,
                },
            )

            return QueueInfo(
                name=self._config.queue_name,
                message_count=message_count,
                consumer_count=consumer_count,
                state=state,
            )

        except Exception as e:
            self._logger.error(
                f"Failed to get queue info: {e}",
                exc_info=True,
                extra={
                    "queue_name": self._config.queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )

            return QueueInfo(
                name=self._config.queue_name,
                message_count=0,
                consumer_count=0,
                state="error",
                error=str(e),
            )

    async def health_check(self) -> HealthCheckResult:
        """Perform a comprehensive health check of the event bus.

        Checks the status of:
        - RabbitMQ connection
        - AMQP channel
        - Consumer queue accessibility
        - Dead letter queue (if enabled)

        This method is safe to call frequently and does not modify any
        queue or exchange state.

        Returns:
            HealthCheckResult object containing:
            - healthy: True if all components are operational
            - connection_status: "connected", "disconnected", or "closed"
            - channel_status: "open", "closed", or "not_initialized"
            - queue_status: "accessible", "inaccessible", "not_initialized", or "error: ..."
            - dlq_status: Status of DLQ or "disabled" if DLQ not enabled
            - error: Error message if unhealthy
            - details: Additional configuration and state information

        Example:
            >>> result = await bus.health_check()
            >>> if result.healthy:
            ...     print("Event bus is healthy")
            ... else:
            ...     print(f"Unhealthy: {result.error}")
            ...     print(f"Details: {result.details}")
        """
        healthy = True
        error_messages: list[str] = []

        # Check connection status
        if not self._connection:
            connection_status = "disconnected"
            healthy = False
            error_messages.append("Not connected to RabbitMQ")
        elif self._connection.is_closed:
            connection_status = "closed"
            healthy = False
            error_messages.append("RabbitMQ connection is closed")
        else:
            connection_status = "connected"

        # Check channel status
        if not self._channel:
            channel_status = "not_initialized"
            healthy = False
            error_messages.append("Channel not initialized")
        elif self._channel.is_closed:
            channel_status = "closed"
            healthy = False
            error_messages.append("AMQP channel is closed")
        else:
            channel_status = "open"

        # Check consumer queue accessibility
        queue_status = "not_initialized"
        if self._consumer_queue and self._channel and not self._channel.is_closed:
            try:
                # Use passive declaration to check queue accessibility
                await self._channel.declare_queue(
                    name=self._config.queue_name,
                    passive=True,
                )
                queue_status = "accessible"
            except Exception as e:
                queue_status = f"error: {e}"
                healthy = False
                error_messages.append(f"Queue check failed: {e}")
        elif not self._consumer_queue:
            queue_status = "not_initialized"
            # Don't mark as unhealthy if we simply haven't connected yet
            if self._connected:
                healthy = False
                error_messages.append("Consumer queue not initialized")

        # Check DLQ status if enabled
        dlq_status: str | None = None
        if self._config.enable_dlq:
            if self._channel and not self._channel.is_closed:
                try:
                    await self._channel.declare_queue(
                        name=self._config.dlq_queue_name,
                        passive=True,
                    )
                    dlq_status = "accessible"
                except Exception as e:
                    dlq_status = f"error: {e}"
                    # DLQ errors don't make the overall bus unhealthy
                    # but we log them
                    self._logger.warning(
                        f"DLQ health check failed: {e}",
                        extra={"dlq_queue": self._config.dlq_queue_name},
                    )
            else:
                dlq_status = "inaccessible"
        else:
            dlq_status = "disabled"

        # Build details dictionary
        details: dict[str, Any] = {
            "exchange": self._config.exchange_name,
            "queue": self._config.queue_name,
            "consumer_group": self._config.consumer_group,
            "consuming": self._consuming,
            "dlq_enabled": self._config.enable_dlq,
        }

        if self._config.enable_dlq:
            details["dlq_queue"] = self._config.dlq_queue_name

        # Add stats summary
        details["stats"] = {
            "events_published": self._stats.events_published,
            "events_consumed": self._stats.events_consumed,
            "events_processed_success": self._stats.events_processed_success,
            "events_processed_failed": self._stats.events_processed_failed,
            "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
            "reconnections": self._stats.reconnections,
        }

        # Combine error messages
        error = "; ".join(error_messages) if error_messages else None

        self._logger.debug(
            f"Health check completed: healthy={healthy}",
            extra={
                "healthy": healthy,
                "connection_status": connection_status,
                "channel_status": channel_status,
                "queue_status": queue_status,
                "dlq_status": dlq_status,
            },
        )

        return HealthCheckResult(
            healthy=healthy,
            connection_status=connection_status,
            channel_status=channel_status,
            queue_status=queue_status,
            dlq_status=dlq_status,
            error=error,
            details=details,
        )

    async def _force_disconnect(self) -> None:
        """Force disconnect without waiting for graceful completion.

        This method immediately cancels any running consumer task and closes
        the channel and connection without waiting. It suppresses all exceptions
        during cleanup to ensure the disconnect completes.

        Use this method when graceful shutdown has timed out or failed and
        you need to immediately release resources.

        Note:
            This method may result in unacknowledged messages being redelivered
            by RabbitMQ to other consumers. It should only be used as a last resort.

        Example:
            >>> try:
            ...     await bus.shutdown(timeout=5.0)
            ... except TimeoutError:
            ...     await bus._force_disconnect()
        """
        self._logger.warning("Forcing disconnect")

        self._consuming = False
        self._shutdown_initiated = True

        # Cancel consumer task immediately
        if self._consumer_task:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._consumer_task
            self._consumer_task = None

        await self._connection_manager.force_disconnect()

        # Clear exchange/queue references
        self._exchange = None
        self._dlq_exchange = None
        self._consumer_queue = None
        self._dlq_queue = None

        self._logger.info("Forced disconnect completed")

    @property
    def is_shutdown(self) -> bool:
        """Check if shutdown has been initiated.

        Returns:
            True if shutdown() has been called on this instance,
            False otherwise. Once shutdown is initiated, the event bus
            cannot be reused.
        """
        return self._shutdown_initiated


__all__ = [
    "BatchPublishError",
    "DLQMessage",
    "HealthCheckResult",
    "OTEL_AVAILABLE",
    "QueueInfo",
    "RabbitMQEventBus",
    "RabbitMQEventBusConfig",
    "RabbitMQEventBusStats",
    "RabbitMQNotAvailableError",
    "RABBITMQ_AVAILABLE",
    "ShutdownError",
]
