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
from eventsource.bus.rabbitmq.models import (
    BatchPublishError,
    DLQMessage,
    HealthCheckResult,
    QueueInfo,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
    ShutdownError,
)
from eventsource.bus.retry import RetryPolicy
from eventsource.events.base import DomainEvent
from eventsource.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_HANDLER_SUCCESS,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from eventsource.events.registry import EventRegistry

# Optional aio-pika import - fail gracefully if not installed
try:
    import aio_pika
    from aio_pika import DeliveryMode, ExchangeType, Message
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
    ExchangeType = None  # type: ignore[assignment, misc]
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

        # Consumer state (not yet extracted -- see Tasks 5/7)
        self._consuming = False

        # Exchange and queue references (set in P1-006)
        self._exchange: AbstractExchange | None = None
        self._dlq_exchange: AbstractExchange | None = None
        self._consumer_queue: AbstractQueue | None = None
        self._dlq_queue: AbstractQueue | None = None

        # Statistics
        self._stats = RabbitMQEventBusStats()

        # Connection lifecycle (connection/channel/connect-lock/reconnect
        # and close callbacks) is owned by RabbitMQConnectionManager.
        self._connection_manager = RabbitMQConnectionManager(config=self._config, stats=self._stats)
        self._connection_manager._is_consuming = lambda: self._consuming

        # Temporary reconnect callbacks: topology redeclare and consumer
        # (queue) resume still live on the facade until Tasks 5/7 extract
        # them into dedicated topology/consumer collaborators. These get
        # swapped to point at the new collaborators' methods then.
        async def _redeclare_topology() -> None:
            if self._config.enable_dlq:
                await self._declare_dlq()
            await self._declare_exchange()

        async def _resume_consumer_topology() -> None:
            await self._declare_queue()
            await self._bind_queue()

        self._connection_manager.on_reconnect(_redeclare_topology)
        self._connection_manager.on_reconnect(_resume_consumer_topology)

        self._retry_policy = RetryPolicy(
            base_delay=self._config.retry_base_delay,
            max_delay=self._config.retry_max_delay,
            jitter=self._config.retry_jitter,
            max_retries=self._config.max_retries,
        )

        # Background tasks
        self._consumer_task: asyncio.Task[None] | None = None

        # Shutdown state tracking
        self._shutdown_initiated: bool = False

        # Logger (named explicitly -- see module-level `logger` for rationale)
        self._logger = logging.getLogger("eventsource.bus.rabbitmq")

        # Initialize tracing via composition (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, self._config.enable_tracing)
        self._enable_tracing = self._tracer.enabled

    @property
    def config(self) -> RabbitMQEventBusConfig:
        """Get the configuration."""
        return self._config

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

    @property
    def is_consuming(self) -> bool:
        """Check if currently consuming events."""
        return self._consuming

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
    # =========================================================================

    async def _declare_exchange(self) -> None:
        """Declare the main event exchange.

        Creates an exchange with the configured type (topic, direct, fanout, or headers)
        for event routing. The exchange type determines how messages are routed to queues.

        - topic: Route based on routing key patterns (e.g., "order.*", "*.created")
        - direct: Route to queues with exact routing key match
        - fanout: Broadcast to all bound queues regardless of routing key
        - headers: Route based on message header attributes

        Raises:
            RuntimeError: If channel not initialized
        """
        if not self._channel:
            raise RuntimeError("Channel not initialized")

        # Map string to ExchangeType enum
        exchange_type_map = {
            "topic": ExchangeType.TOPIC,
            "direct": ExchangeType.DIRECT,
            "fanout": ExchangeType.FANOUT,
            "headers": ExchangeType.HEADERS,
        }

        exchange_type = exchange_type_map.get(
            self._config.exchange_type.lower(), ExchangeType.TOPIC
        )

        self._exchange = await self._channel.declare_exchange(
            name=self._config.exchange_name,
            type=exchange_type,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
        )

        self._logger.info(
            f"Declared exchange: {self._config.exchange_name}",
            extra={
                "exchange_name": self._config.exchange_name,
                "exchange_type": self._config.exchange_type,
                "durable": self._config.durable,
                "auto_delete": self._config.auto_delete,
            },
        )

    async def _declare_queue(self) -> None:
        """Declare the consumer queue with optional DLQ configuration.

        Creates a durable queue for this consumer group. If DLQ is enabled,
        the queue is configured with dead letter exchange arguments so that
        rejected messages are automatically routed to the DLQ.

        Raises:
            RuntimeError: If channel not initialized
        """
        if not self._channel:
            raise RuntimeError("Channel not initialized")

        # Queue arguments for DLQ routing
        arguments: dict[str, Any] = {}

        if self._config.enable_dlq:
            arguments["x-dead-letter-exchange"] = self._config.dlq_exchange_name
            arguments["x-dead-letter-routing-key"] = self._config.queue_name

        self._consumer_queue = await self._channel.declare_queue(
            name=self._config.queue_name,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
            arguments=arguments if arguments else None,
        )

        self._logger.info(
            f"Declared queue: {self._config.queue_name}",
            extra={
                "queue_name": self._config.queue_name,
                "durable": self._config.durable,
                "auto_delete": self._config.auto_delete,
                "dlq_enabled": self._config.enable_dlq,
            },
        )

    async def _bind_queue(self) -> None:
        """Bind consumer queue to exchange based on exchange type.

        The binding behavior varies by exchange type:
        - topic: Binds with routing key pattern (default "#" for all messages).
          Supports wildcards: "*" matches one word, "#" matches zero or more.
        - direct: Binds with exact routing key (default is queue_name).
          Only messages with matching routing key are delivered.
          Multiple consumers on the same queue = competing consumers (load balanced).
        - fanout: Routing key is ignored. All bound queues receive all messages.
        - headers: Routing key is ignored. Routing is based on message headers.

        The routing key pattern can be customized via config.routing_key_pattern.
        If not set, an appropriate default is chosen based on exchange type.

        Raises:
            RuntimeError: If queue or exchange not initialized
        """
        if not self._consumer_queue or not self._exchange:
            raise RuntimeError("Queue or exchange not initialized")

        # Get effective routing key based on exchange type and config
        routing_key = self._config.get_effective_routing_key()

        # Bind queue to exchange
        await self._consumer_queue.bind(
            exchange=self._exchange,
            routing_key=routing_key,
        )

        # Log binding with exchange-type-specific information
        exchange_type_lower = self._config.exchange_type.lower()
        if exchange_type_lower == "fanout":
            # Fanout-specific logging to clarify broadcast behavior
            self._logger.info(
                f"Bound queue {self._config.queue_name} to fanout exchange "
                f"{self._config.exchange_name} (broadcast mode - all messages will "
                f"be delivered to this queue regardless of routing key)",
                extra={
                    "queue_name": self._config.queue_name,
                    "exchange_name": self._config.exchange_name,
                    "exchange_type": self._config.exchange_type,
                    "routing_key": routing_key,
                    "broadcast_mode": True,
                    "routing_key_ignored": True,
                },
            )
        else:
            self._logger.info(
                f"Bound queue {self._config.queue_name} to exchange "
                f"{self._config.exchange_name} with routing key '{routing_key}'",
                extra={
                    "queue_name": self._config.queue_name,
                    "exchange_name": self._config.exchange_name,
                    "exchange_type": self._config.exchange_type,
                    "routing_key": routing_key,
                },
            )

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
        if not self._connected or not self._consumer_queue or not self._exchange:
            raise RuntimeError("Not connected or queue/exchange not initialized")

        # Generate routing key for this event type
        # Use the same pattern as publish: {aggregate_type}.{event_type}
        # For Pydantic models, we need to check model_fields for default values
        aggregate_type = self._get_event_field_default(event_type, "aggregate_type", "Unknown")
        event_type_name = self._get_event_field_default(
            event_type, "event_type", event_type.__name__
        )
        routing_key = f"{aggregate_type}.{event_type_name}"

        await self._consumer_queue.bind(
            exchange=self._exchange,
            routing_key=routing_key,
        )

        self._logger.info(
            f"Added binding for event type {event_type.__name__}",
            extra={
                "queue_name": self._config.queue_name,
                "exchange_name": self._config.exchange_name,
                "exchange_type": self._config.exchange_type,
                "event_type": event_type.__name__,
                "routing_key": routing_key,
            },
        )

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
        if not self._connected or not self._consumer_queue or not self._exchange:
            raise RuntimeError("Not connected or queue/exchange not initialized")

        await self._consumer_queue.bind(
            exchange=self._exchange,
            routing_key=routing_key,
        )

        self._logger.info(
            f"Added binding with routing key '{routing_key}'",
            extra={
                "queue_name": self._config.queue_name,
                "exchange_name": self._config.exchange_name,
                "exchange_type": self._config.exchange_type,
                "routing_key": routing_key,
            },
        )

    async def _declare_dlq(self) -> None:
        """Declare dead letter exchange and queue.

        Creates a DLQ exchange and queue for handling messages that fail
        after max_retries attempts. The DLQ uses a direct exchange type
        for simple routing based on the original queue name.

        This allows failed messages to be inspected and potentially replayed
        after fixing the underlying issue.

        Raises:
            RuntimeError: If channel not initialized
        """
        if not self._channel:
            raise RuntimeError("Channel not initialized")

        # Declare DLQ exchange as direct type
        self._dlq_exchange = await self._channel.declare_exchange(
            name=self._config.dlq_exchange_name,
            type=ExchangeType.DIRECT,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
        )

        self._logger.info(
            f"Declared DLQ exchange: {self._config.dlq_exchange_name}",
            extra={
                "dlq_exchange_name": self._config.dlq_exchange_name,
                "durable": self._config.durable,
            },
        )

        # Build DLQ queue arguments
        dlq_arguments: dict[str, Any] = {}

        if self._config.dlq_message_ttl is not None:
            dlq_arguments["x-message-ttl"] = self._config.dlq_message_ttl

        if self._config.dlq_max_length is not None:
            dlq_arguments["x-max-length"] = self._config.dlq_max_length

        # Declare DLQ queue with optional TTL and max_length
        self._dlq_queue = await self._channel.declare_queue(
            name=self._config.dlq_queue_name,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
            arguments=dlq_arguments if dlq_arguments else None,
        )

        self._logger.info(
            f"Declared DLQ queue: {self._config.dlq_queue_name}",
            extra={
                "dlq_queue_name": self._config.dlq_queue_name,
                "durable": self._config.durable,
                "dlq_message_ttl": self._config.dlq_message_ttl,
                "dlq_max_length": self._config.dlq_max_length,
            },
        )

        # Bind DLQ queue to DLQ exchange
        # Uses the main queue name as routing key (matches x-dead-letter-routing-key)
        await self._dlq_queue.bind(
            exchange=self._dlq_exchange,
            routing_key=self._config.queue_name,
        )

        self._logger.info(
            f"Bound DLQ queue {self._config.dlq_queue_name} to DLQ exchange "
            f"{self._config.dlq_exchange_name} with routing key '{self._config.queue_name}'",
            extra={
                "dlq_queue_name": self._config.dlq_queue_name,
                "dlq_exchange_name": self._config.dlq_exchange_name,
                "routing_key": self._config.queue_name,
            },
        )

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

        Delegates to the shared RetryPolicy so Kafka and RabbitMQ cannot drift
        apart again.

        Args:
            retry_count: Zero-based retry attempt number.

        Returns:
            Delay in seconds, with symmetric jitter applied.
        """
        return self._retry_policy.delay_for(retry_count)

    async def _handle_failed_message(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a failed message - retry with backoff or route to DLQ.

        This method implements the retry logic with exponential backoff. When
        a message fails processing:
        1. If retry_count < max_retries: republish with incremented retry count
           after applying exponential backoff delay
        2. If retry_count >= max_retries: send to DLQ with failure metadata

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Current retry count (from x-retry-count header)
        """
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))

        if retry_count >= self._config.max_retries:
            # Max retries exceeded - send to DLQ
            await self._send_to_dlq(message, error, retry_count)
            await message.ack()  # Ack to remove from main queue

            self._logger.warning(
                f"Message sent to DLQ after {retry_count} retries: {event_type_name}",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "error": str(error),
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
        else:
            # Calculate backoff delay
            delay = self._calculate_retry_delay(retry_count)

            self._logger.info(
                f"Scheduling retry {retry_count + 1}/{self._config.max_retries} "
                f"for {event_type_name} after {delay:.2f}s delay",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "next_retry": retry_count + 1,
                    "max_retries": self._config.max_retries,
                    "delay_seconds": delay,
                },
            )

            # Apply backoff delay
            if delay > 0:
                await asyncio.sleep(delay)

            # Retry - republish with incremented retry count
            await self._republish_for_retry(message, retry_count + 1)
            await message.ack()  # Ack original, republished copy will be processed

            self._logger.info(
                f"Republished message for retry {retry_count + 1}",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count + 1,
                },
            )

    async def _republish_for_retry(
        self,
        original_message: AbstractIncomingMessage,
        new_retry_count: int,
    ) -> None:
        """Republish a message with incremented retry count.

        Creates a new message with updated headers containing the incremented
        retry count and timestamp of the retry attempt. The message body and
        other properties are preserved from the original message.

        Args:
            original_message: The original message to retry
            new_retry_count: The new retry count value

        Raises:
            RuntimeError: If exchange is not initialized
        """
        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        # Copy headers and update retry count
        headers = dict(original_message.headers or {})
        headers["x-retry-count"] = new_retry_count
        headers["x-last-retry-at"] = datetime.now(UTC).isoformat()

        # Create new message with updated headers
        retry_message = Message(
            body=original_message.body,
            content_type=original_message.content_type,
            content_encoding=original_message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=original_message.message_id,
            headers=headers,
        )

        # Republish to exchange with original routing key
        routing_key = original_message.routing_key or ""
        await self._exchange.publish(retry_message, routing_key=routing_key)

    async def _send_to_dlq(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Send a failed message to the dead letter queue.

        Publishes the failed message to the DLQ exchange with additional
        headers containing failure metadata:
        - x-dlq-reason: Error message
        - x-dlq-error-type: Exception class name
        - x-dlq-retry-count: Number of retries before DLQ
        - x-dlq-timestamp: When message was sent to DLQ
        - x-original-routing-key: Original routing key

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Final retry count before DLQ

        Note:
            If DLQ is not enabled or DLQ exchange is not initialized,
            this method logs a warning and returns without action.
        """
        if not self._config.enable_dlq or not self._dlq_exchange:
            self._logger.warning(
                "DLQ not enabled or not initialized, message will be lost",
                extra={"message_id": message.message_id},
            )
            return

        headers = dict(message.headers or {})
        event_type_name = str(headers.get("event_type", "unknown"))

        # Add failure metadata to headers
        headers["x-dlq-reason"] = str(error)
        headers["x-dlq-error-type"] = type(error).__name__
        headers["x-dlq-retry-count"] = retry_count
        headers["x-dlq-timestamp"] = datetime.now(UTC).isoformat()
        headers["x-original-routing-key"] = message.routing_key or ""

        # Create DLQ message with failure metadata
        dlq_message = Message(
            body=message.body,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            headers=headers,
        )

        # Publish to DLQ exchange with queue name as routing key
        await self._dlq_exchange.publish(
            dlq_message,
            routing_key=self._config.queue_name,
        )

        self._stats.messages_sent_to_dlq += 1

        self._logger.warning(
            f"Sent message to DLQ after {retry_count} retries: {event_type_name}",
            extra={
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "error": str(error),
                "error_type": type(error).__name__,
                "dlq_queue": self._config.dlq_queue_name,
            },
        )

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
            await self._publish_batch(events, wait_for_confirm=not background)

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

        if preserve_order:
            # Sequential publishing for order guarantees
            return await self._publish_batch_sequential(events)
        else:
            # Concurrent publishing for performance
            return await self._publish_batch_concurrent(events)

    async def _publish_single(
        self,
        event: DomainEvent,
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish a single event to the exchange with optional tracing.

        Creates an OpenTelemetry span for the publish operation if tracing
        is enabled. The span includes messaging semantic attributes and
        event metadata for distributed tracing correlation.

        Args:
            event: The event to publish
            wait_for_confirm: Whether to wait for publisher confirm.
                            aio-pika handles confirms automatically with RobustConnection,
                            so publish() returns after broker acknowledges receipt.

        Raises:
            RuntimeError: If exchange not initialized
            Exception: If publishing fails
        """
        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        routing_key = self._get_routing_key(event)
        span = None

        # Use Tracer's start_span with SpanKindEnum.PRODUCER for distributed tracing
        # This is needed for context propagation (inject trace context into message)
        if self._enable_tracing and PROPAGATION_AVAILABLE:
            span = self._tracer.start_span(
                "eventsource.event_bus.publish",
                kind=SpanKindEnum.PRODUCER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "rabbitmq",
                    ATTR_MESSAGING_DESTINATION: self._config.exchange_name,
                    "messaging.destination_kind": "exchange",
                    "messaging.rabbitmq.routing_key": routing_key,
                    ATTR_EVENT_TYPE: event.event_type,
                    ATTR_EVENT_ID: str(event.event_id),
                    "aggregate.type": event.aggregate_type,
                    ATTR_AGGREGATE_ID: str(event.aggregate_id),
                },
            )

        try:
            # Create AMQP message from event with optional trace context injection
            message = self._create_message_with_tracing(event, span)

            # Publish to exchange
            # aio-pika's RobustConnection handles publisher confirms automatically
            # The publish() call returns after the broker acknowledges receipt
            await self._exchange.publish(
                message,
                routing_key=routing_key,
            )

            # Update statistics
            self._stats.events_published += 1
            self._stats.last_publish_at = datetime.now(UTC)
            if wait_for_confirm:
                self._stats.publish_confirms += 1

            if span:
                span.set_status(Status(StatusCode.OK))

            self._logger.debug(
                f"Published {event.event_type}",
                extra={
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": str(event.aggregate_id),
                    "routing_key": routing_key,
                    "wait_for_confirm": wait_for_confirm,
                },
            )

        except Exception as e:
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            self._logger.error(
                f"Failed to publish {event.event_type}: {e}",
                exc_info=True,
                extra={
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": str(event.aggregate_id),
                    "routing_key": routing_key,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise

        finally:
            if span:
                span.end()

    async def _publish_single_no_stats(
        self,
        event: DomainEvent,
    ) -> None:
        """Publish a single event without updating statistics.

        This is an internal method used by batch publishing to avoid
        double-counting statistics. The batch method updates stats
        for all events at once after the batch completes.

        Args:
            event: The event to publish

        Raises:
            RuntimeError: If exchange not initialized
            Exception: If publishing fails
        """
        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        routing_key = self._get_routing_key(event)
        message = self._create_message(event)

        await self._exchange.publish(
            message,
            routing_key=routing_key,
        )

    async def _publish_batch(
        self,
        events: list[DomainEvent],
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish multiple events with batch optimization.

        Internal method used by publish() for multiple events.
        Uses asyncio.gather for concurrent publishing with chunking
        to prevent overwhelming the broker.

        Creates a parent span for the batch operation when tracing is enabled,
        providing observability into batch publish performance.

        Args:
            events: Events to publish
            wait_for_confirm: Whether to wait for confirms

        Raises:
            RuntimeError: If exchange not initialized
            Exception: Re-raises first error encountered in batch
        """
        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        total_events = len(events)
        chunk_size = self._config.batch_size
        max_concurrent = self._config.max_concurrent_publishes

        self._logger.debug(
            f"Publishing batch of {total_events} events",
            extra={
                "batch_size": total_events,
                "chunk_size": chunk_size,
                "max_concurrent": max_concurrent,
            },
        )

        # Create parent span for batch operation if tracing is enabled
        span = None
        if self._enable_tracing and PROPAGATION_AVAILABLE:
            span = self._tracer.start_span(
                "eventsource.event_bus.publish_batch",
                kind=SpanKindEnum.PRODUCER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "rabbitmq",
                    ATTR_MESSAGING_DESTINATION: self._config.exchange_name,
                    ATTR_EVENT_COUNT: total_events,
                    "messaging.destination_kind": "exchange",
                    "messaging.batch.size": total_events,
                    "messaging.batch.chunk_size": chunk_size,
                    "messaging.batch.max_concurrent": max_concurrent,
                },
            )

        try:
            # Track batch stats
            self._stats.batch_publishes += 1
            published_count = 0
            errors: list[Exception] = []

            # Process in chunks to prevent overwhelming the broker
            for chunk_start in range(0, total_events, chunk_size):
                chunk_end = min(chunk_start + chunk_size, total_events)
                chunk = events[chunk_start:chunk_end]

                # Within each chunk, limit concurrency
                chunk_published = await self._publish_chunk_concurrent(
                    chunk, max_concurrent, errors
                )
                published_count += chunk_published

            # Update statistics
            self._stats.events_published += published_count
            self._stats.batch_events_published += published_count
            self._stats.last_publish_at = datetime.now(UTC)

            if wait_for_confirm:
                self._stats.publish_confirms += published_count

            if errors:
                self._stats.batch_partial_failures += 1
                self._logger.error(
                    f"Batch publish had {len(errors)} failures out of {total_events} events",
                    extra={
                        "failures": len(errors),
                        "published": published_count,
                        "total": total_events,
                    },
                )
                if span:
                    span.set_attribute("messaging.batch.published", published_count)
                    span.set_attribute("messaging.batch.failed", len(errors))
                    span.set_status(Status(StatusCode.ERROR, f"{len(errors)} events failed"))
                # Raise the first error to indicate batch failure
                raise errors[0]

            self._logger.debug(
                f"Successfully published batch of {total_events} events",
                extra={"batch_size": total_events, "published": published_count},
            )

            if span:
                span.set_attribute("messaging.batch.published", published_count)
                span.set_status(Status(StatusCode.OK))

        except Exception as e:
            if span:
                span.record_exception(e)
                if not errors:  # Only set error status if not already set above
                    span.set_status(Status(StatusCode.ERROR, str(e)))
            raise

        finally:
            if span:
                span.end()

    async def _publish_chunk_concurrent(
        self,
        events: list[DomainEvent],
        max_concurrent: int,
        errors: list[Exception],
    ) -> int:
        """Publish a chunk of events concurrently with concurrency limit.

        Uses asyncio.Semaphore to limit the number of concurrent publish
        operations, preventing resource exhaustion.

        Args:
            events: Events in this chunk to publish
            max_concurrent: Maximum concurrent publish operations
            errors: List to append any errors to

        Returns:
            Number of successfully published events in this chunk
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def publish_with_semaphore(event: DomainEvent) -> bool:
            """Publish a single event with semaphore control."""
            async with semaphore:
                try:
                    await self._publish_single_no_stats(event)
                    return True
                except Exception as e:
                    errors.append(e)
                    self._logger.warning(
                        f"Failed to publish event in batch: {e}",
                        extra={
                            "event_id": str(event.event_id),
                            "event_type": event.event_type,
                            "error": str(e),
                        },
                    )
                    return False

        # Execute all publishes concurrently (up to semaphore limit)
        results = await asyncio.gather(
            *[publish_with_semaphore(event) for event in events],
            return_exceptions=False,  # Exceptions are caught in publish_with_semaphore
        )

        # Count successful publishes
        return sum(1 for result in results if result)

    async def _publish_batch_concurrent(
        self,
        events: list[DomainEvent],
    ) -> dict[str, int]:
        """Publish events concurrently with detailed result tracking.

        Public batch method implementation for concurrent publishing.

        Args:
            events: Events to publish

        Returns:
            Dictionary with batch statistics
        """
        total_events = len(events)
        chunk_size = self._config.batch_size
        max_concurrent = self._config.max_concurrent_publishes

        self._logger.debug(
            f"Batch publishing {total_events} events (concurrent)",
            extra={
                "batch_size": total_events,
                "chunk_size": chunk_size,
                "max_concurrent": max_concurrent,
            },
        )

        # Track stats
        self._stats.batch_publishes += 1
        published_count = 0
        errors: list[Exception] = []
        num_chunks = 0

        # Process in chunks
        for chunk_start in range(0, total_events, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_events)
            chunk = events[chunk_start:chunk_end]
            num_chunks += 1

            chunk_published = await self._publish_chunk_concurrent(chunk, max_concurrent, errors)
            published_count += chunk_published

            self._logger.debug(
                f"Published chunk {num_chunks} ({chunk_published}/{len(chunk)} events)",
                extra={
                    "chunk_number": num_chunks,
                    "chunk_published": chunk_published,
                    "chunk_total": len(chunk),
                    "total_published": published_count,
                },
            )

        # Update statistics
        self._stats.events_published += published_count
        self._stats.batch_events_published += published_count
        self._stats.last_publish_at = datetime.now(UTC)
        self._stats.publish_confirms += published_count

        failed_count = total_events - published_count
        if failed_count > 0:
            self._stats.batch_partial_failures += 1

        result = {
            "total": total_events,
            "published": published_count,
            "failed": failed_count,
            "chunks": num_chunks,
        }

        self._logger.info(
            f"Batch publish completed: {published_count}/{total_events} events",
            extra=result,
        )

        # Raise BatchPublishError if any failures
        if errors:
            raise BatchPublishError(
                f"Batch publish had {len(errors)} failures",
                results=result,
                errors=errors,
            )

        return result

    async def _publish_batch_sequential(
        self,
        events: list[DomainEvent],
    ) -> dict[str, int]:
        """Publish events sequentially to preserve order.

        Used when preserve_order=True in publish_batch().
        Slower than concurrent publishing but guarantees event ordering.

        Args:
            events: Events to publish in order

        Returns:
            Dictionary with batch statistics
        """
        total_events = len(events)

        self._logger.debug(
            f"Batch publishing {total_events} events (sequential/ordered)",
            extra={"batch_size": total_events},
        )

        # Track stats
        self._stats.batch_publishes += 1
        published_count = 0
        errors: list[Exception] = []

        for event in events:
            try:
                await self._publish_single_no_stats(event)
                published_count += 1
            except Exception as e:
                errors.append(e)
                self._logger.warning(
                    f"Failed to publish event in ordered batch: {e}",
                    extra={
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "error": str(e),
                    },
                )

        # Update statistics
        self._stats.events_published += published_count
        self._stats.batch_events_published += published_count
        self._stats.last_publish_at = datetime.now(UTC)
        self._stats.publish_confirms += published_count

        failed_count = total_events - published_count
        if failed_count > 0:
            self._stats.batch_partial_failures += 1

        result = {
            "total": total_events,
            "published": published_count,
            "failed": failed_count,
            "chunks": 1,  # Sequential is always one "chunk"
        }

        self._logger.info(
            f"Ordered batch publish completed: {published_count}/{total_events} events",
            extra=result,
        )

        # Raise BatchPublishError if any failures
        if errors:
            raise BatchPublishError(
                f"Ordered batch publish had {len(errors)} failures",
                results=result,
                errors=errors,
            )

        return result

    # =========================================================================
    # Consumer Loop Methods (P1-010)
    # =========================================================================

    async def start_consuming(self) -> None:
        """Start consuming events from the RabbitMQ queue.

        This method runs continuously, consuming messages from the queue
        and dispatching them to registered handlers.

        The consumer runs until stop_consuming() is called or the
        connection is lost.

        Raises:
            RuntimeError: If not connected and connection fails, or if
                         consumer queue not initialized
        """
        if not self._connected:
            await self.connect()

        if not self._consumer_queue:
            raise RuntimeError("Consumer queue not initialized")

        self._consuming = True
        consumer_name = self._config.consumer_name

        self._logger.info(
            f"Starting RabbitMQ consumer: {consumer_name}",
            extra={
                "consumer_name": consumer_name,
                "queue": self._config.queue_name,
                "prefetch_count": self._config.prefetch_count,
            },
        )

        try:
            # Use queue iterator for consuming
            async with self._consumer_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self._consuming:
                        break

                    await self._process_message(message)

        except asyncio.CancelledError:
            self._logger.info(
                "Consumer loop cancelled",
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                },
            )
        except Exception as e:
            self._logger.error(
                f"Error in consumer loop: {e}",
                exc_info=True,
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise
        finally:
            self._consuming = False
            self._logger.info(
                "Consumer loop stopped",
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                    "events_consumed": self._stats.events_consumed,
                    "events_processed_success": self._stats.events_processed_success,
                    "events_processed_failed": self._stats.events_processed_failed,
                },
            )

    async def stop_consuming(self) -> None:
        """Stop the consumer loop gracefully.

        Sets the consuming flag to False, which will cause the
        consumer loop to exit after processing the current message.
        """
        self._consuming = False
        self._logger.info(
            "Stop consuming requested",
            extra={
                "consumer_name": self._config.consumer_name,
                "queue": self._config.queue_name,
            },
        )

    def start_consuming_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task.

        Returns:
            The background task running the consumer

        Raises:
            RuntimeError: If consumer is already running in background

        Usage:
            task = bus.start_consuming_in_background()
            # ... do other work ...
            await bus.stop_consuming()
            await task
        """
        if self._consumer_task is not None:
            raise RuntimeError("Consumer already running in background")

        self._consumer_task = asyncio.create_task(
            self.start_consuming(),
            name=f"rabbitmq-consumer-{self._config.consumer_name}",
        )
        return self._consumer_task

    async def _process_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        """Process a single message from the queue with retry handling and tracing.

        Deserializes the event and dispatches to registered handlers.
        Tracks retry count via x-retry-count header and implements
        exponential backoff before DLQ routing.

        When tracing is enabled, creates a consumer span that extracts
        trace context from message headers for distributed tracing correlation.

        On success: acknowledges the message.
        On failure:
        - If retries remaining: republish with incremented retry count
        - If max_retries exceeded: send to DLQ with failure metadata

        Also tracks DLQ-related information from x-death headers for
        observability and debugging purposes.

        Args:
            message: The incoming AMQP message
        """
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))
        # Extract retry count with type-safe conversion
        # Header values can be various types, so we ensure numeric conversion
        retry_count_value = headers.get("x-retry-count")
        if retry_count_value is None:
            retry_count = 0
        elif isinstance(retry_count_value, int):
            retry_count = retry_count_value
        else:
            # Handle string or other numeric types
            retry_count = int(str(retry_count_value))

        # Extract death info for logging and tracking
        death_info = self.get_death_info(message)
        is_redelivered = self.is_from_dlq(message)

        log_extra: dict[str, Any] = {
            "message_id": message.message_id,
            "event_type": event_type_name,
            "routing_key": message.routing_key,
            "retry_count": retry_count,
        }

        # Add death info if message was dead-lettered
        if is_redelivered:
            log_extra.update(
                {
                    "is_dead_lettered": True,
                    "death_count": death_info["death_count"],
                    "first_death_queue": death_info["first_death_queue"],
                    "first_death_reason": death_info["first_death_reason"],
                    "original_routing_key": death_info["original_routing_key"],
                }
            )
            self._logger.info(
                f"Processing dead-lettered message: {event_type_name}",
                extra=log_extra,
            )
        else:
            self._logger.debug(
                f"Processing message (attempt {retry_count + 1}): {event_type_name}",
                extra=log_extra,
            )

        processing_start = datetime.now(UTC)

        # Set up tracing if enabled with context extraction for distributed tracing
        span = None
        ctx = None

        # Use Tracer's start_span with SpanKindEnum.CONSUMER for distributed tracing
        # Extract trace context from message headers to link consumer span to publisher span
        if self._enable_tracing and PROPAGATION_AVAILABLE:
            # Extract trace context from message headers for distributed tracing
            if extract is not None:
                ctx = extract(dict(headers))

            span = self._tracer.start_span(
                "eventsource.event_bus.consume",
                kind=SpanKindEnum.CONSUMER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "rabbitmq",
                    ATTR_MESSAGING_DESTINATION: self._config.queue_name,
                    "messaging.destination_kind": "queue",
                    "messaging.message_id": message.message_id or "",
                    ATTR_EVENT_TYPE: event_type_name,
                    "messaging.rabbitmq.routing_key": message.routing_key or "",
                },
                context=ctx,
            )

        try:
            # Deserialize event
            event = self._deserialize_event(message)

            if event is None:
                # Unknown event type - acknowledge to prevent blocking
                self._logger.warning(
                    f"Unknown event type: {event_type_name}, acknowledging to skip",
                    extra={
                        "event_type": event_type_name,
                        "message_id": message.message_id,
                    },
                )
                if span:
                    span.set_attribute("event.unknown_type", True)
                    span.set_status(Status(StatusCode.OK, "Unknown event type"))
                await message.ack()
                return

            if span:
                span.set_attribute("event.id", str(event.event_id))

            # Dispatch to handlers with tracing
            await self._dispatch_event(event, message, span)

            # Acknowledge successful processing
            await message.ack()

            self._stats.events_consumed += 1
            self._stats.events_processed_success += 1
            self._stats.last_consume_at = datetime.now(UTC)

            processing_duration = (datetime.now(UTC) - processing_start).total_seconds()

            if span:
                span.set_status(Status(StatusCode.OK))

            self._logger.debug(
                f"Successfully processed {event_type_name}",
                extra={
                    "message_id": message.message_id,
                    "event_id": str(event.event_id),
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "duration_ms": processing_duration * 1000,
                    "success": True,
                },
            )

        except Exception as e:
            self._stats.events_processed_failed += 1
            self._stats.last_error_at = datetime.now(UTC)

            processing_duration = (datetime.now(UTC) - processing_start).total_seconds()
            error_extra: dict[str, Any] = {
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "duration_ms": processing_duration * 1000,
                "error": str(e),
                "error_type": type(e).__name__,
            }

            # Include death info in error logging
            if is_redelivered:
                error_extra.update(
                    {
                        "is_dead_lettered": True,
                        "death_count": death_info["death_count"],
                        "first_death_queue": death_info["first_death_queue"],
                    }
                )

            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            self._logger.error(
                f"Failed to process message: {e}",
                exc_info=True,
                extra=error_extra,
            )

            # Handle retry or DLQ routing. Unwrap a single-failure
            # HandlerDispatchError so retry/DLQ metadata (x-dlq-error-type,
            # etc.) still reflects the handler's own exception type rather
            # than the aggregate wrapper -- error isolation changes how
            # dispatch runs handlers, not what gets reported for a single
            # failing handler.
            dlq_error: Exception = e
            if isinstance(e, HandlerDispatchError) and len(e.failures) == 1:
                dlq_error = e.failures[0][1]
            await self._handle_failed_message(message, dlq_error, retry_count)

        finally:
            if span:
                span.end()

    async def _dispatch_event(
        self,
        event: DomainEvent,
        message: AbstractIncomingMessage,
        parent_span: Any = None,
    ) -> None:
        """Dispatch an event to all matching handlers with optional tracing.

        Invokes handlers for the specific event type and wildcard handlers.
        When tracing is enabled and a parent span is provided, creates child
        spans for each handler execution.

        Args:
            event: The deserialized domain event
            message: Original AMQP message for context
            parent_span: Optional parent span for tracing. If provided and
                        tracing is enabled, child spans are created for each
                        handler execution.

        Raises:
            HandlerDispatchError: If one or more handlers raise. Every handler
                still runs for this delivery (error isolation); failures are
                collected and raised together afterward so the message is
                still rejected for retry/DLQ exactly as a single raise would
                have done.
        """
        event_type = type(event)

        # Get all handlers
        handlers = self._handlers_for(event_type)

        if not handlers:
            self._logger.warning(
                f"No handlers registered for {event.event_type}",
                extra={"event_type": event.event_type},
            )
            return

        # Add handler count to parent span for observability
        if parent_span is not None:
            parent_span.set_attribute(ATTR_HANDLER_COUNT, len(handlers))

        self._logger.debug(
            f"Dispatching {event.event_type} to {len(handlers)} handler(s)",
            extra={
                "event_type": event.event_type,
                "event_id": str(event.event_id),
                "handler_count": len(handlers),
            },
        )

        # Process handlers sequentially for ordering guarantees. Every handler
        # runs for this delivery even if an earlier one failed (error
        # isolation) -- failures are collected and raised together as one
        # HandlerDispatchError afterward.
        failures: list[tuple[str, Exception]] = []
        for adapter in handlers:
            handler_name = adapter.name
            handler_start = datetime.now(UTC)
            handler_span = None

            # Create handler span if tracing is enabled (using composition-based tracer)
            if self._enable_tracing and parent_span and PROPAGATION_AVAILABLE:
                handler_span = self._tracer.start_span(
                    "eventsource.event_bus.handle",
                    kind=SpanKindEnum.INTERNAL,
                    attributes={
                        ATTR_HANDLER_NAME: handler_name,
                        ATTR_EVENT_TYPE: event.event_type,
                        ATTR_EVENT_ID: str(event.event_id),
                    },
                )

            try:
                await adapter.handle(event)

                handler_duration = (datetime.now(UTC) - handler_start).total_seconds()

                if handler_span:
                    handler_span.set_attribute("handler.duration_ms", handler_duration * 1000)
                    handler_span.set_attribute(ATTR_HANDLER_SUCCESS, True)
                    handler_span.set_status(Status(StatusCode.OK))

                self._logger.debug(
                    f"Handler {handler_name} processed {event.event_type}",
                    extra={
                        "handler": handler_name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "duration_ms": handler_duration * 1000,
                    },
                )

            except Exception as e:
                self._stats.handler_errors += 1
                self._stats.last_error_at = datetime.now(UTC)
                handler_duration = (datetime.now(UTC) - handler_start).total_seconds()

                if handler_span:
                    handler_span.set_attribute(ATTR_HANDLER_SUCCESS, False)
                    handler_span.set_status(Status(StatusCode.ERROR, str(e)))
                    handler_span.record_exception(e)

                self._logger.error(
                    f"Handler {handler_name} failed: {e}",
                    exc_info=True,
                    extra={
                        "handler": handler_name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "message_id": message.message_id,
                        "duration_ms": handler_duration * 1000,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                failures.append((handler_name, e))

            finally:
                if handler_span:
                    handler_span.end()

        if failures:
            raise HandlerDispatchError(failures)

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

        This method signals the consumer loop to stop by setting _consuming
        to False, then waits for the consumer task to complete. If the task
        doesn't complete within the timeout, it is cancelled.

        Args:
            timeout: Maximum time to wait for the consumer to stop.
                    Half of this time is used for graceful stop, the other
                    half for cancellation if needed.

        Raises:
            asyncio.TimeoutError: If the consumer doesn't stop within timeout
        """
        if not self._consuming:
            self._logger.debug("Not consuming, skipping consumer stop")
            return

        self._logger.debug("Stopping consumer...")

        # Signal consumer to stop
        self._consuming = False

        # Wait for consumer task if running
        if self._consumer_task:
            try:
                # Use half timeout for graceful wait, reserve half for cleanup
                await asyncio.wait_for(
                    asyncio.shield(self._consumer_task),
                    timeout=timeout / 2,
                )
                self._logger.debug("Consumer task completed gracefully")
            except TimeoutError:
                self._logger.warning(
                    "Consumer task did not stop in time, cancelling",
                    extra={"timeout": timeout / 2},
                )
                self._consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._consumer_task
                self._logger.debug("Consumer task cancelled successfully")
            except asyncio.CancelledError:
                self._logger.debug("Consumer task was already cancelled")
            finally:
                self._consumer_task = None

        self._logger.debug("Consumer stopped")

    async def _drain_in_flight(self, timeout: float) -> None:
        """Wait for any in-flight message processing to complete.

        This is a simple implementation that waits a portion of the timeout
        to allow any ongoing message handlers to complete. In the current
        implementation, handlers run synchronously within the consumer loop,
        so once the consumer stops, no handlers should be running.

        A more sophisticated implementation could track active handlers
        with a counter or semaphore for more precise draining.

        Args:
            timeout: Maximum time available for draining.
                    Actual drain time is min(timeout / 4, 5.0) seconds.
        """
        # In current implementation, handlers run synchronously in consumer loop
        # So if consumer is stopped, no handlers are running
        # This is a placeholder for future async handler support
        drain_time = min(timeout / 4, 5.0)  # Wait up to 5 seconds

        if drain_time > 0:
            self._logger.debug(
                f"Draining in-flight messages ({drain_time:.2f}s)",
                extra={"drain_time_seconds": drain_time},
            )
            await asyncio.sleep(drain_time)
            self._logger.debug("Drain period completed")

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
