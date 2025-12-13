"""Redis event bus implementation using Redis Streams.

This module provides a distributed event bus implementation using Redis Streams
for durable event distribution across multiple processes and servers.

Features:
- Durable event storage (events survive restarts)
- Consumer groups for load balancing
- At-least-once delivery guarantees
- Event replay capability
- Horizontal scaling support
- Pipeline optimization for batch publishing
- Dead letter queue for unrecoverable failures
- Pending message recovery with configurable idle time
- Optional OpenTelemetry tracing

Example:
    >>> from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig
    >>>
    >>> config = RedisEventBusConfig(
    ...     redis_url="redis://localhost:6379",
    ...     stream_prefix="myapp",
    ...     consumer_group="projections",
    ... )
    >>> bus = RedisEventBus(config=config, event_registry=my_registry)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])
    >>> await bus.start_consuming()
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.bus.interface import (
    EventBus,
    EventHandlerFunc,
)
from eventsource.events.base import DomainEvent
from eventsource.handlers.adapter import HandlerAdapter
from eventsource.observability import Tracer, create_tracer
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
from eventsource.protocols import (
    FlexibleEventHandler,
    FlexibleEventSubscriber,
)

if TYPE_CHECKING:
    from eventsource.events.registry import EventRegistry

# Optional Redis import - fail gracefully if not installed
try:
    import redis.asyncio as aioredis
    from redis.asyncio import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import ResponseError

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    aioredis = None  # type: ignore[assignment]
    Redis = None  # type: ignore[assignment, misc]
    RedisConnectionError = Exception  # type: ignore[assignment, misc]
    ResponseError = Exception  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)


class RedisNotAvailableError(ImportError):
    """Raised when redis package is not installed."""

    def __init__(self) -> None:
        super().__init__(
            "Redis package is not installed. Install it with: pip install eventsource[redis]"
        )


@dataclass
class RedisEventBusConfig:
    """Configuration for Redis event bus.

    Attributes:
        redis_url: Redis connection URL (e.g., "redis://localhost:6379")
        stream_prefix: Prefix for Redis stream names (default: "events")
        consumer_group: Name of the consumer group (default: "default")
        consumer_name: Name of this consumer instance (auto-generated if None)
        batch_size: Maximum events to read per iteration (default: 100)
        block_ms: Milliseconds to block waiting for new messages (default: 5000)
        max_retries: Maximum retries before sending to DLQ (default: 3)
        pending_idle_ms: Minimum idle time before claiming pending messages (default: 60000)
        enable_dlq: Whether to enable dead letter queue (default: True)
        dlq_stream_suffix: Suffix for DLQ stream name (default: "_dlq")
        socket_timeout: Socket timeout in seconds (default: 5.0)
        socket_connect_timeout: Socket connection timeout in seconds (default: 5.0)
        enable_tracing: Enable OpenTelemetry tracing if available (default: True)
        retry_key_prefix: Prefix for retry count keys (default: "retry")
        retry_key_expiry_seconds: Expiry for retry count keys (default: 86400 = 24h)
        single_connection_client: Use single connection instead of pool (default: False).
            Useful for testing to avoid event loop issues.
    """

    redis_url: str = "redis://localhost:6379"
    stream_prefix: str = "events"
    consumer_group: str = "default"
    consumer_name: str | None = None
    batch_size: int = 100
    block_ms: int = 5000
    max_retries: int = 3
    pending_idle_ms: int = 60000  # 1 minute
    enable_dlq: bool = True
    dlq_stream_suffix: str = "_dlq"
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    enable_tracing: bool = True
    retry_key_prefix: str = "retry"
    retry_key_expiry_seconds: int = 86400  # 24 hours
    single_connection_client: bool = False

    def __post_init__(self) -> None:
        """Generate consumer name if not provided."""
        if self.consumer_name is None:
            hostname = socket.gethostname()
            unique_id = str(uuid.uuid4())[:8]
            self.consumer_name = f"{hostname}-{unique_id}"

    @property
    def stream_name(self) -> str:
        """Get the main stream name."""
        return f"{self.stream_prefix}:stream"

    @property
    def dlq_stream_name(self) -> str:
        """Get the dead letter queue stream name."""
        return f"{self.stream_prefix}:stream{self.dlq_stream_suffix}"

    def get_retry_key(self, message_id: str) -> str:
        """Get the Redis key for tracking retry count."""
        return f"{self.stream_prefix}:{self.retry_key_prefix}:{message_id}"


@dataclass
class RedisEventBusStats:
    """Statistics for Redis event bus operations.

    Attributes:
        events_published: Total events published
        events_consumed: Total events consumed
        events_processed_success: Events processed successfully
        events_processed_failed: Events that failed processing
        messages_recovered: Messages recovered from pending
        messages_sent_to_dlq: Messages sent to dead letter queue
        handler_errors: Total handler errors
        reconnections: Number of reconnection attempts
    """

    events_published: int = 0
    events_consumed: int = 0
    events_processed_success: int = 0
    events_processed_failed: int = 0
    messages_recovered: int = 0
    messages_sent_to_dlq: int = 0
    handler_errors: int = 0
    reconnections: int = 0


class RedisEventBus(EventBus):
    """
    Event bus using Redis Streams for durable event distribution.

    This implementation provides distributed event delivery with:
    - At-least-once delivery guarantees via consumer groups
    - Horizontal scaling via multiple consumers
    - Automatic recovery of pending messages
    - Dead letter queue for failed messages
    - Pipeline optimization for batch publishing

    Thread Safety:
        - Subscription methods are thread-safe
        - Publishing and consuming should only be called from async context

    Example:
        >>> from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig
        >>> from eventsource.events.registry import EventRegistry
        >>>
        >>> config = RedisEventBusConfig(redis_url="redis://localhost:6379")
        >>> registry = EventRegistry()
        >>> bus = RedisEventBus(config=config, event_registry=registry)
        >>>
        >>> await bus.connect()
        >>> bus.subscribe(OrderCreated, order_handler)
        >>> await bus.publish([OrderCreated(...)])
        >>> await bus.start_consuming()
    """

    def __init__(
        self,
        config: RedisEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        """
        Initialize the Redis event bus.

        Args:
            config: Configuration for the Redis event bus.
                   Defaults to RedisEventBusConfig() with default values.
            event_registry: Event registry for deserializing events.
                          If None, uses the default registry.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on config.enable_tracing setting.

        Raises:
            RedisNotAvailableError: If redis package is not installed
        """
        if not REDIS_AVAILABLE:
            raise RedisNotAvailableError()

        self._config = config or RedisEventBusConfig()
        self._event_registry = event_registry
        self._redis: Redis | None = None
        self._connected = False
        self._consuming = False

        # Subscriber management
        self._subscribers: dict[type[DomainEvent], list[HandlerAdapter]] = defaultdict(list)
        self._all_event_handlers: list[HandlerAdapter] = []
        self._lock = asyncio.Lock()

        # Statistics
        self._stats = RedisEventBusStats()

        # Background tasks
        self._consumer_task: asyncio.Task[None] | None = None

        # Initialize tracing via composition (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, self._config.enable_tracing)
        self._enable_tracing = self._tracer.enabled

    @property
    def config(self) -> RedisEventBusConfig:
        """Get the configuration."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Check if connected to Redis."""
        return self._connected

    @property
    def is_consuming(self) -> bool:
        """Check if currently consuming events."""
        return self._consuming

    @property
    def stats(self) -> RedisEventBusStats:
        """Get current statistics."""
        return self._stats

    async def connect(self) -> None:
        """
        Connect to Redis and create consumer group.

        Creates the consumer group if it doesn't exist.

        Raises:
            RedisConnectionError: If connection fails
        """
        if self._connected:
            logger.warning("RedisEventBus already connected")
            return

        try:
            self._redis = await aioredis.from_url(  # type: ignore[no-untyped-call]
                self._config.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=self._config.socket_timeout,
                socket_connect_timeout=self._config.socket_connect_timeout,
                single_connection_client=self._config.single_connection_client,
            )

            # Test connection
            if self._redis is None:
                raise RuntimeError("Redis client not initialized")
            await self._redis.ping()
            logger.info(
                "Connected to Redis",
                extra={
                    "redis_url": self._config.redis_url,
                    "stream": self._config.stream_name,
                },
            )

            # Create consumer group (ignore error if exists)
            await self._ensure_consumer_group_exists()

            self._connected = True

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._consumer_task:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None

        if self._redis:
            await self._redis.aclose()
            self._redis = None
            self._connected = False
            self._consuming = False
            logger.info("Disconnected from Redis")

    async def _ensure_consumer_group_exists(self) -> None:
        """
        Ensure the consumer group exists, creating it if necessary.

        This is called during connect and before consuming to handle cases where:
        - The stream was deleted externally
        - The consumer group doesn't exist yet
        - Redis was restarted without persistence
        """
        if not self._redis:
            return

        try:
            await self._redis.xgroup_create(
                name=self._config.stream_name,
                groupname=self._config.consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                f"Created consumer group '{self._config.consumer_group}' "
                f"on stream '{self._config.stream_name}'"
            )
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                # Group already exists, this is fine
                logger.debug(f"Consumer group '{self._config.consumer_group}' already exists")
            else:
                logger.error(f"Failed to create consumer group: {e}")
                raise

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """
        Publish events to Redis stream.

        Events are serialized to JSON and added to the stream.
        Consumer workers will pick them up for processing.

        Uses pipeline for batch operations (10-100x faster for multiple events).

        Args:
            events: Events to publish
            background: Ignored for Redis (Redis is inherently async)

        Raises:
            RedisConnectionError: If Redis connection fails
        """
        if not events:
            return

        if not self._connected:
            await self.connect()

        if not self._redis:
            raise RuntimeError("Redis client not initialized")

        with self._tracer.span(
            "eventsource.event_bus.publish",
            {
                ATTR_EVENT_COUNT: len(events),
                ATTR_MESSAGING_SYSTEM: "redis",
                ATTR_MESSAGING_DESTINATION: self._config.stream_name,
            },
        ) as span:
            try:
                if len(events) > 1:
                    # Use pipeline for batch operations
                    await self._publish_batch(events)
                else:
                    # Single event - no need for pipeline overhead
                    await self._publish_single(events[0])

                if span:
                    span.set_attribute("publish.success", True)

            except Exception as e:
                if span:
                    span.set_attribute("publish.success", False)
                    span.record_exception(e)
                raise

    async def _publish_single(self, event: DomainEvent) -> str:
        """Publish a single event to the stream."""
        if not self._redis:
            raise RuntimeError("Redis client not initialized")

        event_data = self._serialize_event(event)

        message_id = await self._redis.xadd(
            name=self._config.stream_name,
            fields=event_data,  # type: ignore[arg-type]
        )

        self._stats.events_published += 1

        logger.debug(
            f"Published {event.event_type} to Redis stream",
            extra={
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "message_id": message_id,
                "stream": self._config.stream_name,
            },
        )

        return str(message_id)

    async def _publish_batch(self, events: list[DomainEvent]) -> list[str]:
        """Publish multiple events using pipeline for efficiency."""
        if not self._redis:
            raise RuntimeError("Redis client not initialized")

        async with self._redis.pipeline(transaction=False) as pipe:
            for event in events:
                event_data = self._serialize_event(event)
                pipe.xadd(name=self._config.stream_name, fields=event_data)  # type: ignore[arg-type]

            # Execute all XADDs in a single network round-trip
            message_ids = await pipe.execute()

        # Record metrics and log for all events
        for i, event in enumerate(events):
            self._stats.events_published += 1
            logger.debug(
                f"Published {event.event_type} to Redis stream (batch)",
                extra={
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "message_id": message_ids[i] if i < len(message_ids) else None,
                    "stream": self._config.stream_name,
                    "batch_size": len(events),
                },
            )

        return [str(mid) for mid in message_ids]

    def _serialize_event(self, event: DomainEvent) -> dict[str, str]:
        """Serialize an event to Redis-compatible format."""
        return {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "aggregate_id": str(event.aggregate_id),
            "aggregate_type": event.aggregate_type,
            "tenant_id": str(event.tenant_id) if event.tenant_id else "",
            "occurred_at": event.occurred_at.isoformat(),
            "payload": event.model_dump_json(),
        }

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to a specific event type.

        Note: For Redis, subscriptions are registered locally.
        The actual consumption happens via start_consuming().

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable
        """
        adapter = HandlerAdapter(handler)

        # Use asyncio.Lock in non-async context requires different approach
        # Since subscribe is sync, we use a simple list append (thread-safe for GIL)
        self._subscribers[event_type].append(adapter)

        logger.info(
            f"Registered handler {adapter.name} for {event_type.__name__}",
            extra={
                "handler": adapter.name,
                "event_type": event_type.__name__,
            },
        )

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from a specific event type.

        Args:
            event_type: The event class to unsubscribe from
            handler: The handler to remove

        Returns:
            True if the handler was found and removed, False otherwise
        """
        target_adapter = HandlerAdapter(handler)

        adapters = self._subscribers.get(event_type, [])
        for i, adapter in enumerate(adapters):
            if adapter == target_adapter:
                adapters.pop(i)
                logger.info(
                    f"Unsubscribed handler {adapter.name} from {event_type.__name__}",
                    extra={
                        "handler": adapter.name,
                        "event_type": event_type.__name__,
                    },
                )
                return True

        logger.debug(
            f"Handler {target_adapter.name} not found for {event_type.__name__}",
            extra={
                "handler": target_adapter.name,
                "event_type": event_type.__name__,
            },
        )
        return False

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        """
        Subscribe a FlexibleEventSubscriber to all its declared event types.

        Args:
            subscriber: The subscriber to register
        """
        event_types = subscriber.subscribed_to()
        for event_type in event_types:
            self.subscribe(event_type, subscriber)

    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to all event types (wildcard subscription).

        The handler will receive every event published to the bus,
        regardless of event type.

        Args:
            handler: Handler that will receive all events
        """
        adapter = HandlerAdapter(handler)

        self._all_event_handlers.append(adapter)

        logger.info(
            f"Registered wildcard handler {adapter.name}",
            extra={"handler": adapter.name},
        )

    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from the wildcard subscription.

        Args:
            handler: The handler to remove from wildcard subscriptions

        Returns:
            True if the handler was found and removed, False otherwise
        """
        target_adapter = HandlerAdapter(handler)

        for i, adapter in enumerate(self._all_event_handlers):
            if adapter == target_adapter:
                self._all_event_handlers.pop(i)
                logger.info(
                    f"Unsubscribed wildcard handler {adapter.name}",
                    extra={"handler": adapter.name},
                )
                return True

        logger.debug(
            f"Wildcard handler {target_adapter.name} not found",
            extra={"handler": target_adapter.name},
        )
        return False

    async def start_consuming(
        self,
        consumer_name: str | None = None,
    ) -> None:
        """
        Start consuming events from Redis stream.

        This method runs continuously, reading events from the stream
        and dispatching them to registered handlers.

        Args:
            consumer_name: Optional override for consumer name from config

        Raises:
            RedisConnectionError: If connection is lost and cannot be recovered
        """
        if not self._connected:
            await self.connect()

        if not self._redis:
            raise RuntimeError("Redis client not initialized")

        # Ensure consumer group exists
        await self._ensure_consumer_group_exists()

        actual_consumer_name = consumer_name or self._config.consumer_name
        if actual_consumer_name is None:
            raise ValueError("Consumer name must be set")
        self._consuming = True

        logger.info(
            f"Starting Redis event consumer: {actual_consumer_name}",
            extra={
                "consumer_name": actual_consumer_name,
                "stream": self._config.stream_name,
                "consumer_group": self._config.consumer_group,
            },
        )

        while self._consuming:
            try:
                # Read from stream as part of consumer group
                # '>' means read only new messages not delivered to other consumers
                messages = await self._redis.xreadgroup(
                    groupname=self._config.consumer_group,
                    consumername=actual_consumer_name,
                    streams={self._config.stream_name: ">"},
                    count=self._config.batch_size,
                    block=self._config.block_ms,
                )

                if not messages:
                    # No new messages, continue loop
                    continue

                # Process messages
                for _stream_name, stream_messages in messages:
                    for message_id, message_data in stream_messages:
                        await self._process_message(message_id, message_data, actual_consumer_name)

            except asyncio.CancelledError:
                logger.info("Consumer loop cancelled")
                break
            except RedisConnectionError as e:
                self._stats.reconnections += 1
                logger.error(
                    f"Redis connection error in consumer loop: {e}",
                    exc_info=True,
                )
                # Try to reconnect
                await asyncio.sleep(1)
                try:
                    await self.connect()
                except Exception:
                    logger.error("Failed to reconnect, will retry...")
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}", exc_info=True)
                await asyncio.sleep(1)

        self._consuming = False
        logger.info("Consumer loop stopped")

    async def stop_consuming(self) -> None:
        """Stop the consumer loop gracefully."""
        self._consuming = False
        logger.info("Stop consuming requested")

    async def _process_message(
        self,
        message_id: str,
        message_data: dict[str, str],
        consumer_name: str,
    ) -> None:
        """
        Process a single message from the stream.

        Deserializes the event and dispatches to registered handlers.

        Args:
            message_id: Redis stream message ID
            message_data: Event data from stream
            consumer_name: Name of this consumer
        """
        if not self._redis:
            return

        event_type_name = message_data.get("event_type", "unknown")
        event_id = message_data.get("event_id", "unknown")

        processing_start = datetime.now(UTC)

        with self._tracer.span(
            "eventsource.event_bus.process",
            {
                ATTR_EVENT_TYPE: event_type_name,
                ATTR_EVENT_ID: event_id,
                ATTR_MESSAGING_SYSTEM: "redis",
                "message.id": message_id,
                "consumer.name": consumer_name,
            },
        ) as span:
            try:
                logger.debug(
                    f"Processing message {message_id}: {event_type_name}",
                    extra={
                        "message_id": message_id,
                        "event_type": event_type_name,
                        "event_id": event_id,
                    },
                )

                # Calculate processing lag
                occurred_at_str = message_data.get("occurred_at")
                if occurred_at_str:
                    try:
                        occurred_at = datetime.fromisoformat(occurred_at_str)
                        now = datetime.now(UTC)
                        if occurred_at.tzinfo is None:
                            occurred_at = occurred_at.replace(tzinfo=UTC)
                        lag_seconds = (now - occurred_at).total_seconds()
                        logger.debug(
                            f"Processing lag: {lag_seconds:.3f}s",
                            extra={"lag_seconds": lag_seconds},
                        )
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to calculate processing lag: {e}")

                # Deserialize event
                event = self._deserialize_event(event_type_name, message_data)
                if event is None:
                    logger.warning(
                        f"Unknown event type: {event_type_name}, skipping",
                        extra={"event_type": event_type_name, "message_id": message_id},
                    )
                    # Acknowledge to prevent blocking
                    await self._redis.xack(
                        self._config.stream_name,
                        self._config.consumer_group,
                        message_id,
                    )
                    return

                # Dispatch to registered handlers
                await self._dispatch_event(event, message_id)

                # Acknowledge message after successful processing
                await self._redis.xack(
                    self._config.stream_name,
                    self._config.consumer_group,
                    message_id,
                )

                self._stats.events_consumed += 1
                self._stats.events_processed_success += 1

                processing_duration = (datetime.now(UTC) - processing_start).total_seconds()

                if span:
                    span.set_attribute("processing.success", True)
                    span.set_attribute("processing.duration_ms", processing_duration * 1000)

                logger.debug(
                    f"Successfully processed {event_type_name}",
                    extra={
                        "message_id": message_id,
                        "event_type": event_type_name,
                        "event_id": event_id,
                        "duration_ms": processing_duration * 1000,
                    },
                )

            except Exception as e:
                self._stats.events_processed_failed += 1

                processing_duration = (datetime.now(UTC) - processing_start).total_seconds()

                if span:
                    span.set_attribute("processing.success", False)
                    span.record_exception(e)

                logger.error(
                    f"Failed to process message {message_id}: {e}",
                    exc_info=True,
                    extra={
                        "message_id": message_id,
                        "event_type": event_type_name,
                        "event_id": event_id,
                        "error": str(e),
                        "duration_ms": processing_duration * 1000,
                    },
                )
                # Don't ack - message will be redelivered or recovered later

    def _deserialize_event(
        self,
        event_type_name: str,
        message_data: dict[str, str],
    ) -> DomainEvent | None:
        """
        Deserialize an event from Redis message data.

        Args:
            event_type_name: Name of the event type
            message_data: Raw message data from Redis

        Returns:
            Deserialized event, or None if event type is unknown
        """
        # Get event class from registry
        event_class = self._get_event_class(event_type_name)
        if event_class is None:
            return None

        # Deserialize from payload
        payload = message_data.get("payload", "{}")
        return event_class.model_validate_json(payload)

    def _get_event_class(self, event_type_name: str) -> type[DomainEvent] | None:
        """
        Get event class by name from registry.

        Args:
            event_type_name: Name of the event class

        Returns:
            Event class, or None if not found
        """
        if self._event_registry:
            return self._event_registry.get_or_none(event_type_name)

        # Fallback to default registry
        from eventsource.events.registry import default_registry

        return default_registry.get_or_none(event_type_name)

    async def _dispatch_event(self, event: DomainEvent, message_id: str) -> None:
        """
        Dispatch an event to all matching handlers.

        Args:
            event: The event to dispatch
            message_id: Redis message ID for logging
        """
        event_type = type(event)

        # Get handlers
        specific_handlers = list(self._subscribers.get(event_type, []))
        wildcard_handlers = list(self._all_event_handlers)
        handlers = specific_handlers + wildcard_handlers

        if not handlers:
            logger.warning(
                f"No handlers registered for {event.event_type}",
                extra={"event_type": event.event_type},
            )
            return

        logger.debug(
            f"Dispatching {event.event_type} to {len(handlers)} handler(s)",
            extra={
                "event_type": event.event_type,
                "event_id": str(event.event_id),
                "handler_count": len(handlers),
            },
        )

        # Trace event dispatch with dynamic attributes
        with self._tracer.span(
            "eventsource.event_bus.dispatch",
            {
                ATTR_EVENT_TYPE: event_type.__name__,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_HANDLER_COUNT: len(handlers),
                ATTR_MESSAGING_SYSTEM: "redis",
            },
        ):
            # Process handlers sequentially for ordering guarantees
            for adapter in handlers:
                await self._invoke_handler(adapter, event, message_id)

    async def _invoke_handler(
        self,
        adapter: HandlerAdapter,
        event: DomainEvent,
        message_id: str,
    ) -> None:
        """
        Invoke a single handler with tracing support.

        Args:
            adapter: The HandlerAdapter wrapping the handler
            event: The event to handle
            message_id: Redis message ID for logging
        """
        # Trace handler execution with dynamic attributes and error recording
        with self._tracer.span(
            "eventsource.event_bus.handle",
            {
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_HANDLER_NAME: adapter.name,
                ATTR_MESSAGING_SYSTEM: "redis",
            },
        ) as span:
            try:
                await adapter.handle(event)
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, True)
                logger.debug(
                    f"Handler {adapter.name} processed {event.event_type}",
                    extra={
                        "handler": adapter.name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                    },
                )
            except Exception as e:
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, False)
                    span.record_exception(e)
                self._stats.handler_errors += 1
                logger.error(
                    f"Handler {adapter.name} failed: {e}",
                    exc_info=True,
                    extra={
                        "handler": adapter.name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "message_id": message_id,
                    },
                )
                # Re-raise to prevent acking the message
                raise

    async def recover_pending_messages(
        self,
        min_idle_time_ms: int | None = None,
        max_retries: int | None = None,
        batch_size: int | None = None,
    ) -> dict[str, int]:
        """
        Recover pending messages that have been idle too long.

        Uses XPENDING to find messages that have been read but not acknowledged,
        and XCLAIM to reclaim them for reprocessing. Messages that have been
        retried too many times are sent to a dead letter queue.

        Args:
            min_idle_time_ms: Minimum idle time in ms before claiming (default: from config)
            max_retries: Maximum retries before sending to DLQ (default: from config)
            batch_size: Maximum messages to recover in one batch (default: from config)

        Returns:
            Dictionary with recovery statistics:
            - checked: Number of pending messages checked
            - claimed: Number of messages claimed
            - reprocessed: Number of messages reprocessed successfully
            - dlq: Number of messages sent to DLQ
            - failed: Number of recovery failures
        """
        if not self._connected:
            await self.connect()

        if not self._redis:
            raise RuntimeError("Redis client not initialized")

        # Use config defaults if not specified
        min_idle_time_ms = min_idle_time_ms or self._config.pending_idle_ms
        max_retries = max_retries or self._config.max_retries
        batch_size = batch_size or self._config.batch_size

        stats = {
            "checked": 0,
            "claimed": 0,
            "reprocessed": 0,
            "dlq": 0,
            "failed": 0,
        }

        try:
            # Get pending messages info
            pending_info = await self._redis.xpending(
                self._config.stream_name,
                self._config.consumer_group,
            )

            if not pending_info or not isinstance(pending_info, dict):
                logger.debug("No pending messages found")
                return stats

            pending_count = pending_info.get("pending", 0)
            if pending_count == 0:
                logger.debug("No pending messages to recover")
                return stats

            stats["checked"] = pending_count
            logger.info(
                f"Found {pending_count} pending messages",
                extra={"pending_count": pending_count},
            )

            # Get detailed pending messages
            pending_messages = await self._redis.xpending_range(
                name=self._config.stream_name,
                groupname=self._config.consumer_group,
                min="-",
                max="+",
                count=batch_size,
            )

            for pending_msg in pending_messages:
                message_id = pending_msg["message_id"]
                idle_time_ms = pending_msg["time_since_delivered"]
                times_delivered = pending_msg["times_delivered"]

                # Skip if not idle long enough
                if idle_time_ms < min_idle_time_ms:
                    continue

                logger.info(
                    f"Processing pending message {message_id}",
                    extra={
                        "message_id": message_id,
                        "idle_time_ms": idle_time_ms,
                        "times_delivered": times_delivered,
                    },
                )

                # Get retry count from Redis
                retry_key = self._config.get_retry_key(message_id)
                retry_count_str = await self._redis.get(retry_key)
                retry_count = int(retry_count_str) if retry_count_str else times_delivered - 1

                try:
                    # Claim the message
                    claimed = await self._redis.xclaim(
                        name=self._config.stream_name,
                        groupname=self._config.consumer_group,
                        consumername="recovery-worker",
                        min_idle_time=min_idle_time_ms,
                        message_ids=[message_id],
                    )

                    if not claimed:
                        logger.warning(f"Failed to claim message {message_id}")
                        continue

                    stats["claimed"] += 1

                    # Get the message data
                    message_data = claimed[0][1]
                    event_type_name = message_data.get("event_type", "unknown")

                    # Check if max retries exceeded
                    if retry_count >= max_retries:
                        # Send to DLQ
                        if self._config.enable_dlq:
                            await self._send_to_dlq(message_id, message_data, retry_count)
                            stats["dlq"] += 1
                            self._stats.messages_sent_to_dlq += 1

                        # Acknowledge to remove from pending
                        await self._redis.xack(
                            self._config.stream_name,
                            self._config.consumer_group,
                            message_id,
                        )

                        # Clean up retry counter
                        await self._redis.delete(retry_key)

                        logger.warning(
                            f"Message {message_id} sent to DLQ after {retry_count} retries",
                            extra={
                                "message_id": message_id,
                                "event_type": event_type_name,
                                "retry_count": retry_count,
                            },
                        )
                    else:
                        # Reprocess the message
                        try:
                            await self._process_message(message_id, message_data, "recovery-worker")
                            stats["reprocessed"] += 1
                            self._stats.messages_recovered += 1

                            # Clean up retry counter on success
                            await self._redis.delete(retry_key)

                            logger.info(
                                f"Successfully reprocessed message {message_id}",
                                extra={
                                    "message_id": message_id,
                                    "event_type": event_type_name,
                                    "retry_count": retry_count,
                                },
                            )
                        except Exception as e:
                            # Increment retry count
                            new_retry_count = retry_count + 1
                            await self._redis.setex(
                                retry_key,
                                self._config.retry_key_expiry_seconds,
                                new_retry_count,
                            )

                            stats["failed"] += 1
                            logger.error(
                                f"Failed to reprocess message {message_id}: {e}",
                                exc_info=True,
                                extra={
                                    "message_id": message_id,
                                    "event_type": event_type_name,
                                    "retry_count": new_retry_count,
                                },
                            )

                except Exception as e:
                    stats["failed"] += 1
                    logger.error(
                        f"Error recovering message {message_id}: {e}",
                        exc_info=True,
                        extra={"message_id": message_id},
                    )

            logger.info(
                f"Pending message recovery completed: {stats['reprocessed']} reprocessed, "
                f"{stats['dlq']} sent to DLQ, {stats['failed']} failed",
                extra=stats,
            )

        except Exception as e:
            logger.error(f"Pending message recovery failed: {e}", exc_info=True)
            stats["failed"] += 1

        return stats

    async def _send_to_dlq(
        self,
        message_id: str,
        message_data: dict[str, str],
        retry_count: int,
    ) -> None:
        """
        Send a message to the dead letter queue.

        Args:
            message_id: Original message ID
            message_data: Message data
            retry_count: Number of times the message was retried
        """
        if not self._redis:
            return

        dlq_data = {
            **message_data,
            "original_message_id": message_id,
            "retry_count": str(retry_count),
            "dlq_timestamp": datetime.now(UTC).isoformat(),
        }

        await self._redis.xadd(
            name=self._config.dlq_stream_name,
            fields=dlq_data,  # type: ignore[arg-type]
        )

        logger.info(
            f"Sent message {message_id} to DLQ",
            extra={
                "message_id": message_id,
                "dlq_stream": self._config.dlq_stream_name,
                "retry_count": retry_count,
            },
        )

    async def get_stream_info(self) -> dict[str, Any]:
        """
        Get information about the Redis stream.

        Returns:
            Stream statistics and health info including:
            - connected: Whether connected to Redis
            - stream: Stream length and entry info
            - consumer_groups: List of consumer groups with stats
            - pending_messages: Count of pending messages
            - dlq_messages: Count of DLQ messages
        """
        if not self._connected or not self._redis:
            return {"connected": False}

        try:
            # Stream info
            stream_info = await self._redis.xinfo_stream(self._config.stream_name)

            # Consumer group info
            groups = await self._redis.xinfo_groups(self._config.stream_name)

            # Pending messages
            pending = await self._redis.xpending(
                self._config.stream_name,
                self._config.consumer_group,
            )

            # Get DLQ length
            try:
                dlq_info = await self._redis.xinfo_stream(self._config.dlq_stream_name)
                dlq_length = dlq_info.get("length", 0)
            except Exception:
                dlq_length = 0

            pending_count = pending.get("pending", 0) if isinstance(pending, dict) else 0

            # Count active consumers
            total_consumers = sum(group["consumers"] for group in groups)

            return {
                "connected": True,
                "stream": {
                    "name": self._config.stream_name,
                    "length": stream_info.get("length", 0),
                    "first_entry_id": stream_info.get("first-entry", [None])[0],
                    "last_entry_id": stream_info.get("last-entry", [None])[0],
                },
                "consumer_groups": [
                    {
                        "name": group["name"],
                        "consumers": group["consumers"],
                        "pending": group["pending"],
                    }
                    for group in groups
                ],
                "pending_messages": pending_count,
                "dlq_messages": dlq_length,
                "active_consumers": total_consumers,
                "stats": {
                    "events_published": self._stats.events_published,
                    "events_consumed": self._stats.events_consumed,
                    "events_processed_success": self._stats.events_processed_success,
                    "events_processed_failed": self._stats.events_processed_failed,
                    "messages_recovered": self._stats.messages_recovered,
                    "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
                    "handler_errors": self._stats.handler_errors,
                    "reconnections": self._stats.reconnections,
                },
            }

        except Exception as e:
            logger.error(f"Failed to get stream info: {e}", exc_info=True)
            return {"connected": True, "error": str(e)}

    async def get_dlq_messages(
        self,
        count: int = 100,
        start: str = "-",
        end: str = "+",
    ) -> list[dict[str, Any]]:
        """
        Get messages from the dead letter queue.

        Args:
            count: Maximum number of messages to retrieve
            start: Start ID for range query (default: oldest)
            end: End ID for range query (default: newest)

        Returns:
            List of DLQ messages with their data
        """
        if not self._connected or not self._redis:
            return []

        try:
            messages = await self._redis.xrange(
                name=self._config.dlq_stream_name,
                min=start,
                max=end,
                count=count,
            )

            return [
                {
                    "message_id": msg_id,
                    "data": data,
                }
                for msg_id, data in messages
            ]

        except Exception as e:
            logger.error(f"Failed to get DLQ messages: {e}", exc_info=True)
            return []

    async def replay_dlq_message(self, message_id: str) -> bool:
        """
        Replay a message from the DLQ back to the main stream.

        Args:
            message_id: The DLQ message ID to replay

        Returns:
            True if message was replayed successfully
        """
        if not self._connected or not self._redis:
            return False

        try:
            # Get the message from DLQ
            messages = await self._redis.xrange(
                name=self._config.dlq_stream_name,
                min=message_id,
                max=message_id,
                count=1,
            )

            if not messages:
                logger.warning(f"DLQ message {message_id} not found")
                return False

            _, data = messages[0]

            # Remove DLQ-specific fields
            replay_data = {
                k: v
                for k, v in data.items()
                if k not in ("original_message_id", "retry_count", "dlq_timestamp")
            }

            # Add back to main stream
            new_message_id = await self._redis.xadd(
                name=self._config.stream_name,
                fields=replay_data,
            )

            # Delete from DLQ
            await self._redis.xdel(self._config.dlq_stream_name, message_id)

            logger.info(
                f"Replayed DLQ message {message_id} as {new_message_id}",
                extra={
                    "original_message_id": message_id,
                    "new_message_id": new_message_id,
                },
            )

            return True

        except Exception as e:
            logger.error(f"Failed to replay DLQ message {message_id}: {e}", exc_info=True)
            return False

    def clear_subscribers(self) -> None:
        """
        Clear all subscribers.

        Useful for testing or reinitializing the bus.
        """
        self._subscribers.clear()
        self._all_event_handlers.clear()
        logger.info("All event subscribers cleared")

    def get_subscriber_count(self, event_type: type[DomainEvent] | None = None) -> int:
        """
        Get the number of registered subscribers.

        Args:
            event_type: If provided, count subscribers for this event type only.

        Returns:
            Number of registered subscribers
        """
        if event_type is None:
            return sum(len(handlers) for handlers in self._subscribers.values())
        return len(self._subscribers.get(event_type, []))

    def get_wildcard_subscriber_count(self) -> int:
        """
        Get the number of wildcard subscribers.

        Returns:
            Number of wildcard subscribers
        """
        return len(self._all_event_handlers)

    def get_stats_dict(self) -> dict[str, int]:
        """
        Get statistics as a dictionary.

        Returns:
            Dictionary with all statistics
        """
        return {
            "events_published": self._stats.events_published,
            "events_consumed": self._stats.events_consumed,
            "events_processed_success": self._stats.events_processed_success,
            "events_processed_failed": self._stats.events_processed_failed,
            "messages_recovered": self._stats.messages_recovered,
            "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
            "handler_errors": self._stats.handler_errors,
            "reconnections": self._stats.reconnections,
        }

    async def shutdown(self, timeout: float = 30.0) -> None:
        """
        Shutdown the event bus gracefully.

        Stops consuming and disconnects from Redis.

        Args:
            timeout: Maximum time to wait in seconds
        """
        logger.info("Shutting down RedisEventBus")

        # Stop consuming
        await self.stop_consuming()

        # Wait a bit for current processing to complete
        if self._consuming:
            await asyncio.sleep(min(timeout, 5.0))

        # Disconnect
        await self.disconnect()

        logger.info("RedisEventBus shutdown complete")


__all__ = [
    "RedisEventBus",
    "RedisEventBusConfig",
    "RedisEventBusStats",
    "RedisNotAvailableError",
    "REDIS_AVAILABLE",
]
