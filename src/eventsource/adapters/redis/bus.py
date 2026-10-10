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
    >>> from eventsource.adapters.redis import RedisEventBus, RedisEventBusConfig
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
import logging
from typing import TYPE_CHECKING

from eventsource.adapters._bus.base import DEFAULT_MAX_BACKGROUND_TASKS, BaseEventBus
from eventsource.adapters.redis.bus_connection import RedisBusConnectionMixin
from eventsource.adapters.redis.bus_consume import RedisBusConsumeMixin
from eventsource.adapters.redis.bus_dispatch import RedisBusDispatchMixin
from eventsource.adapters.redis.bus_dlq import RedisBusDLQMixin
from eventsource.adapters.redis.bus_publish import RedisBusPublishMixin
from eventsource.adapters.redis.config import (
    REDIS_AVAILABLE,
    DecodedEntry,
    DecodedFields,
    DecodedPending,
    DecodedStreams,
    RedisConnectionError,
    RedisEventBusConfig,
    RedisNotAvailableError,
    ResponseError,
    aioredis,
)
from eventsource.adapters.redis.models import RedisEventBusStats
from eventsource.observability import OTEL_AVAILABLE, Tracer, create_tracer
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
    from redis.asyncio import Redis

    from eventsource.domain.event_registry import EventRegistry

logger = logging.getLogger(__name__)

# Standard span names and attribute conventions implemented across Redis event bus modules:
# - self._tracer.span("eventsource.event_bus.publish", ...)
# - self._tracer.span("eventsource.event_bus.process", ...)
# - self._tracer.span("eventsource.event_bus.dispatch", ...)
# - self._tracer.span("eventsource.event_bus.handle", ...)
# Uses ATTR_MESSAGING_SYSTEM, ATTR_MESSAGING_DESTINATION, ATTR_EVENT_TYPE,
# ATTR_HANDLER_NAME, ATTR_HANDLER_SUCCESS, ATTR_EVENT_ID, ATTR_AGGREGATE_ID,
# ATTR_EVENT_COUNT, ATTR_HANDLER_COUNT.


class RedisEventBus(
    RedisBusPublishMixin,
    RedisBusConsumeMixin,
    RedisBusDispatchMixin,
    RedisBusDLQMixin,
    RedisBusConnectionMixin,
    BaseEventBus,
):
    """Event bus using Redis Streams for durable event distribution.

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
        >>> from eventsource.adapters.redis import RedisEventBus, RedisEventBusConfig
        >>> from eventsource.domain.event_registry import EventRegistry
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
        max_background_tasks: int | None = DEFAULT_MAX_BACKGROUND_TASKS,
    ) -> None:
        """Initialize the Redis event bus.

        Args:
            config: Configuration for the Redis event bus.
                   Defaults to RedisEventBusConfig() with default values.
            event_registry: Event registry for deserializing events.
                          If None, uses the default registry.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on config.enable_tracing setting.
            max_background_tasks: Maximum concurrent background publish tasks.

        Raises:
            RedisNotAvailableError: If redis package is not installed
        """
        if not REDIS_AVAILABLE:
            raise RedisNotAvailableError()

        super().__init__(
            event_registry=event_registry,
            max_background_tasks=max_background_tasks,
        )

        self._config = config or RedisEventBusConfig()
        self._redis: Redis | None = None
        self._connected = False
        self._consuming = False
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


__all__ = [
    "ATTR_AGGREGATE_ID",
    "ATTR_EVENT_COUNT",
    "ATTR_EVENT_ID",
    "ATTR_EVENT_TYPE",
    "ATTR_HANDLER_COUNT",
    "ATTR_HANDLER_NAME",
    "ATTR_HANDLER_SUCCESS",
    "ATTR_MESSAGING_DESTINATION",
    "ATTR_MESSAGING_SYSTEM",
    "DecodedEntry",
    "DecodedFields",
    "DecodedPending",
    "DecodedStreams",
    "OTEL_AVAILABLE",
    "REDIS_AVAILABLE",
    "RedisConnectionError",
    "RedisEventBus",
    "RedisEventBusConfig",
    "RedisEventBusStats",
    "RedisNotAvailableError",
    "ResponseError",
    "aioredis",
]
