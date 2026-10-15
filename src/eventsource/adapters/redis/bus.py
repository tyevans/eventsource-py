"""Redis event bus implementation using Redis Streams for durable event distribution."""

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


class RedisEventBus(
    RedisBusPublishMixin,
    RedisBusConsumeMixin,
    RedisBusDispatchMixin,
    RedisBusDLQMixin,
    RedisBusConnectionMixin,
    BaseEventBus,
):
    """Event bus using Redis Streams for durable event distribution across consumer groups."""

    def __init__(
        self,
        config: RedisEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        *,
        tracer: Tracer | None = None,
        max_background_tasks: int | None = DEFAULT_MAX_BACKGROUND_TASKS,
    ) -> None:
        """Initialize the Redis event bus (raises RedisNotAvailableError if redis is missing)."""
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
        self._stats = RedisEventBusStats()
        self._consumer_task: asyncio.Task[None] | None = None
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
