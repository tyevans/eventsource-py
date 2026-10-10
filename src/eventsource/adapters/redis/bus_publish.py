"""Publishing mixin for Redis event bus.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from eventsource.domain.event import DomainEvent
from eventsource.observability.attributes import (
    ATTR_EVENT_COUNT,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eventsource.adapters.redis.config import RedisEventBusConfig
    from eventsource.adapters.redis.models import RedisEventBusStats
    from eventsource.observability import Tracer

logger = logging.getLogger("eventsource.adapters.redis")


class RedisBusPublishMixin:
    """Publishing path (single/batch/background) for RedisEventBus."""

    _connected: bool
    _redis: Redis | None
    _config: RedisEventBusConfig
    _stats: RedisEventBusStats
    _tracer: Tracer
    if TYPE_CHECKING:

        async def connect(self) -> None: ...
        async def _track_background(self, coro: Coroutine[Any, Any, Any]) -> None: ...

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to Redis stream.

        Events are serialized to JSON and added to the stream.
        Consumer workers will pick them up for processing.

        Uses pipeline for batch operations (10-100x faster for multiple events).

        Args:
            events: Events to publish
            background: If True, return without waiting for the Redis
                       round-trip to complete. The write is tracked and
                       drained by shutdown().

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
                if background:
                    await self._track_background(self._publish_all(events))
                elif len(events) > 1:
                    await self._publish_batch(events)
                else:
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

    async def _publish_all(self, events: list[DomainEvent]) -> None:
        """Publish events, choosing single-write or pipeline by count."""
        if len(events) > 1:
            await self._publish_batch(events)
        else:
            await self._publish_single(events[0])

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


__all__ = ["RedisBusPublishMixin"]
