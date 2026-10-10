"""Consumption and handler dispatch mixin for Redis event bus.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from eventsource.adapters.redis.config import (
    DecodedStreams,
    RedisConnectionError,
)
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eventsource.adapters.redis.config import RedisEventBusConfig
    from eventsource.adapters.redis.models import RedisEventBusStats
    from eventsource.domain.event import DomainEvent
    from eventsource.observability import Tracer

logger = logging.getLogger("eventsource.adapters.redis")


class RedisBusConsumeMixin:
    """Consume loop and message processing for RedisEventBus."""

    _connected: bool
    _redis: Redis | None
    _consuming: bool
    _consumer_task: asyncio.Task[None] | None
    _config: RedisEventBusConfig
    _stats: RedisEventBusStats
    _tracer: Tracer
    if TYPE_CHECKING:

        async def connect(self) -> None: ...
        async def _ensure_consumer_group_exists(self) -> None: ...
        def _deserialize_event(
            self, event_type_name: str, message_data: dict[str, str]
        ) -> DomainEvent | None: ...
        async def _dispatch_event(self, event: DomainEvent, message_id: str) -> None: ...

    async def start_consuming(
        self,
        consumer_name: str | None = None,
    ) -> None:
        """Start consuming events from Redis stream.

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
                messages = cast(
                    DecodedStreams,
                    await self._redis.xreadgroup(
                        groupname=self._config.consumer_group,
                        consumername=actual_consumer_name,
                        streams={self._config.stream_name: ">"},
                        count=self._config.stream_read_count,
                        block=self._config.block_ms,
                    ),
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

    def start_consuming_in_background(
        self,
        consumer_name: str | None = None,
    ) -> asyncio.Task[None]:
        """Start consuming in a background task.

        `start_consuming()` blocks for the lifetime of the consumer, so it
        cannot be called from a coroutine that also needs to do anything else.
        This schedules it as a task instead, matching the Kafka and RabbitMQ
        buses. The task is retained on the bus, so `disconnect()` cancels it.

        Args:
            consumer_name: Optional override for consumer name from config

        Returns:
            The background task running the consumer.

        Raises:
            RuntimeError: If a background consumer is already running.
        """
        if self._consumer_task is not None and not self._consumer_task.done():
            raise RuntimeError("Consumer is already running in the background")

        self._consumer_task = asyncio.create_task(self.start_consuming(consumer_name))
        return self._consumer_task

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
        """Process a single message from the stream.

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


__all__ = ["RedisBusConsumeMixin"]
