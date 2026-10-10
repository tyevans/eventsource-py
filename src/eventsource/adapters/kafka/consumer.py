"""Kafka consume loop: poll, dispatch, commit, retry, and DLQ routing.

Extracted from ``KafkaEventBus`` so the consume-side mechanics live in one
focused collaborator.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.retry_scheduler import RetryScheduler
from eventsource.adapters.kafka.consumer_dispatch import KafkaConsumerDispatchMixin
from eventsource.adapters.kafka.consumer_helpers import get_retry_delay_remaining
from eventsource.adapters.kafka.consumer_message import KafkaConsumerMessageMixin
from eventsource.adapters.kafka.consumer_retry import KafkaConsumerRetryMixin
from eventsource.ports.exceptions import EventBusConnectionError

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters._bus.retry import RetryPolicy
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats
    from eventsource.adapters.kafka.serialization import EventSerializer
    from eventsource.domain.event import DomainEvent
    from eventsource.observability import Tracer

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaConsumerLoop(
    KafkaConsumerMessageMixin,
    KafkaConsumerDispatchMixin,
    KafkaConsumerRetryMixin,
):
    """Owns the Kafka consume loop and delegates message dispatch and retry handling."""

    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics | None,
        retry_policy: RetryPolicy,
        handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]],
        resolve_event_class: Callable[[str], type[DomainEvent] | None],
        tracer: Tracer,
        enable_tracing: bool,
        shutdown_event: asyncio.Event,
        on_start: Callable[[], None] | None = None,
    ) -> None:
        """Initialize the consume loop."""
        self._config = config
        self._connection = connection
        self._serializer = serializer
        self._stats = stats
        self._metrics = metrics
        self._retry_policy = retry_policy
        self._handlers_for = handlers_for
        self._resolve_event_class = resolve_event_class
        self._tracer = tracer
        self._enable_tracing = enable_tracing
        self._shutdown_event = shutdown_event
        self._on_start = on_start

        self._consuming = False
        self._consume_task: asyncio.Task[None] | None = None
        self._retry_scheduler = RetryScheduler()

    @property
    def is_consuming(self) -> bool:
        """Check if actively consuming messages."""
        return self._consuming

    @property
    def _consumer(self) -> Any:
        """The active aiokafka consumer, or None."""
        return self._connection.consumer

    @property
    def _producer(self) -> Any:
        """The active aiokafka producer, or None."""
        return self._connection.producer

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self, auto_reconnect: bool = True) -> None:
        """Start consuming events from Kafka.

        Blocks and continuously polls for messages, dispatching them to
        registered handlers. Use stop() from another coroutine to stop.
        """
        if not self._connection.is_connected or not self._consumer:
            raise EventBusConnectionError("Not connected to Kafka. Call connect() first.")

        if self._consuming:
            logger.warning("Already consuming events")
            return

        self._consuming = True
        self._shutdown_event.clear()

        if self._on_start is not None:
            self._on_start()

        logger.info(
            "Starting Kafka consumer",
            extra={
                "topic": self._config.topic_name,
                "consumer_group": self._config.consumer_group,
                "auto_reconnect": auto_reconnect,
            },
        )

        reconnect_delay = 1.0
        max_reconnect_delay = 60.0

        while self._consuming and not self._shutdown_event.is_set():
            try:
                async for message in self._consumer:
                    if self._shutdown_event.is_set() or not self._consuming:
                        break

                    remaining = get_retry_delay_remaining(message.headers)
                    if remaining > 0:

                        async def _retry_action(msg: Any = message) -> None:
                            await self._process_message(msg, skip_await_retry=True)

                        self._retry_scheduler.schedule(
                            remaining,
                            _retry_action,
                            name=f"kafka-retry-p{message.partition}-o{message.offset}",
                        )
                    else:
                        await self._process_message(message)

                    reconnect_delay = 1.0

                if not self._consuming or self._shutdown_event.is_set():
                    break

            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
                raise
            except Exception as e:
                if self._metrics:
                    self._metrics.connection_errors.add(
                        1,
                        attributes={
                            "error.type": type(e).__name__,
                        },
                    )

                logger.error(
                    "Consumer error",
                    extra={"error": str(e), "auto_reconnect": auto_reconnect},
                    exc_info=True,
                )

                if not auto_reconnect or not self._consuming:
                    raise

                logger.info(
                    "Attempting to reconnect consumer",
                    extra={"delay_seconds": reconnect_delay},
                )

                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                self._connection.record_reconnection()

                try:
                    await self._connection.reconnect_consumer()
                except Exception as reconnect_error:
                    logger.error(
                        "Failed to reconnect consumer",
                        extra={"error": str(reconnect_error)},
                        exc_info=True,
                    )

        self._consuming = False
        logger.info("Consumer stopped")

    async def drain_retries(self, timeout: float | None = None) -> None:
        """Wait for any scheduled non-blocking retries to complete."""
        await self._retry_scheduler.drain(timeout)

    async def stop(self, timeout: float | None = None) -> None:
        """Stop the consumer loop gracefully."""
        self._consuming = False
        await self._retry_scheduler.drain(timeout)
        logger.info("Stop consuming requested")

    def start_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task."""
        if self._consume_task is not None and not self._consume_task.done():
            raise RuntimeError("Consumer already running in background")

        self._consume_task = asyncio.create_task(
            self.start(),
            name=f"kafka-consumer-{self._config.consumer_name}",
        )
        return self._consume_task


__all__ = ["KafkaConsumerLoop"]
