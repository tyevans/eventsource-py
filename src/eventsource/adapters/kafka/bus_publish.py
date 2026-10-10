"""Publishing mixin for KafkaEventBus.

Governed by ADR-0002 (<500 lines per module).
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.domain.event import DomainEvent
from eventsource.ports.exceptions import EventBusConnectionError

if TYPE_CHECKING:
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats
    from eventsource.adapters.kafka.publisher import KafkaPublisher

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaBusPublishMixin:
    """Mixin providing event publishing methods for KafkaEventBus."""

    # Declared for type checking; provided by KafkaEventBus / BaseEventBus
    _config: KafkaEventBusConfig
    _publisher: KafkaPublisher
    _stats: KafkaEventBusStats
    _metrics: KafkaEventBusMetrics | None
    _connected: bool
    _producer: Any
    _track_background: Any

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to Kafka.

        Events are published to the configured topic with the aggregate_id as
        the partition key. This ensures events for the same aggregate are
        processed in order -- **within a single publish() call**.

        Args:
            events: List of domain events to publish.
            background: If True, don't wait for broker acknowledgment.

        Raises:
            EventBusConnectionError: If not connected to Kafka.
            KafkaError: If publishing fails and background=False.
        """
        if not self._connected or not self._producer:
            raise EventBusConnectionError("Not connected to Kafka. Call connect() first.")

        if not events:
            return

        start_time = time.perf_counter()

        logger.debug(
            "Publishing events to Kafka",
            extra={
                "event_count": len(events),
                "topic": self._config.topic_name,
                "background": background,
            },
        )

        if background:
            await self._track_background(self._publish_and_record(events, background, start_time))
        else:
            await self._publish_and_record(events, background, start_time)

    async def _publish_and_record(
        self,
        events: list[DomainEvent],
        background: bool,
        start_time: float,
    ) -> None:
        """Hand events to the publisher, then record stats and metrics."""
        await self._publisher.publish_all(events, background)

        self._stats.events_published += len(events)
        self._stats.last_publish_at = datetime.now(UTC)

        if self._metrics:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self._metrics.publish_duration.record(
                duration_ms,
                attributes={
                    "messaging.destination": self._config.topic_name,
                },
            )
            self._metrics.batch_publish_size.record(len(events))

        logger.debug(
            "Events published successfully",
            extra={"event_count": len(events)},
        )

    def _serialize_event(self, event: DomainEvent) -> bytes:
        """Serialize an event to bytes using the configured serializer."""
        return self._publisher._serialize_event(event)
