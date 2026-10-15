"""RabbitMQ publish-path implementation."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.adapters.rabbitmq import serialization
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
from eventsource.adapters.rabbitmq.publisher_batch import RabbitMQPublisherBatchMixin
from eventsource.adapters.rabbitmq.publisher_many import RabbitMQPublisherManyMixin
from eventsource.domain.event import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology

try:
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

logger = logging.getLogger("eventsource.adapters.rabbitmq")


class RabbitMQPublisher(RabbitMQPublisherManyMixin, RabbitMQPublisherBatchMixin):
    """Publishes domain events to the RabbitMQ main exchange."""

    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
        tracer: Tracer | None,
        enable_tracing: bool,
    ) -> None:
        self._config = config
        self._connection = connection
        self._topology = topology
        self._stats = stats
        self._tracer = tracer
        self._enable_tracing = enable_tracing
        self._logger = logging.getLogger("eventsource.adapters.rabbitmq")
        self._publish_semaphore = asyncio.Semaphore(config.max_concurrent_publishes)

    async def publish_one(
        self,
        event: DomainEvent,
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish a single event to the exchange with optional tracing."""
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        routing_key = serialization.get_routing_key(event)
        span = None

        if self._enable_tracing and PROPAGATION_AVAILABLE and self._tracer is not None:
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
            message = serialization.create_message_with_tracing(event, span)
            await exchange.publish(message, routing_key=routing_key)

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

    async def _publish_single_no_stats(self, event: DomainEvent) -> None:
        """Publish a single event without updating statistics."""
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        routing_key = serialization.get_routing_key(event)
        message = serialization.create_message(event)
        await exchange.publish(message, routing_key=routing_key)


__all__ = [
    "PROPAGATION_AVAILABLE",
    "RabbitMQPublisher",
    "RabbitMQPublisherBatchMixin",
    "RabbitMQPublisherManyMixin",
]
