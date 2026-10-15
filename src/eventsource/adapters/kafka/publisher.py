"""Split-phase Kafka publish (send handoff, then batched ack-await)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.kafka.serialization import EventSerializer
from eventsource.domain.event import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from opentelemetry.propagate import inject
    from opentelemetry.trace import Status, StatusCode

    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
else:
    try:
        from opentelemetry.propagate import inject
        from opentelemetry.trace import Status, StatusCode

        PROPAGATION_AVAILABLE = OTEL_AVAILABLE
    except ImportError:
        inject = Status = StatusCode = None
        PROPAGATION_AVAILABLE = False

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaPublisher:
    """Owns the send/ack mechanics for publishing events to Kafka."""

    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics | None,
        tracer: Tracer,
        enable_tracing: bool,
    ) -> None:
        self._config, self._connection, self._serializer = config, connection, serializer
        self._stats, self._metrics = stats, metrics
        self._tracer, self._enable_tracing = tracer, enable_tracing

    async def publish_all(self, events: Sequence[DomainEvent], background: bool) -> None:
        """Hand every event to the producer sequentially, then resolve delivery."""
        pending = [await self._begin_publish_single_event(event, background) for event in events]
        if background:
            for future, published_event, span in pending:
                self._track_background_publish(future, published_event, span)
                if span:
                    span.end()
        else:
            await asyncio.gather(*(self._await_publish_ack(handle) for handle in pending))

    def _metric_attrs(self, event_type: str, **extra: str) -> dict[str, str]:
        base = {"messaging.system": "kafka", "messaging.destination": self._config.topic_name}
        return {**base, "event.type": event_type, **extra}

    def _record_publish_error(self, event: DomainEvent, span: Any, error: Exception) -> None:
        if span:
            span.set_status(Status(StatusCode.ERROR, str(error)))
            span.record_exception(error)
        if self._metrics:
            attrs = self._metric_attrs(event.event_type, **{"error.type": type(error).__name__})
            self._metrics.publish_errors.add(1, attributes=attrs)
        logger.error("Failed to publish event", extra={"error": str(error)}, exc_info=True)
        self._stats.last_error_at = datetime.now(UTC)

    async def _begin_publish_single_event(
        self, event: DomainEvent, background: bool
    ) -> tuple[Any, DomainEvent, Any]:
        producer = self._connection.require_producer()
        key = self._get_partition_key(event)
        value = self._serialize_event(event)
        headers = self._create_headers(event)
        span: Any = None
        if self._enable_tracing:
            span = self._tracer.start_span(
                name=f"eventsource.event_bus.publish {event.event_type}",
                kind=SpanKindEnum.PRODUCER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "kafka",
                    ATTR_MESSAGING_DESTINATION: self._config.topic_name,
                    "messaging.destination_kind": "topic",
                    ATTR_MESSAGING_OPERATION: "publish",
                    ATTR_EVENT_TYPE: event.event_type,
                    ATTR_EVENT_ID: str(event.event_id),
                    ATTR_AGGREGATE_ID: str(event.aggregate_id),
                    ATTR_AGGREGATE_TYPE: event.aggregate_type,
                },
            )
            if PROPAGATION_AVAILABLE and inject is not None:
                carrier: dict[str, str] = {}
                inject(carrier)
                headers.extend((k, v.encode("utf-8")) for k, v in carrier.items())
        try:
            future = await producer.send(
                topic=self._config.topic_name, key=key, value=value, headers=headers
            )
        except Exception as e:
            self._record_publish_error(event, span, e)
            if span:
                span.end()
            raise
        return future, event, span

    async def _await_publish_ack(self, handle: tuple[Any, DomainEvent, Any]) -> None:
        future, event, span = handle
        try:
            meta = await future
            if span:
                if meta is not None:
                    span.set_attribute("messaging.kafka.partition", meta.partition)
                    span.set_attribute("messaging.kafka.offset", meta.offset)
                span.set_status(Status(StatusCode.OK))
            if meta is not None:
                extra = {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}
                logger.debug("Event published", extra=extra)
            if self._metrics:
                self._metrics.messages_published.add(
                    1, attributes=self._metric_attrs(event.event_type)
                )
        except Exception as e:
            self._record_publish_error(event, span, e)
            raise
        finally:
            if span:
                span.end()

    def _track_background_publish(self, future: Any, event: DomainEvent, span: Any) -> None:
        event_type, event_id = event.event_type, str(event.event_id)
        base_attrs = self._metric_attrs(event_type)

        def on_send_success(meta: Any) -> None:
            extra = {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}
            logger.debug(
                "Background event published successfully",
                extra={**extra, "event_type": event_type, "event_id": event_id},
            )
            if self._metrics:
                self._metrics.messages_published.add(1, attributes=dict(base_attrs))

        def on_send_error(exc: Exception) -> None:
            logger.error(
                "Background event publish failed",
                extra={"error": str(exc), "event_type": event_type, "event_id": event_id},
                exc_info=False,
            )
            self._stats.last_error_at = datetime.now(UTC)
            if self._metrics:
                self._metrics.publish_errors.add(
                    1, attributes={**base_attrs, "error.type": type(exc).__name__}
                )
            if span:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)

        def on_done(fut: asyncio.Future[Any]) -> None:
            if not fut.cancelled():
                exc = fut.exception()
                on_send_error(exc) if isinstance(exc, Exception) else on_send_success(fut.result())

        future.add_done_callback(on_done)

    def _get_partition_key(self, event: DomainEvent) -> bytes:
        return str(event.aggregate_id).encode("utf-8")

    def _serialize_event(self, event: DomainEvent) -> bytes:
        return self._serializer.serialize(event)

    def _create_headers(self, event: DomainEvent) -> list[tuple[str, bytes]]:
        headers: list[tuple[str, bytes]] = [
            ("event_id", str(event.event_id).encode("utf-8")),
            ("event_type", event.event_type.encode("utf-8")),
            ("aggregate_id", str(event.aggregate_id).encode("utf-8")),
            ("aggregate_type", event.aggregate_type.encode("utf-8")),
            ("aggregate_version", str(event.aggregate_version).encode("utf-8")),
            ("occurred_at", event.occurred_at.isoformat().encode("utf-8")),
            ("correlation_id", str(event.correlation_id).encode("utf-8")),
        ]
        for attr in ("tenant_id", "causation_id", "actor_id"):
            if val := getattr(event, attr):
                headers.append((attr, str(val).encode("utf-8")))
        return headers
