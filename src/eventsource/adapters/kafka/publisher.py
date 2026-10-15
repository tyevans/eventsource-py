"""Split-phase Kafka publish (send handoff, then batched ack-await)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.kafka.serialization import EventSerializer
from eventsource.domain.event import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability import attributes as _attr

if TYPE_CHECKING:
    from collections.abc import Sequence

    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats

try:
    from opentelemetry.propagate import inject
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    inject = Status = StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaPublisher:
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
        pending = [await self._begin_publish_single_event(event, background) for event in events]
        if not background:
            await asyncio.gather(*(self._await_publish_ack(handle) for handle in pending))
        for future, published_event, span in pending if background else ():
            self._track_background_publish(future, published_event, span)
            span.end() if span else None

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
        value, headers = self._serialize_event(event), self._create_headers(event)
        span: Any = None
        if self._enable_tracing:
            attrs = {_attr.ATTR_MESSAGING_SYSTEM: "kafka", "messaging.destination_kind": "topic"}
            attrs[_attr.ATTR_MESSAGING_DESTINATION] = self._config.topic_name
            attrs[_attr.ATTR_MESSAGING_OPERATION] = "publish"
            attrs[_attr.ATTR_EVENT_TYPE] = event.event_type
            attrs[_attr.ATTR_EVENT_ID] = str(event.event_id)
            attrs[_attr.ATTR_AGGREGATE_ID] = str(event.aggregate_id)
            attrs[_attr.ATTR_AGGREGATE_TYPE] = event.aggregate_type
            name = f"eventsource.event_bus.publish {event.event_type}"
            span = self._tracer.start_span(name=name, kind=SpanKindEnum.PRODUCER, attributes=attrs)
            if PROPAGATION_AVAILABLE and inject is not None:
                carrier: dict[str, str] = {}
                inject(carrier)
                headers.extend((k, v.encode("utf-8")) for k, v in carrier.items())
        try:
            topic = self._config.topic_name
            future = await producer.send(topic=topic, key=key, value=value, headers=headers)
        except Exception as e:
            self._record_publish_error(event, span, e)
            span.end() if span else None
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
                attrs = self._metric_attrs(event.event_type)
                self._metrics.messages_published.add(1, attributes=attrs)
        except Exception as e:
            self._record_publish_error(event, span, e)
            raise
        finally:
            span.end() if span else None

    def _track_background_publish(self, future: Any, event: DomainEvent, span: Any) -> None:
        event_type, event_id = event.event_type, str(event.event_id)
        base_attrs = self._metric_attrs(event_type)

        def on_done(fut: asyncio.Future[Any]) -> None:
            if fut.cancelled():
                return
            if isinstance(exc := fut.exception(), Exception):
                err_extra = {"error": str(exc), "event_type": event_type, "event_id": event_id}
                logger.error("Background event publish failed", extra=err_extra, exc_info=False)
                self._stats.last_error_at = datetime.now(UTC)
                if self._metrics:
                    err_attrs = {**base_attrs, "error.type": type(exc).__name__}
                    self._metrics.publish_errors.add(1, attributes=err_attrs)
                if span:
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    span.record_exception(exc)
            else:
                meta = fut.result()
                extra = {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}
                extra.update(event_type=event_type, event_id=event_id)
                logger.debug("Background event published successfully", extra=extra)
                if self._metrics:
                    self._metrics.messages_published.add(1, attributes=dict(base_attrs))

        future.add_done_callback(on_done)

    def _get_partition_key(self, event: DomainEvent) -> bytes:
        return str(event.aggregate_id).encode("utf-8")

    def _serialize_event(self, event: DomainEvent) -> bytes:
        return self._serializer.serialize(event)

    def _create_headers(self, event: DomainEvent) -> list[tuple[str, bytes]]:
        fields = ("event_id", "event_type", "aggregate_id", "aggregate_type", "aggregate_version")
        headers = [(f, str(getattr(event, f)).encode("utf-8")) for f in fields]
        headers.append(("occurred_at", event.occurred_at.isoformat().encode("utf-8")))
        headers.append(("correlation_id", str(event.correlation_id).encode("utf-8")))
        headers.extend(
            (a, str(v).encode("utf-8"))
            for a in ("tenant_id", "causation_id", "actor_id")
            if (v := getattr(event, a))
        )
        return headers
