"""Message processing and deserialization mixin for KafkaConsumerLoop.

Governed by ADR-0002 (<500 lines per module).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.kafka.consumer_helpers import get_header_value
from eventsource.adapters.kafka.models import DeserializationError
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats
    from eventsource.adapters.kafka.serialization import EventSerializer

try:
    from opentelemetry.propagate import extract
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaConsumerMessageMixin:
    """Mixin providing message intake, span extraction, and deserialization."""

    # Collaborator attributes and methods provided by sibling mixins / loop
    _config: KafkaEventBusConfig
    _stats: KafkaEventBusStats
    _metrics: KafkaEventBusMetrics | None
    _serializer: EventSerializer
    _handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]]
    _resolve_event_class: Callable[[str], type[DomainEvent] | None]
    _tracer: Tracer
    _enable_tracing: bool
    _consumer: Any
    _dispatch_to_handlers: Any
    _handle_processing_error: Any
    _send_to_dlq: Any
    _warn_uncommitted: Any

    async def _process_message(self, message: Any, skip_await_retry: bool = False) -> None:
        """Process a single Kafka message with optional tracing."""
        self._stats.events_consumed += 1
        self._stats.last_consume_at = datetime.now(UTC)

        event_type_name = self._get_header_value(message.headers, "event_type")

        if self._metrics and event_type_name:
            self._metrics.messages_consumed.add(
                1,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": message.topic,
                    "messaging.kafka.partition": message.partition,
                    "event.type": event_type_name,
                },
            )

        if not event_type_name:
            logger.error(
                "Message missing event_type header",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )
            if self._consumer:
                await self._consumer.commit()
            return

        logger.debug(
            "Processing message",
            extra={
                "event_type": event_type_name,
                "partition": message.partition,
                "offset": message.offset,
            },
        )

        if self._enable_tracing and PROPAGATION_AVAILABLE and extract is not None:
            carrier = self._extract_trace_context(message.headers)
            context = extract(carrier)

            with self._tracer.span_with_kind(
                name=f"eventsource.event_bus.consume {event_type_name}",
                kind=SpanKindEnum.CONSUMER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "kafka",
                    "messaging.source": message.topic,
                    "messaging.source_kind": "topic",
                    ATTR_MESSAGING_OPERATION: "receive",
                    "messaging.kafka.partition": message.partition,
                    "messaging.kafka.offset": message.offset,
                    "messaging.kafka.consumer_group": self._config.consumer_group,
                    ATTR_EVENT_TYPE: event_type_name,
                },
                context=context,
            ) as span:
                await self._process_message_with_span(
                    message, event_type_name, span, skip_await_retry=skip_await_retry
                )
        else:
            await self._process_message_with_span(
                message, event_type_name, None, skip_await_retry=skip_await_retry
            )

    def _extract_trace_context(
        self,
        headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None,
    ) -> dict[str, str]:
        """Extract OpenTelemetry trace context from message headers."""
        carrier: dict[str, str] = {}
        if headers:
            for key, value in headers:
                if key in ("traceparent", "tracestate", "baggage"):
                    carrier[key] = value.decode("utf-8")
        return carrier

    async def _process_message_with_span(
        self,
        message: Any,
        event_type_name: str,
        span: Any,
        skip_await_retry: bool = False,
    ) -> None:
        """Process message with optional span updates."""
        start_time = time.perf_counter()
        retry_count = self._get_retry_count(message.headers)

        if not skip_await_retry:
            await self._await_retry_after(message.headers)

        try:
            try:
                event = self._deserialize_message(message)
            except DeserializationError as e:
                logger.error(
                    "Deserialization error, sending directly to DLQ",
                    extra={
                        "event_type": event_type_name,
                        "error": str(e),
                    },
                )
                retained = await self._send_to_dlq(
                    message, e, retry_count, reason="deserialization_error"
                )
                if retained and self._consumer:
                    await self._consumer.commit()
                elif not retained:
                    self._warn_uncommitted(message, "deserialization_error")

                if span is not None and Status is not None and StatusCode is not None:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)

                if self._metrics:
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    self._metrics.consume_duration.record(
                        duration_ms,
                        attributes={"messaging.destination": message.topic},
                    )
                return

            if span is not None:
                span.set_attribute(ATTR_EVENT_ID, str(event.event_id))
                span.set_attribute(ATTR_AGGREGATE_ID, str(event.aggregate_id))
                span.set_attribute(ATTR_AGGREGATE_TYPE, event.aggregate_type)

            handlers = self._handlers_for(type(event))

            if not handlers:
                logger.debug(
                    "No handlers for event type",
                    extra={"event_type": event_type_name},
                )
            else:
                await self._dispatch_to_handlers(event, handlers)

            if self._consumer:
                await self._consumer.commit()
            self._stats.events_processed_success += 1

            if span is not None and Status is not None and StatusCode is not None:
                span.set_status(Status(StatusCode.OK))

            if self._metrics:
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.consume_duration.record(
                    duration_ms,
                    attributes={
                        "messaging.destination": message.topic,
                    },
                )

        except Exception as e:
            if self._metrics:
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.consume_duration.record(
                    duration_ms,
                    attributes={
                        "messaging.destination": message.topic,
                    },
                )

            if span is not None and Status is not None and StatusCode is not None:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            dlq_error: Exception = e
            if isinstance(e, HandlerDispatchError) and len(e.failures) == 1:
                dlq_error = e.failures[0][1]
            await self._handle_processing_error(message, dlq_error, retry_count)

    def _deserialize_message(self, message: Any) -> DomainEvent:
        """Deserialize a Kafka message to a DomainEvent using the configured serializer."""
        event_type_name = self._get_header_value(message.headers, "event_type")

        if not event_type_name:
            raise DeserializationError("Message missing event_type header")

        event_class = self._resolve_event_class(event_type_name)

        if not event_class:
            raise DeserializationError(f"Unknown event type: {event_type_name}")

        return self._serializer.deserialize(message.value, event_type_name, event_class)

    def _get_header_value(
        self,
        headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None,
        key: str,
    ) -> str | None:
        """Get a header value by key."""
        return get_header_value(headers, key)

    async def _await_retry_after(
        self,
        headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None,
    ) -> None:
        """Wait until a republished message's scheduled retry time."""
        value = self._get_header_value(headers, "retry_after")
        if not value:
            return

        try:
            retry_after = float(value)
        except ValueError:
            logger.warning("Ignoring unparseable retry_after header: %r", value)
            return

        remaining = retry_after - datetime.now(UTC).timestamp()
        if remaining > 0:
            await asyncio.sleep(remaining)

    def _get_retry_count(
        self,
        headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None,
    ) -> int:
        """Get the retry count from message headers."""
        value = self._get_header_value(headers, "retry_count")
        if value:
            try:
                return int(value)
            except ValueError:
                return 0
        return 0
