"""Message processing and deserialization mixin for KafkaConsumerLoop."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.kafka.consumer_helpers import get_header_value, get_retry_delay_remaining
from eventsource.adapters.kafka.models import DeserializationError, KafkaEventBusStats
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability import attributes as attr

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters._bus.retry_scheduler import RetryScheduler
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.serialization import EventSerializer

try:
    from opentelemetry.propagate import extract
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = Status = StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

logger = logging.getLogger("eventsource.bus.kafka")
_Headers = list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None


class KafkaConsumerMessageMixin:
    """Mixin providing message intake, span extraction, and deserialization."""

    _config: KafkaEventBusConfig
    _stats: KafkaEventBusStats
    _metrics: KafkaEventBusMetrics | None
    _serializer: EventSerializer
    _handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]]
    _resolve_event_class: Callable[[str], type[DomainEvent] | None]
    _tracer: Tracer
    _enable_tracing: bool
    _consumer: Any
    _retry_scheduler: RetryScheduler
    _dispatch_to_handlers: Any
    _handle_processing_error: Any
    _send_to_dlq: Any
    _warn_uncommitted: Any

    async def _process_message(self, message: Any, skip_await_retry: bool = False) -> None:
        if not skip_await_retry and (remaining := get_retry_delay_remaining(message.headers)) > 0:

            async def _retry_action(msg: Any = message) -> None:
                await self._process_message(msg, skip_await_retry=True)

            name = f"kafka-retry-p{message.partition}-o{message.offset}"
            self._retry_scheduler.schedule(remaining, _retry_action, name=name)
            return
        self._stats.events_consumed += 1
        self._stats.last_consume_at = datetime.now(UTC)
        t, p, o = message.topic, message.partition, message.offset
        event_type_name = self._get_header_value(message.headers, "event_type")
        if self._metrics and event_type_name:
            m_attrs = {"messaging.system": "kafka", "messaging.destination": t}
            m_attrs.update({"messaging.kafka.partition": p, "event.type": event_type_name})
            self._metrics.messages_consumed.add(1, attributes=m_attrs)
        msg_meta = {"topic": t, "partition": p, "offset": o}
        if not event_type_name:
            logger.error("Message missing event_type header", extra=msg_meta)
            if self._consumer:
                await self._consumer.commit()
            return
        logger.debug("Processing message", extra={"event_type": event_type_name, **msg_meta})
        if self._enable_tracing and PROPAGATION_AVAILABLE and extract is not None:
            ctx = extract(self._extract_trace_context(message.headers))
            s_attrs = {attr.ATTR_MESSAGING_SYSTEM: "kafka", "messaging.source": t}
            s_attrs.update({"messaging.source_kind": "topic", "messaging.kafka.partition": p})
            s_attrs.update({attr.ATTR_MESSAGING_OPERATION: "receive", "messaging.kafka.offset": o})
            s_attrs["messaging.kafka.consumer_group"] = self._config.consumer_group
            s_attrs[attr.ATTR_EVENT_TYPE] = event_type_name
            name = f"eventsource.event_bus.consume {event_type_name}"
            with self._tracer.span_with_kind(
                name=name, kind=SpanKindEnum.CONSUMER, attributes=s_attrs, context=ctx
            ) as span:
                await self._process_message_with_span(message, event_type_name, span)
        else:
            await self._process_message_with_span(message, event_type_name, None)

    def _extract_trace_context(self, headers: _Headers) -> dict[str, str]:
        keys = ("traceparent", "tracestate", "baggage")
        return {k: v.decode("utf-8") for k, v in (headers or ()) if k in keys}

    def _fail_span(self, span: Any, exc: Exception) -> None:
        if span is not None and Status is not None and StatusCode is not None:
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            span.record_exception(exc)

    def _record_duration(self, start_time: float, topic: str) -> None:
        if self._metrics:
            dur = (time.perf_counter() - start_time) * 1000
            self._metrics.consume_duration.record(dur, attributes={"messaging.destination": topic})

    async def _process_message_with_span(
        self, message: Any, event_type_name: str, span: Any
    ) -> None:
        start_time, retry_count = time.perf_counter(), self._get_retry_count(message.headers)
        try:
            try:
                event = self._deserialize_message(message)
            except DeserializationError as e:
                err_extra = {"event_type": event_type_name, "error": str(e)}
                logger.error("Deserialization error, sending directly to DLQ", extra=err_extra)
                if await self._send_to_dlq(message, e, retry_count, reason="deserialization_error"):
                    if self._consumer:
                        await self._consumer.commit()
                else:
                    self._warn_uncommitted(message, "deserialization_error")
                self._fail_span(span, e)
                return self._record_duration(start_time, message.topic)
            if span is not None:
                span.set_attribute(attr.ATTR_EVENT_ID, str(event.event_id))
                span.set_attribute(attr.ATTR_AGGREGATE_ID, str(event.aggregate_id))
                span.set_attribute(attr.ATTR_AGGREGATE_TYPE, event.aggregate_type)
            if handlers := self._handlers_for(type(event)):
                await self._dispatch_to_handlers(event, handlers)
            else:
                logger.debug("No handlers for event type", extra={"event_type": event_type_name})
            if self._consumer:
                await self._consumer.commit()
            self._stats.events_processed_success += 1
            if span is not None and Status is not None and StatusCode is not None:
                span.set_status(Status(StatusCode.OK))
            self._record_duration(start_time, message.topic)
        except Exception as e:
            self._record_duration(start_time, message.topic)
            self._fail_span(span, e)
            if isinstance(e, HandlerDispatchError) and len(e.failures) == 1:
                e = e.failures[0][1]
            await self._handle_processing_error(message, e, retry_count)

    def _deserialize_message(self, message: Any) -> DomainEvent:
        if not (event_type_name := self._get_header_value(message.headers, "event_type")):
            raise DeserializationError("Message missing event_type header")
        if not (event_class := self._resolve_event_class(event_type_name)):
            raise DeserializationError(f"Unknown event type: {event_type_name}")
        return self._serializer.deserialize(message.value, event_type_name, event_class)

    def _get_header_value(self, headers: _Headers, key: str) -> str | None:
        return get_header_value(headers, key)

    def _get_retry_count(self, headers: _Headers) -> int:
        try:
            return int(val) if (val := self._get_header_value(headers, "retry_count")) else 0
        except ValueError:
            return 0
