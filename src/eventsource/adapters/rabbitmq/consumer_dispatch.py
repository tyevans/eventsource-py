"""Message processing and event dispatch mixin for RabbitMQ consumer."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq import death_headers, serialization
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability import attributes as attr

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats

try:
    from opentelemetry.propagate import extract
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = None  # type: ignore[assignment]
    Status = StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False


class RabbitMQConsumerDispatchMixin:
    """Mixin providing message processing and event dispatch for RabbitMQConsumer."""

    _config: RabbitMQEventBusConfig
    _stats: RabbitMQEventBusStats
    _handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]]
    _resolve_event_class: Callable[[str], type[DomainEvent] | None]
    _tracer: Tracer | None
    _enable_tracing: bool
    _logger: logging.Logger

    if TYPE_CHECKING:

        async def _handle_failed_message(
            self, message: AbstractIncomingMessage, error: Exception, retry_count: int
        ) -> None: ...  # fmt: skip

    def _deserialize_event(self, message: AbstractIncomingMessage) -> DomainEvent | None:
        return serialization.deserialize_event(message, self._resolve_event_class, self._logger)

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))
        raw_retry = headers.get("x-retry-count")
        retry_count = int(str(raw_retry)) if raw_retry is not None else 0
        death_info = death_headers.get_death_info(message)
        log_extra: dict[str, Any] = {"message_id": message.message_id, "event_type": event_type_name, "routing_key": message.routing_key, "retry_count": retry_count}  # fmt: skip
        if death_headers.is_from_dlq(message):
            log_extra.update(is_dead_lettered=True, death_count=death_info["death_count"], first_death_queue=death_info["first_death_queue"], first_death_reason=death_info["first_death_reason"], original_routing_key=death_info["original_routing_key"])  # fmt: skip
            self._logger.info(f"Processing dead-lettered message: {event_type_name}", extra=log_extra)  # fmt: skip
        else:
            self._logger.debug(f"Processing message (attempt {retry_count + 1}): {event_type_name}", extra=log_extra)  # fmt: skip

        processing_start, span = datetime.now(UTC), None
        if self._enable_tracing and PROPAGATION_AVAILABLE and self._tracer is not None:
            from eventsource.adapters.rabbitmq import consumer as _c

            ctx = _c.extract(dict(headers)) if _c.extract is not None else None
            span_attrs = {
                attr.ATTR_MESSAGING_SYSTEM: "rabbitmq",
                attr.ATTR_MESSAGING_DESTINATION: self._config.queue_name,
                "messaging.destination_kind": "queue",
                "messaging.message_id": message.message_id or "",
                attr.ATTR_EVENT_TYPE: event_type_name,
                "messaging.rabbitmq.routing_key": message.routing_key or "",
            }
            span = self._tracer.start_span("eventsource.event_bus.consume", SpanKindEnum.CONSUMER, span_attrs, ctx)  # fmt: skip

        try:
            if (event := self._deserialize_event(message)) is None:
                self._logger.warning(f"Unknown event type: {event_type_name}, acknowledging to skip", extra={"event_type": event_type_name, "message_id": message.message_id})  # fmt: skip
                if span:
                    span.set_attribute("event.unknown_type", True)
                    span.set_status(Status(StatusCode.OK, "Unknown event type"))
                await message.ack()
                return

            if span:
                span.set_attribute("event.id", str(event.event_id))
            await self._dispatch_event(event, message, span)
            await message.ack()
            self._stats.events_consumed += 1
            self._stats.events_processed_success += 1
            self._stats.last_consume_at = datetime.now(UTC)
            duration_ms = (datetime.now(UTC) - processing_start).total_seconds() * 1000
            if span:
                span.set_status(Status(StatusCode.OK))
            self._logger.debug(f"Successfully processed {event_type_name}", extra={**log_extra, "event_id": str(event.event_id), "duration_ms": duration_ms, "success": True})  # fmt: skip
        except Exception as e:
            self._stats.events_processed_failed += 1
            self._stats.last_error_at = datetime.now(UTC)
            duration_ms = (datetime.now(UTC) - processing_start).total_seconds() * 1000
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
            self._logger.error(f"Failed to process message: {e}", exc_info=True, extra={**log_extra, "duration_ms": duration_ms, "error": str(e), "error_type": type(e).__name__})  # fmt: skip
            dlq_err = e.failures[0][1] if isinstance(e, HandlerDispatchError) and len(e.failures) == 1 else e  # fmt: skip
            await self._handle_failed_message(message, dlq_err, retry_count)
        finally:
            if span:
                span.end()

    async def _dispatch_event(
        self, event: DomainEvent, message: AbstractIncomingMessage, parent_span: Any = None
    ) -> None:
        if not (handlers := self._handlers_for(type(event))):
            self._logger.warning(f"No handlers registered for {event.event_type}", extra={"event_type": event.event_type})  # fmt: skip
            return

        if parent_span is not None:
            parent_span.set_attribute(attr.ATTR_HANDLER_COUNT, len(handlers))
        eid_str, etype = str(event.event_id), event.event_type
        base_extra: dict[str, Any] = {"event_type": etype, "event_id": eid_str}
        self._logger.debug(f"Dispatching {etype} to {len(handlers)} handler(s)", extra={**base_extra, "handler_count": len(handlers)})  # fmt: skip

        failures: list[tuple[str, Exception]] = []
        tracer = self._tracer if (self._enable_tracing and parent_span and PROPAGATION_AVAILABLE) else None  # fmt: skip
        for adapter in handlers:
            h_name, h_start = adapter.name, datetime.now(UTC)
            s_attrs = {attr.ATTR_HANDLER_NAME: h_name, attr.ATTR_EVENT_TYPE: etype, attr.ATTR_EVENT_ID: eid_str}  # fmt: skip
            h_span = tracer.start_span("eventsource.event_bus.handle", SpanKindEnum.INTERNAL, s_attrs) if tracer else None  # fmt: skip
            try:
                await adapter.handle(event)
                dur_ms = (datetime.now(UTC) - h_start).total_seconds() * 1000
                if h_span:
                    h_span.set_attribute("handler.duration_ms", dur_ms)
                    h_span.set_attribute(attr.ATTR_HANDLER_SUCCESS, True)
                    h_span.set_status(Status(StatusCode.OK))
                self._logger.debug(f"Handler {h_name} processed {etype}", extra={**base_extra, "handler": h_name, "duration_ms": dur_ms})  # fmt: skip
            except Exception as e:
                self._stats.handler_errors += 1
                self._stats.last_error_at = datetime.now(UTC)
                dur_ms = (datetime.now(UTC) - h_start).total_seconds() * 1000
                if h_span:
                    h_span.set_attribute(attr.ATTR_HANDLER_SUCCESS, False)
                    h_span.set_status(Status(StatusCode.ERROR, str(e)))
                    h_span.record_exception(e)
                err_ex = {**base_extra, "handler": h_name, "message_id": message.message_id, "duration_ms": dur_ms, "error": str(e), "error_type": type(e).__name__}  # fmt: skip
                self._logger.error(f"Handler {h_name} failed: {e}", exc_info=True, extra=err_ex)
                failures.append((h_name, e))
            finally:
                if h_span:
                    h_span.end()

        if failures:
            raise HandlerDispatchError(failures)
