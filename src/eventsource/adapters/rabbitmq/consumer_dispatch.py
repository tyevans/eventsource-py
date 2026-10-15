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
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
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
        ) -> None: ...

    def _deserialize_event(self, message: AbstractIncomingMessage) -> DomainEvent | None:
        return serialization.deserialize_event(message, self._resolve_event_class, self._logger)

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        """Process a single message from the queue with retry handling and tracing."""
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))
        raw_retry = headers.get("x-retry-count")
        retry_count = int(str(raw_retry)) if raw_retry is not None else 0
        death_info, is_redelivered = (
            death_headers.get_death_info(message),
            death_headers.is_from_dlq(message),
        )
        log_extra: dict[str, Any] = {
            "message_id": message.message_id,
            "event_type": event_type_name,
            "routing_key": message.routing_key,
            "retry_count": retry_count,
        }
        if is_redelivered:
            log_extra.update(
                is_dead_lettered=True,
                death_count=death_info["death_count"],
                first_death_queue=death_info["first_death_queue"],
                first_death_reason=death_info["first_death_reason"],
                original_routing_key=death_info["original_routing_key"],
            )
            msg = f"Processing dead-lettered message: {event_type_name}"
            self._logger.info(msg, extra=log_extra)
        else:
            msg = f"Processing message (attempt {retry_count + 1}): {event_type_name}"
            self._logger.debug(msg, extra=log_extra)

        processing_start = datetime.now(UTC)
        span = None
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
            span = self._tracer.start_span(
                "eventsource.event_bus.consume", SpanKindEnum.CONSUMER, span_attrs, context=ctx
            )

        try:
            event = self._deserialize_event(message)
            if event is None:
                skip_extra = {"event_type": event_type_name, "message_id": message.message_id}
                self._logger.warning(
                    f"Unknown event type: {event_type_name}, acknowledging to skip",
                    extra=skip_extra,
                )
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
            ok_extra = {
                **log_extra,
                "event_id": str(event.event_id),
                "duration_ms": duration_ms,
                "success": True,
            }
            self._logger.debug(f"Successfully processed {event_type_name}", extra=ok_extra)
        except Exception as e:
            self._stats.events_processed_failed += 1
            self._stats.last_error_at = datetime.now(UTC)
            duration_ms = (datetime.now(UTC) - processing_start).total_seconds() * 1000
            error_extra: dict[str, Any] = {
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "duration_ms": duration_ms,
                "error": str(e),
                "error_type": type(e).__name__,
            }
            if is_redelivered:
                error_extra.update(
                    is_dead_lettered=True,
                    death_count=death_info["death_count"],
                    first_death_queue=death_info["first_death_queue"],
                )
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
            self._logger.error(f"Failed to process message: {e}", exc_info=True, extra=error_extra)
            dlq_err = (
                e.failures[0][1]
                if isinstance(e, HandlerDispatchError) and len(e.failures) == 1
                else e
            )
            await self._handle_failed_message(message, dlq_err, retry_count)
        finally:
            if span:
                span.end()

    async def _dispatch_event(
        self, event: DomainEvent, message: AbstractIncomingMessage, parent_span: Any = None
    ) -> None:
        """Dispatch an event to all matching handlers with error isolation."""
        handlers = self._handlers_for(type(event))
        if not handlers:
            msg = f"No handlers registered for {event.event_type}"
            self._logger.warning(msg, extra={"event_type": event.event_type})
            return

        if parent_span is not None:
            parent_span.set_attribute(attr.ATTR_HANDLER_COUNT, len(handlers))
        event_id_str = str(event.event_id)
        base_extra = {"event_type": event.event_type, "event_id": event_id_str}
        msg = f"Dispatching {event.event_type} to {len(handlers)} handler(s)"
        self._logger.debug(msg, extra={**base_extra, "handler_count": len(handlers)})

        failures: list[tuple[str, Exception]] = []
        use_tracer = bool(self._enable_tracing and parent_span and PROPAGATION_AVAILABLE)
        tracer = self._tracer if use_tracer else None
        for adapter in handlers:
            handler_name, handler_start = adapter.name, datetime.now(UTC)
            span_attrs = {
                attr.ATTR_HANDLER_NAME: handler_name,
                attr.ATTR_EVENT_TYPE: event.event_type,
                attr.ATTR_EVENT_ID: event_id_str,
            }
            span_name = "eventsource.event_bus.handle"
            h_span = (
                tracer.start_span(span_name, SpanKindEnum.INTERNAL, span_attrs) if tracer else None
            )
            try:
                await adapter.handle(event)
                duration_ms = (datetime.now(UTC) - handler_start).total_seconds() * 1000
                if h_span:
                    h_span.set_attribute("handler.duration_ms", duration_ms)
                    h_span.set_attribute(attr.ATTR_HANDLER_SUCCESS, True)
                    h_span.set_status(Status(StatusCode.OK))
                ok_extra = {**base_extra, "handler": handler_name, "duration_ms": duration_ms}
                self._logger.debug(
                    f"Handler {handler_name} processed {event.event_type}", extra=ok_extra
                )
            except Exception as e:
                self._stats.handler_errors += 1
                self._stats.last_error_at = datetime.now(UTC)
                duration_ms = (datetime.now(UTC) - handler_start).total_seconds() * 1000
                if h_span:
                    h_span.set_attribute(attr.ATTR_HANDLER_SUCCESS, False)
                    h_span.set_status(Status(StatusCode.ERROR, str(e)))
                    h_span.record_exception(e)
                err_extra = {
                    **base_extra,
                    "handler": handler_name,
                    "message_id": message.message_id,
                    "duration_ms": duration_ms,
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
                err_msg = f"Handler {handler_name} failed: {e}"
                self._logger.error(err_msg, exc_info=True, extra=err_extra)
                failures.append((handler_name, e))
            finally:
                if h_span:
                    h_span.end()

        if failures:
            raise HandlerDispatchError(failures)
