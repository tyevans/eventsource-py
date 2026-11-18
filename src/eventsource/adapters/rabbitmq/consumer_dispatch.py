"""Message processing and event dispatch mixin for RabbitMQ consumer."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.headers import read_header_int, read_header_str
from eventsource.adapters.rabbitmq import death_headers, serialization
from eventsource.adapters.rabbitmq.consumer_handlers import RabbitMQConsumerHandlerMixin
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability import attributes as attr
from eventsource.observability.error_summary import error_summary
from eventsource.observability.span_errors import record_error

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

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


class RabbitMQConsumerDispatchMixin(RabbitMQConsumerHandlerMixin):
    """Mixin providing message processing and event dispatch for RabbitMQConsumer."""

    _config: RabbitMQEventBusConfig
    _stats: RabbitMQEventBusStats
    _resolve_event_class: Callable[[str], type[DomainEvent] | None]
    _tracer: Tracer | None
    _enable_tracing: bool
    _logger: logging.Logger

    if TYPE_CHECKING:

        async def _handle_failed_message(
            self, message: AbstractIncomingMessage, error: Exception, retry_count: int
        ) -> None: ...  # fmt: skip

        async def _send_to_dlq(
            self,
            message: AbstractIncomingMessage,
            error: Exception,
            retry_count: int,
            *,
            reason: str | None = None,
        ) -> None: ...  # fmt: skip

    def _deserialize_event(self, message: AbstractIncomingMessage) -> DomainEvent:
        """Decode a message to an event; raises EventDecodeError when it cannot be decoded."""
        return serialization.decode_event(message, self._resolve_event_class, self._logger)

    async def _dead_letter_undecodable(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
        span: Any,
    ) -> None:
        """Send a message that cannot be decoded to the DLQ, then acknowledge it."""
        record_error(span, error)
        await self._send_to_dlq(message, error, retry_count)
        await message.ack()

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        headers = message.headers or {}
        event_type_name = read_header_str(headers.get("event_type"), "unknown") or "unknown"
        retry_count = read_header_int(headers.get("x-retry-count"), 0)
        death_info = death_headers.get_death_info(message)
        log_extra: dict[str, Any] = {"message_id": message.message_id, "event_type": event_type_name, "routing_key": message.routing_key, "retry_count": retry_count}  # fmt: skip
        if death_headers.is_from_dlq(message):
            log_extra.update(is_dead_lettered=True, death_count=death_info["death_count"], first_death_queue=death_info["first_death_queue"], first_death_reason=death_info["first_death_reason"], original_routing_key=death_info["original_routing_key"])  # fmt: skip
            self._logger.info("Processing dead-lettered message", extra=log_extra)  # fmt: skip
        else:
            self._logger.debug("Processing message", extra=log_extra)  # fmt: skip

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
            try:
                event = self._deserialize_event(message)
            except serialization.EventDecodeError as e:
                await self._dead_letter_undecodable(message, e, retry_count, span)
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
            self._logger.debug("Successfully processed message", extra={**log_extra, "event_id": str(event.event_id), "duration_ms": duration_ms, "success": True})  # fmt: skip
        except Exception as e:
            self._stats.events_processed_failed += 1
            self._stats.last_error_at = datetime.now(UTC)
            duration_ms = (datetime.now(UTC) - processing_start).total_seconds() * 1000
            summary = error_summary(e)
            record_error(span, e)
            # No exc_info: the traceback repeats str(e), which can echo payload values.
            self._logger.error("Failed to process message", extra={**log_extra, "duration_ms": duration_ms, "error": summary})  # fmt: skip
            dlq_err = e.failures[0][1] if isinstance(e, HandlerDispatchError) and len(e.failures) == 1 else e  # fmt: skip
            await self._handle_failed_message(message, dlq_err, retry_count)
        finally:
            if span:
                span.end()
