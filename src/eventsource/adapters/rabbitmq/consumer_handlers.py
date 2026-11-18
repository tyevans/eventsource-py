"""Handler fan-out with per-handler spans and error containment for the RabbitMQ consumer.

``HandlerDispatchError`` carries every handler failure out to the caller, which
decides between retry and DLQ. Payload values stay out of message text: the
error is reduced with ``error_summary`` before it reaches a log or span.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability import attributes as attr
from eventsource.observability.error_summary import error_summary
from eventsource.observability.span_errors import record_error

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
    from eventsource.domain.event import DomainEvent

try:
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    Status = StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False


class RabbitMQConsumerHandlerMixin:
    """Mixin providing per-event handler dispatch for RabbitMQConsumer."""

    _stats: RabbitMQEventBusStats
    _handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]]
    _tracer: Tracer | None
    _enable_tracing: bool
    _logger: logging.Logger

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
        self._logger.debug("Dispatching event to handlers", extra={**base_extra, "handler_count": len(handlers)})  # fmt: skip

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
                summary = error_summary(e)
                if h_span:
                    h_span.set_attribute(attr.ATTR_HANDLER_SUCCESS, False)
                    record_error(h_span, e)
                err_ex = {**base_extra, "handler": h_name, "message_id": message.message_id, "duration_ms": dur_ms, "error": summary}  # fmt: skip
                self._logger.error("Handler failed", extra={**err_ex, "handler": h_name})
                failures.append((h_name, e))
            finally:
                if h_span:
                    h_span.end()

        if failures:
            raise HandlerDispatchError(failures)


__all__ = ["RabbitMQConsumerHandlerMixin"]
