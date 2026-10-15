"""Message processing and event dispatch mixin for RabbitMQ consumer.

Provides deserialization, distributed trace context propagation, handler dispatch
with error isolation, and delivery acknowledgment.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq import death_headers, serialization
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_HANDLER_SUCCESS,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats

# OpenTelemetry propagation import - kept separate for distributed tracing.
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
            self,
            message: AbstractIncomingMessage,
            error: Exception,
            retry_count: int,
        ) -> None: ...

    def _deserialize_event(self, message: AbstractIncomingMessage) -> DomainEvent | None:
        """Deserialize an AMQP message to a domain event."""
        return serialization.deserialize_event(message, self._resolve_event_class, self._logger)

    async def _process_message(self, message: AbstractIncomingMessage) -> None:
        """Process a single message from the queue with retry handling and tracing."""
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))
        raw_retry = headers.get("x-retry-count")
        retry_count = (
            0
            if raw_retry is None
            else (raw_retry if isinstance(raw_retry, int) else int(str(raw_retry)))
        )

        death_info = death_headers.get_death_info(message)
        is_redelivered = death_headers.is_from_dlq(message)

        log_extra: dict[str, Any] = {
            "message_id": message.message_id,
            "event_type": event_type_name,
            "routing_key": message.routing_key,
            "retry_count": retry_count,
        }

        if is_redelivered:
            log_extra.update(
                {
                    "is_dead_lettered": True,
                    "death_count": death_info["death_count"],
                    "first_death_queue": death_info["first_death_queue"],
                    "first_death_reason": death_info["first_death_reason"],
                    "original_routing_key": death_info["original_routing_key"],
                }
            )
            self._logger.info(
                f"Processing dead-lettered message: {event_type_name}", extra=log_extra
            )
        else:
            self._logger.debug(
                f"Processing message (attempt {retry_count + 1}): {event_type_name}",
                extra=log_extra,
            )

        processing_start = datetime.now(UTC)
        span = None
        ctx = None

        if self._enable_tracing and PROPAGATION_AVAILABLE and self._tracer is not None:
            from eventsource.adapters.rabbitmq import consumer as _consumer_mod

            if _consumer_mod.extract is not None:
                ctx = _consumer_mod.extract(dict(headers))

            span = self._tracer.start_span(
                "eventsource.event_bus.consume",
                kind=SpanKindEnum.CONSUMER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "rabbitmq",
                    ATTR_MESSAGING_DESTINATION: self._config.queue_name,
                    "messaging.destination_kind": "queue",
                    "messaging.message_id": message.message_id or "",
                    ATTR_EVENT_TYPE: event_type_name,
                    "messaging.rabbitmq.routing_key": message.routing_key or "",
                },
                context=ctx,
            )

        try:
            event = self._deserialize_event(message)
            if event is None:
                self._logger.warning(
                    f"Unknown event type: {event_type_name}, acknowledging to skip",
                    extra={"event_type": event_type_name, "message_id": message.message_id},
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
            processing_duration = (datetime.now(UTC) - processing_start).total_seconds()

            if span:
                span.set_status(Status(StatusCode.OK))

            self._logger.debug(
                f"Successfully processed {event_type_name}",
                extra={
                    "message_id": message.message_id,
                    "event_id": str(event.event_id),
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "duration_ms": processing_duration * 1000,
                    "success": True,
                },
            )

        except Exception as e:
            self._stats.events_processed_failed += 1
            self._stats.last_error_at = datetime.now(UTC)
            processing_duration = (datetime.now(UTC) - processing_start).total_seconds()
            error_extra: dict[str, Any] = {
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "duration_ms": processing_duration * 1000,
                "error": str(e),
                "error_type": type(e).__name__,
            }
            if is_redelivered:
                error_extra.update(
                    {
                        "is_dead_lettered": True,
                        "death_count": death_info["death_count"],
                        "first_death_queue": death_info["first_death_queue"],
                    }
                )
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            self._logger.error(f"Failed to process message: {e}", exc_info=True, extra=error_extra)
            dlq_error = (
                e.failures[0][1]
                if isinstance(e, HandlerDispatchError) and len(e.failures) == 1
                else e
            )
            await self._handle_failed_message(message, dlq_error, retry_count)

        finally:
            if span:
                span.end()

    async def _dispatch_event(
        self,
        event: DomainEvent,
        message: AbstractIncomingMessage,
        parent_span: Any = None,
    ) -> None:
        """Dispatch an event to all matching handlers with error isolation."""
        handlers = self._handlers_for(type(event))
        if not handlers:
            self._logger.warning(
                f"No handlers registered for {event.event_type}",
                extra={"event_type": event.event_type},
            )
            return

        if parent_span is not None:
            parent_span.set_attribute(ATTR_HANDLER_COUNT, len(handlers))

        self._logger.debug(
            f"Dispatching {event.event_type} to {len(handlers)} handler(s)",
            extra={
                "event_type": event.event_type,
                "event_id": str(event.event_id),
                "handler_count": len(handlers),
            },
        )

        failures: list[tuple[str, Exception]] = []
        for adapter in handlers:
            handler_name = adapter.name
            handler_start = datetime.now(UTC)
            handler_span = None

            if (
                self._enable_tracing
                and parent_span
                and PROPAGATION_AVAILABLE
                and self._tracer is not None
            ):
                handler_span = self._tracer.start_span(
                    "eventsource.event_bus.handle",
                    kind=SpanKindEnum.INTERNAL,
                    attributes={
                        ATTR_HANDLER_NAME: handler_name,
                        ATTR_EVENT_TYPE: event.event_type,
                        ATTR_EVENT_ID: str(event.event_id),
                    },
                )

            try:
                await adapter.handle(event)
                handler_duration = (datetime.now(UTC) - handler_start).total_seconds()

                if handler_span:
                    handler_span.set_attribute("handler.duration_ms", handler_duration * 1000)
                    handler_span.set_attribute(ATTR_HANDLER_SUCCESS, True)
                    handler_span.set_status(Status(StatusCode.OK))

                self._logger.debug(
                    f"Handler {handler_name} processed {event.event_type}",
                    extra={
                        "handler": handler_name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "duration_ms": handler_duration * 1000,
                    },
                )

            except Exception as e:
                self._stats.handler_errors += 1
                self._stats.last_error_at = datetime.now(UTC)
                handler_duration = (datetime.now(UTC) - handler_start).total_seconds()

                if handler_span:
                    handler_span.set_attribute(ATTR_HANDLER_SUCCESS, False)
                    handler_span.set_status(Status(StatusCode.ERROR, str(e)))
                    handler_span.record_exception(e)

                self._logger.error(
                    f"Handler {handler_name} failed: {e}",
                    exc_info=True,
                    extra={
                        "handler": handler_name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "message_id": message.message_id,
                        "duration_ms": handler_duration * 1000,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                failures.append((handler_name, e))

            finally:
                if handler_span:
                    handler_span.end()

        if failures:
            raise HandlerDispatchError(failures)
