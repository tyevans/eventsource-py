"""Handler dispatch mixin for KafkaConsumerLoop."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
)

if TYPE_CHECKING:
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats

try:
    from opentelemetry.trace import Status, StatusCode
except ImportError:
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaConsumerDispatchMixin:
    """Mixin providing handler dispatch and execution tracing for Kafka consumer."""

    _enable_tracing: bool
    _tracer: Tracer
    _metrics: KafkaEventBusMetrics | None
    _stats: KafkaEventBusStats

    async def _dispatch_to_handlers(
        self,
        event: DomainEvent,
        handlers: tuple[HandlerAdapter, ...],
    ) -> None:
        """Dispatch an event to all registered handlers with error isolation and tracing."""
        failures: list[tuple[str, Exception]] = []

        for adapter in handlers:
            handler_name = adapter.name
            handler_start_time = time.perf_counter()
            metric_attrs = {"handler.name": handler_name, "event.type": event.event_type}

            if self._enable_tracing:
                span_attrs = {
                    ATTR_HANDLER_NAME: handler_name,
                    ATTR_EVENT_TYPE: event.event_type,
                    ATTR_EVENT_ID: str(event.event_id),
                }
                with self._tracer.span_with_kind(
                    name=f"eventsource.event_bus.dispatch {handler_name}",
                    kind=SpanKindEnum.INTERNAL,
                    attributes=span_attrs,
                ) as span:
                    try:
                        logger.debug(
                            "Dispatching to handler",
                            extra={
                                "event_type": event.event_type,
                                "event_id": str(event.event_id),
                                "handler": handler_name,
                            },
                        )
                        await adapter.handle(event)
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms, attributes=metric_attrs
                            )
                            self._metrics.handler_invocations.add(1, attributes=metric_attrs)
                        logger.debug(
                            "Handler completed",
                            extra={"event_type": event.event_type, "handler": handler_name},
                        )
                    except Exception as e:
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms, attributes=metric_attrs
                            )
                            self._metrics.handler_errors.add(
                                1, attributes={**metric_attrs, "error.type": type(e).__name__}
                            )
                        if span is not None and Status is not None and StatusCode is not None:
                            span.set_status(Status(StatusCode.ERROR, str(e)))
                            span.record_exception(e)
                        self._stats.handler_errors += 1
                        logger.error(
                            "Handler error",
                            extra={
                                "event_type": event.event_type,
                                "event_id": str(event.event_id),
                                "handler": handler_name,
                                "error": str(e),
                            },
                            exc_info=True,
                        )
                        failures.append((handler_name, e))
            else:
                try:
                    logger.debug(
                        "Dispatching to handler",
                        extra={
                            "event_type": event.event_type,
                            "event_id": str(event.event_id),
                            "handler": handler_name,
                        },
                    )
                    await adapter.handle(event)
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms, attributes=metric_attrs
                        )
                        self._metrics.handler_invocations.add(1, attributes=metric_attrs)
                    logger.debug(
                        "Handler completed",
                        extra={"event_type": event.event_type, "handler": handler_name},
                    )
                except Exception as e:
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms, attributes=metric_attrs
                        )
                        self._metrics.handler_errors.add(
                            1, attributes={**metric_attrs, "error.type": type(e).__name__}
                        )
                    self._stats.handler_errors += 1
                    logger.error(
                        "Handler error",
                        extra={
                            "event_type": event.event_type,
                            "event_id": str(event.event_id),
                            "handler": handler_name,
                            "error": str(e),
                        },
                        exc_info=True,
                    )
                    failures.append((handler_name, e))

        if failures:
            raise HandlerDispatchError(failures)
