"""Handler dispatch mixin for KafkaConsumerLoop.

Governed by ADR-0002 (<500 lines per module).
"""

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
        """Dispatch an event to all registered handlers with optional tracing.

        Every handler runs for this delivery even if an earlier one failed
        (error isolation) -- failures are collected and raised together as a
        single HandlerDispatchError afterward, so the caller's retry/DLQ path
        still sees the delivery as failed exactly as a single raise would.

        When tracing is enabled, creates child spans for each handler
        invocation to provide detailed visibility into handler execution.

        Args:
            event: The event to dispatch.
            handlers: Tuple of HandlerAdapter instances to invoke.

        Raises:
            HandlerDispatchError: If one or more handlers raise.
        """
        failures: list[tuple[str, Exception]] = []

        for adapter in handlers:
            handler_name = adapter.name

            # Start timing for handler duration histogram
            handler_start_time = time.perf_counter()

            # Use composition-based tracer for tracing
            if self._enable_tracing:
                with self._tracer.span_with_kind(
                    name=f"eventsource.event_bus.dispatch {handler_name}",
                    kind=SpanKindEnum.INTERNAL,
                    attributes={
                        ATTR_HANDLER_NAME: handler_name,
                        ATTR_EVENT_TYPE: event.event_type,
                        ATTR_EVENT_ID: str(event.event_id),
                    },
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

                        # Record handler duration and increment counter on success
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )
                            self._metrics.handler_invocations.add(
                                1,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )

                        logger.debug(
                            "Handler completed",
                            extra={
                                "event_type": event.event_type,
                                "handler": handler_name,
                            },
                        )

                    except Exception as e:
                        # Record handler duration on error path
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )
                            self._metrics.handler_errors.add(
                                1,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                    "error.type": type(e).__name__,
                                },
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

                    # Record handler duration and increment counter on success
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )
                        self._metrics.handler_invocations.add(
                            1,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )

                    logger.debug(
                        "Handler completed",
                        extra={
                            "event_type": event.event_type,
                            "handler": handler_name,
                        },
                    )

                except Exception as e:
                    # Record handler duration on error path
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )
                        self._metrics.handler_errors.add(
                            1,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                                "error.type": type(e).__name__,
                            },
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
