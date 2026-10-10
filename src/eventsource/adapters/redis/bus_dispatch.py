"""Event dispatch and handler execution mixin for Redis event bus.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_HANDLER_SUCCESS,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters.redis.models import RedisEventBusStats
    from eventsource.observability import Tracer

logger = logging.getLogger("eventsource.adapters.redis")


class RedisBusDispatchMixin:
    """Event deserialization and handler dispatch for RedisEventBus."""

    _stats: RedisEventBusStats
    _tracer: Tracer

    if TYPE_CHECKING:

        def _resolve_event_class(self, event_type_name: str) -> type[DomainEvent] | None: ...
        def _handlers_for(self, event_type: type[DomainEvent]) -> tuple[HandlerAdapter, ...]: ...

    def _deserialize_event(
        self,
        event_type_name: str,
        message_data: dict[str, str],
    ) -> DomainEvent | None:
        """Deserialize an event from Redis message data."""
        event_class = self._resolve_event_class(event_type_name)
        if event_class is None:
            return None

        payload = message_data.get("payload", "{}")
        return event_class.model_validate_json(payload)

    async def _dispatch_event(self, event: DomainEvent, message_id: str) -> None:
        """Dispatch an event to all matching handlers."""
        event_type = type(event)
        handlers = self._handlers_for(event_type)

        if not handlers:
            logger.warning(
                f"No handlers registered for {event.event_type}",
                extra={"event_type": event.event_type},
            )
            return

        logger.debug(
            f"Dispatching {event.event_type} to {len(handlers)} handler(s)",
            extra={
                "event_type": event.event_type,
                "event_id": str(event.event_id),
                "handler_count": len(handlers),
            },
        )

        with self._tracer.span(
            "eventsource.event_bus.dispatch",
            {
                ATTR_EVENT_TYPE: event_type.__name__,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_HANDLER_COUNT: len(handlers),
                ATTR_MESSAGING_SYSTEM: "redis",
            },
        ):
            failures: list[tuple[str, Exception]] = []
            for adapter in handlers:
                exc = await self._invoke_handler(adapter, event, message_id)
                if exc is not None:
                    failures.append((adapter.name, exc))

            if failures:
                raise HandlerDispatchError(failures)

    async def _invoke_handler(
        self,
        adapter: HandlerAdapter,
        event: DomainEvent,
        message_id: str,
    ) -> Exception | None:
        """Invoke a single handler with tracing support."""
        with self._tracer.span(
            "eventsource.event_bus.handle",
            {
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_HANDLER_NAME: adapter.name,
                ATTR_MESSAGING_SYSTEM: "redis",
            },
        ) as span:
            try:
                await adapter.handle(event)
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, True)
                logger.debug(
                    f"Handler {adapter.name} processed {event.event_type}",
                    extra={
                        "handler": adapter.name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                    },
                )
                return None
            except Exception as e:
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, False)
                    span.record_exception(e)
                self._stats.handler_errors += 1
                logger.error(
                    f"Handler {adapter.name} failed: {e}",
                    exc_info=True,
                    extra={
                        "handler": adapter.name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "message_id": message_id,
                    },
                )
                return e


__all__ = ["RedisBusDispatchMixin"]
