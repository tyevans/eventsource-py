"""In-memory event bus for distributing domain events within the same process."""

import asyncio
import logging
import threading
from collections import deque
from typing import TypeVar

from eventsource.adapters._bus.base import DEFAULT_MAX_BACKGROUND_TASKS, BaseEventBus
from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_HANDLER_SUCCESS,
)

logger = logging.getLogger(__name__)
TEvent = TypeVar("TEvent", bound=DomainEvent)


class InMemoryEventBus(BaseEventBus):
    """In-memory event bus for event distribution within the same process."""

    def __init__(
        self,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        event_registry: EventRegistry | None = None,
        max_background_tasks: int | None = DEFAULT_MAX_BACKGROUND_TASKS,
    ) -> None:
        super().__init__(event_registry=event_registry, max_background_tasks=max_background_tasks)
        self._stats = {
            "events_published": 0,
            "handlers_invoked": 0,
            "handler_errors": 0,
            "background_tasks_created": 0,
            "background_tasks_completed": 0,
        }
        self._published: deque[DomainEvent] = deque(maxlen=10_000)
        self._published_lock = threading.RLock()
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

    @property
    def published_events(self) -> list[DomainEvent]:
        """Get a copy of the published events in publication order."""
        with self._published_lock:
            return list(self._published)

    def clear_published_events(self) -> None:
        """Clear the recorded published events."""
        with self._published_lock:
            self._published.clear()

    def clear(self) -> None:
        """Clear recorded published events (alias for clear_published_events)."""
        self.clear_published_events()

    def __len__(self) -> int:
        with self._published_lock:
            return len(self._published)

    def __bool__(self) -> bool:
        return True

    def published_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        """Return published events matching ``event_type`` in publication order."""
        return [e for e in self.published_events if isinstance(e, event_type)]

    async def publish(self, events: list[DomainEvent], background: bool = False) -> None:
        """Publish events sequentially to all registered subscribers."""
        if not events:
            return
        if background:
            await self._track_background(self._publish_all(events))
            self._stats["background_tasks_created"] += 1
            logger.debug(
                f"Scheduled background publishing of {len(events)} event(s)",
                extra={"event_count": len(events)},
            )
        else:
            await self._publish_all(events)

    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        super()._on_background_task_done(task)
        self._stats["background_tasks_completed"] += 1

    async def _publish_all(self, events: list[DomainEvent]) -> None:
        for event in events:
            with self._published_lock:
                self._published.append(event)
            await self._dispatch_event(event)
            self._stats["events_published"] += 1

    async def _dispatch_event(self, event: DomainEvent) -> None:
        event_type = type(event)
        handlers = self._handlers_for(event_type)
        if not handlers:
            logger.debug(
                f"No handlers registered for event type: {event_type.__name__}",
                extra={"event_type": event_type.__name__},
            )
            return
        attrs = {
            ATTR_EVENT_TYPE: event_type.__name__,
            ATTR_EVENT_ID: str(event.event_id),
            ATTR_AGGREGATE_ID: str(event.aggregate_id),
            ATTR_HANDLER_COUNT: len(handlers),
        }
        logger.debug(
            f"Dispatching {event_type.__name__} to {len(handlers)} handler(s)",
            extra={
                "event_type": event_type.__name__,
                "event_id": str(event.event_id),
                "aggregate_id": str(event.aggregate_id),
                "handler_count": len(handlers),
            },
        )
        with self._tracer.span("eventsource.event_bus.dispatch", attrs):
            await self._invoke_handlers(handlers, event)

    async def _invoke_handlers(
        self, handlers: tuple[HandlerAdapter, ...], event: DomainEvent
    ) -> None:
        await asyncio.gather(
            *(self._safe_handle(a, event) for a in handlers), return_exceptions=True
        )

    async def _safe_handle(self, adapter: HandlerAdapter, event: DomainEvent) -> None:
        etype, eid = type(event).__name__, str(event.event_id)
        attrs = {
            ATTR_EVENT_TYPE: etype,
            ATTR_EVENT_ID: eid,
            ATTR_AGGREGATE_ID: str(event.aggregate_id),
            ATTR_HANDLER_NAME: adapter.name,
        }
        with self._tracer.span("eventsource.event_bus.handle", attrs) as span:
            try:
                await adapter.handle(event)
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, True)
                self._stats["handlers_invoked"] += 1
                logger.debug(
                    f"Handler {adapter.name} processed {etype}",
                    extra={"handler": adapter.name, "event_type": etype, "event_id": eid},
                )
            except Exception as e:
                if span:
                    span.set_attribute(ATTR_HANDLER_SUCCESS, False)
                    span.record_exception(e)
                self._stats["handler_errors"] += 1
                logger.error(
                    f"Handler {adapter.name} failed processing {etype}: {e}",
                    exc_info=True,
                    extra={
                        "handler": adapter.name,
                        "event_type": etype,
                        "event_id": eid,
                        "error": str(e),
                    },
                )

    def get_stats(self) -> dict[str, int]:
        """Get statistics about event bus operation."""
        return dict(self._stats)

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the event bus and wait for background tasks to complete."""
        await self._drain_background(timeout)
        logger.info("Event bus shutdown complete")


__all__ = ["InMemoryEventBus"]
