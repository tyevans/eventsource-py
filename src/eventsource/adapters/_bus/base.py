"""Concrete base class shared by all EventBus implementations.

All four backends keep their subscribers in process memory, resolve event
classes by name from a registry, and spawn fire-and-forget tasks they must
drain on shutdown. That shared behavior lives here, so ``interface.py`` can
stay a pure ABC that third parties may implement directly.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.adapters._bus.registry import SubscriptionRegistry
from eventsource.application.background_tasks import BackgroundTaskManager
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.ports.bus import EventBus, EventHandlerFunc
from eventsource.ports.handlers import FlexibleEventHandler, FlexibleEventSubscriber

logger = logging.getLogger(__name__)

DEFAULT_MAX_BACKGROUND_TASKS = 1000
"""Default ceiling on in-flight ``publish(background=True)`` tasks."""


class BaseEventBus(EventBus):
    """Base class providing subscription management and background tasks."""

    def __init__(
        self,
        *,
        event_registry: EventRegistry | None = None,
        max_background_tasks: int | None = DEFAULT_MAX_BACKGROUND_TASKS,
    ) -> None:
        """Initialize shared bus state."""
        self._registry = SubscriptionRegistry()
        self._event_registry = event_registry
        self._tasks = BackgroundTaskManager(max_pending=max_background_tasks)

    @property
    def max_background_tasks(self) -> int | None:
        """Ceiling on in-flight background publish tasks, or None if unbounded."""
        return self._tasks.max_pending

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        adapter = self._registry.add(event_type, handler)
        logger.info(
            f"Registered handler {adapter.name} for {event_type.__name__}",
            extra={"handler": adapter.name, "event_type": event_type.__name__},
        )

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        removed = self._registry.remove(event_type, handler)
        logger.info(
            f"Unsubscribe from {event_type.__name__}: "
            f"{'removed' if removed else 'handler not found'}",
            extra={"event_type": event_type.__name__, "removed": removed},
        )
        return removed

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        self._registry.add_subscriber(subscriber)

    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        adapter = self._registry.add_wildcard(handler)
        logger.info(
            f"Registered wildcard handler {adapter.name}",
            extra={"handler": adapter.name},
        )

    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        removed = self._registry.remove_wildcard(handler)
        logger.info(
            f"Wildcard unsubscribe: {'removed' if removed else 'handler not found'}",
            extra={"removed": removed},
        )
        return removed

    def clear_subscribers(self) -> None:
        """Remove all subscribers. Useful for testing and reinitialization."""
        self._registry.clear()
        logger.info("All event subscribers cleared")

    def get_subscriber_count(self, event_type: type[DomainEvent] | None = None) -> int:
        """Count type-specific subscribers. Excludes wildcard subscribers."""
        return self._registry.count(event_type)

    def get_wildcard_subscriber_count(self) -> int:
        """Count wildcard subscribers."""
        return self._registry.wildcard_count()

    def _handlers_for(self, event_type: type[DomainEvent]) -> tuple[HandlerAdapter, ...]:
        """Get the cached specific-then-wildcard handler tuple for dispatch."""
        return self._registry.handlers_for(event_type)

    def _resolve_event_class(self, event_type_name: str) -> type[DomainEvent] | None:
        """Resolve an event class by name, for deserializing consumed messages."""
        if self._event_registry is not None:
            return self._event_registry.get_or_none(event_type_name)

        from eventsource.domain.event_registry import default_registry

        return default_registry.get_or_none(event_type_name)

    async def _track_background(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None] | None:
        """Schedule a coroutine as a tracked background task, or run inline at capacity."""
        if self._tasks.at_capacity:
            logger.debug(
                "Background publish at capacity (%s); running inline",
                self._tasks.max_pending,
            )
            await coro
            return None
        return self._tasks.submit(coro, on_done=self._on_background_task_done)

    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        """Log any unexpected failure."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(f"Background task failed: {exc}", exc_info=exc)

    @property
    def _background_tasks(self) -> set[asyncio.Task[None]]:
        """In-flight background tasks. Exposed for introspection/tests."""
        return self._tasks.tasks

    def get_background_task_count(self) -> int:
        """Number of background tasks currently in flight."""
        return self._tasks.pending_count

    async def _drain_background(self, timeout: float = 30.0) -> None:
        """Wait for background tasks to finish, cancelling any stragglers."""
        pending = list(self._tasks.tasks)
        if not pending:
            return

        logger.info(f"Draining {len(pending)} background task(s)")
        try:
            _done, remaining = await asyncio.wait(
                pending, timeout=timeout, return_when=asyncio.ALL_COMPLETED
            )
            if remaining:
                logger.warning(
                    f"{len(remaining)} background task(s) did not complete within "
                    f"{timeout}s; cancelling",
                    extra={"remaining_tasks": len(remaining)},
                )
                for task in remaining:
                    task.cancel()
                await asyncio.gather(*remaining, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error draining background tasks: {e}", exc_info=True)


__all__ = ["BaseEventBus"]
