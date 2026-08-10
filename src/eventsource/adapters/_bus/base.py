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
"""Default ceiling on in-flight ``publish(background=True)`` tasks.

High enough that ordinary bursty publishing never reaches it, low enough that
a producer outrunning its handlers is bounded rather than accumulating tasks
until the process dies. A real default rather than an opt-in knob: a bound
nobody sets is not a bound.
"""


class BaseEventBus(EventBus):
    """Base class providing subscription management and background tasks.

    Subclasses implement ``publish`` and their own transport lifecycle. They
    must call ``super().__init__()``.

    Thread Safety:
        Subscription methods are thread-safe via SubscriptionRegistry.
        Publishing must be called from an async context.
    """

    def __init__(
        self,
        *,
        event_registry: EventRegistry | None = None,
        max_background_tasks: int | None = DEFAULT_MAX_BACKGROUND_TASKS,
    ) -> None:
        """Initialize shared bus state.

        Args:
            event_registry: Registry used to resolve event classes by name
                when consuming. Falls back to the global default registry.
            max_background_tasks: Ceiling on in-flight `publish(background=True)`
                tasks. At the ceiling, publishing runs inline instead of
                spawning another task -- see `_track_background`. Pass None
                for the old unbounded fire-and-forget behavior.
        """
        self._registry = SubscriptionRegistry()
        self._event_registry = event_registry
        self._tasks = BackgroundTaskManager(max_pending=max_background_tasks)

    @property
    def max_background_tasks(self) -> int | None:
        """Ceiling on in-flight background publish tasks, or None if unbounded."""
        return self._tasks.max_pending

    # =========================================================================
    # Subscription management
    # =========================================================================

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

    # =========================================================================
    # Event class resolution
    # =========================================================================

    def _resolve_event_class(self, event_type_name: str) -> type[DomainEvent] | None:
        """Resolve an event class by name, for deserializing consumed messages.

        Args:
            event_type_name: Registered name of the event class.

        Returns:
            The event class, or None if not registered.
        """
        if self._event_registry is not None:
            return self._event_registry.get_or_none(event_type_name)

        from eventsource.domain.event_registry import default_registry

        return default_registry.get_or_none(event_type_name)

    # =========================================================================
    # Background task management
    # =========================================================================

    async def _track_background(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None] | None:
        """Schedule a coroutine as a tracked fire-and-forget task.

        Tracking prevents orphaned coroutines and lets ``_drain_background``
        wait for in-flight work during shutdown.

        **At capacity this awaits `coro` inline and returns None** rather than
        spawning task number `max_background_tasks + 1`. Without a ceiling, a
        producer faster than its handlers grew the in-flight set without
        limit -- the memory hazard ADR 0021 and ADR 0017 record for background
        snapshot scheduling, in a second place -- and `_drain_background` then
        had to wait for or cancel all of it at shutdown.

        Degrading to inline rather than blocking on a slot is deliberate:

        - **It cannot deadlock.** A handler running inside a background
          publish task may itself publish. If that inner call waited for a
          slot, it would wait for one held by the task it is running inside.
          Running inline completes instead of waiting, so this needs no
          re-entrancy guard.
        - **It loses nothing.** Dropping at capacity would silently discard
          events, which is not a tradeoff an event bus gets to make.
        - **It is the backpressure the bound exists for**: a producer
          outrunning its handlers is slowed to the rate they can absorb.

        The cost is that `publish(background=True)` stops returning promptly
        once the bus is saturated. That is the intended signal, not a
        regression -- but it does mean the call is only non-blocking while
        there is headroom.

        Args:
            coro: The coroutine to run.

        Returns:
            The created task, or None if the coroutine ran inline.
        """
        if self._tasks.at_capacity:
            logger.debug(
                "Background publish at capacity (%s); running inline",
                self._tasks.max_pending,
            )
            await coro
            return None
        return self._tasks.submit(coro, on_done=self._on_background_task_done)

    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        """Log any unexpected failure.

        Discarding from tracking is handled by the underlying
        BackgroundTaskManager.
        """
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
        """Wait for background tasks to finish, cancelling any stragglers.

        Args:
            timeout: Seconds to wait before cancelling remaining tasks.
        """
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
