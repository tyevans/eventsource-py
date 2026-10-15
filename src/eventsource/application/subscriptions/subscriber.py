"""
Subscriber base classes for the subscription manager.

The Subscriber/SyncSubscriber/BatchSubscriber Protocols and the
supports_batch_handling()/get_subscribed_event_types() helpers live in
``eventsource.ports.subscribers`` -- this module holds the user-subclassable
base classes with real implementation.

Base Classes:
    BaseSubscriber: Abstract base class with common functionality
    BatchAwareSubscriber: Base class with batch support and single-event fallback
    FilteringSubscriber: Base class with event filtering support

Example:
    >>> from eventsource.application.subscriptions.subscriber import BaseSubscriber
    >>>
    >>> class MyProjection(BaseSubscriber):
    ...     def subscribed_to(self) -> list[type[DomainEvent]]:
    ...         return [OrderCreated, OrderShipped]
    ...
    ...     async def handle(self, event: DomainEvent) -> None:
    ...         if isinstance(event, OrderCreated):
    ...             await self._handle_created(event)
"""

import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING

from eventsource.ports.subscribers import (
    BatchSubscriber,
    Subscriber,
    SyncSubscriber,
    get_subscribed_event_types,
    supports_batch_handling,
)

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent

logger = logging.getLogger(__name__)

__all__ = [
    # Re-exported protocols (canonical home: eventsource.ports.subscribers)
    "Subscriber",
    "SyncSubscriber",
    "BatchSubscriber",
    "supports_batch_handling",
    "get_subscribed_event_types",
    # Base classes
    "BaseSubscriber",
    "BatchAwareSubscriber",
    "FilteringSubscriber",
    # Dispatch
    "settle_handler_result",
]


async def settle_handler_result(result: object) -> None:
    """Await a subscriber handler's return value when it is awaitable.

    `SyncSubscriber` is a published Protocol whose `handle()` is declared
    **not** async, and both runners used to `await` the result of every handler
    call unconditionally. A sync handler returns `None`, and `await None`
    raises `TypeError: object NoneType can't be used in 'await' expression`.

    That `TypeError` landed inside the runners' generic `except Exception`
    around handler dispatch, so it was recorded as a handler failure and
    attributed to user code -- while the handler body had in fact already run
    successfully. Every event "failed" forever, and the reported cause pointed
    at the wrong place.

    Awaiting conditionally covers both Protocols with no capability detection:
    an async `handle()` returns a coroutine and is awaited, a sync one returns
    `None` and is not. There is deliberately no `iscoroutinefunction()` check,
    because that inspects the *declaration* and gets a handler that returns an
    awaitable from a non-async def wrong.

    This lives in the application ring rather than reusing
    `adapters/_bus/handler_adapter.py`'s equivalent normalization: `adapters`
    layers **above** `application`, so a runner importing it would break the
    ring-layering contract in `pyproject.toml`. (An earlier note proposed
    exactly that; it does not typecheck against the architecture.) The bus
    adapter's version may later delegate here -- inward is the legal direction.

    Note for sync handlers: a slow one blocks the event loop, so
    `SubscriptionConfig.processing_timeout` cannot interrupt it. `asyncio`
    timeouts only fire at an await point, and a sync handler has none. Use an
    async handler for anything that can block.

    Args:
        result: Whatever the handler call returned.
    """
    if inspect.isawaitable(result):
        await result


class BaseSubscriber(ABC):
    """Abstract base class for event subscribers.

    Subclasses must implement `subscribed_to()` and `handle()`.
    """

    @abstractmethod
    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """Return list of event types this subscriber handles."""
        pass

    @abstractmethod
    async def handle(self, event: "DomainEvent") -> None:
        """Handle a single domain event."""
        pass

    def can_handle(self, event: "DomainEvent") -> bool:
        """Check if this subscriber can handle the given event."""
        return type(event) in self.subscribed_to()

    def __repr__(self) -> str:
        """String representation of the subscriber."""
        event_types = [et.__name__ for et in self.subscribed_to()]
        return f"{self.__class__.__name__}(subscribed_to={event_types})"


class BatchAwareSubscriber(BaseSubscriber):
    """Base class for subscribers with batch processing support.

    Extends BaseSubscriber with `handle_batch()` that defaults to processing
    events individually. Implements both Subscriber and BatchSubscriber protocols.
    """

    async def handle_batch(self, events: Sequence["DomainEvent"]) -> None:
        """Handle a batch of events by processing each individually."""
        for event in events:
            await self.handle(event)

    async def handle_batch_with_error_tracking(
        self,
        events: Sequence["DomainEvent"],
    ) -> tuple[int, list[tuple["DomainEvent", Exception]]]:
        """Process a batch while tracking individual failures without stopping."""
        success_count = 0
        failures: list[tuple[DomainEvent, Exception]] = []

        for event in events:
            try:
                await self.handle(event)
                success_count += 1
            except Exception as e:
                failures.append((event, e))
                logger.warning(
                    "Event processing failed in batch",
                    extra={
                        "subscriber": self.__class__.__name__,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "error": str(e),
                    },
                )

        return success_count, failures


class FilteringSubscriber(BatchAwareSubscriber):
    """Subscriber with built-in event filtering capabilities.

    Extends BatchAwareSubscriber with `should_handle()` filtering before `_process_event()`.
    """

    def should_handle(self, event: "DomainEvent") -> bool:
        """Determine if an event should be processed (defaults to `can_handle()`)."""
        return self.can_handle(event)

    async def handle(self, event: "DomainEvent") -> None:
        """Handle event with filtering via `should_handle()` and `_process_event()`."""
        if self.should_handle(event):
            await self._process_event(event)

    @abstractmethod
    async def _process_event(self, event: "DomainEvent") -> None:
        """Process a single event that passed filtering."""
        pass

    async def handle_batch(self, events: Sequence["DomainEvent"]) -> None:
        """Handle batch with filtering applied."""
        filtered_events = [e for e in events if self.should_handle(e)]
        for event in filtered_events:
            await self._process_event(event)
