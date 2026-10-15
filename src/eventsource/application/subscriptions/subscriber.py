"""Subscriber base classes for the subscription manager."""

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
    "Subscriber",
    "SyncSubscriber",
    "BatchSubscriber",
    "supports_batch_handling",
    "get_subscribed_event_types",
    "BaseSubscriber",
    "BatchAwareSubscriber",
    "FilteringSubscriber",
    "settle_handler_result",
]


async def settle_handler_result(result: object) -> None:
    """Await a subscriber handler's return value when it is awaitable (supports sync and async handlers)."""
    if inspect.isawaitable(result):
        await result


class BaseSubscriber(ABC):
    """Abstract base class for event subscribers (`subscribed_to()` and `handle()`)."""

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
    """Base class for subscribers with batch processing support and single-event fallback."""

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
    """Subscriber with built-in event filtering via `should_handle()` before `_process_event()`."""

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
        for event in (e for e in events if self.should_handle(e)):
            await self._process_event(event)
