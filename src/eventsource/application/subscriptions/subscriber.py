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
    from eventsource.events.base import DomainEvent

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
]


class BaseSubscriber(ABC):
    """
    Abstract base class for event subscribers.

    Provides a foundation for building subscribers with common functionality.
    This is an alternative to using the protocol directly when you want
    to leverage inheritance for shared behavior.

    Subclasses must implement:
    - subscribed_to(): Return list of event types to handle
    - handle(): Process a single event

    Example:
        >>> class OrderProjection(BaseSubscriber):
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self._handle_created(event)
        ...         elif isinstance(event, OrderShipped):
        ...             await self._handle_shipped(event)
    """

    @abstractmethod
    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """
        Return list of event types this subscriber handles.

        Returns:
            List of event type classes
        """
        pass

    @abstractmethod
    async def handle(self, event: "DomainEvent") -> None:
        """
        Handle a single domain event.

        Args:
            event: The domain event to process
        """
        pass

    def can_handle(self, event: "DomainEvent") -> bool:
        """
        Check if this subscriber can handle the given event.

        Default implementation checks if the event type is in subscribed_to().
        Override for custom filtering logic.

        Args:
            event: The event to check

        Returns:
            True if this subscriber handles the event type
        """
        return type(event) in self.subscribed_to()

    def __repr__(self) -> str:
        """String representation of the subscriber."""
        event_types = [et.__name__ for et in self.subscribed_to()]
        return f"{self.__class__.__name__}(subscribed_to={event_types})"


class BatchAwareSubscriber(BaseSubscriber):
    """
    Base class for subscribers with batch processing support.

    Extends BaseSubscriber with handle_batch() that defaults to processing
    events individually. Subclasses can override handle_batch() to provide
    optimized bulk processing while maintaining single-event compatibility.

    This class implements both the Subscriber and BatchSubscriber protocols.

    Example:
        >>> class AnalyticsProjection(BatchAwareSubscriber):
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         # Called for individual events or as fallback
        ...         await self._record_metric(event)
        ...
        ...     async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
        ...         # Override for optimized batch processing
        ...         await self._bulk_record_metrics(events)
    """

    async def handle_batch(self, events: Sequence["DomainEvent"]) -> None:
        """
        Handle a batch of events by processing each individually.

        Default implementation calls handle() for each event in sequence.
        Override this method to implement optimized batch processing.

        Args:
            events: Sequence of events to process
        """
        for event in events:
            await self.handle(event)

    async def handle_batch_with_error_tracking(
        self,
        events: Sequence["DomainEvent"],
    ) -> tuple[int, list[tuple["DomainEvent", Exception]]]:
        """
        Process a batch while tracking individual failures.

        Processes all events in the batch, collecting any failures without
        stopping processing. Useful when you want to continue processing
        remaining events even if some fail.

        Args:
            events: Sequence of events to process

        Returns:
            Tuple of (success_count, failures) where failures is a list
            of (event, exception) pairs for events that failed processing
        """
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
    """
    Subscriber with built-in event filtering capabilities.

    Extends BatchAwareSubscriber with methods to filter events based on
    various criteria before processing. Useful for projections that need
    to filter events beyond just type matching.

    Override the filter methods to customize filtering behavior.

    Example:
        >>> class TenantOrderProjection(FilteringSubscriber):
        ...     def __init__(self, tenant_id: UUID):
        ...         self.tenant_id = tenant_id
        ...
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated]
        ...
        ...     def should_handle(self, event: DomainEvent) -> bool:
        ...         # Only handle events for our tenant
        ...         return event.tenant_id == self.tenant_id
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         await self._process_order(event)
    """

    def should_handle(self, event: "DomainEvent") -> bool:
        """
        Determine if an event should be processed.

        Override this method to implement custom filtering logic.
        Default implementation returns True for all events whose type
        is in subscribed_to().

        Args:
            event: The event to evaluate

        Returns:
            True if the event should be processed
        """
        return self.can_handle(event)

    async def handle(self, event: "DomainEvent") -> None:
        """
        Handle event with filtering.

        Checks should_handle() before delegating to _process_event().
        Subclasses should implement _process_event() instead of handle().

        Args:
            event: The event to potentially process
        """
        if self.should_handle(event):
            await self._process_event(event)

    @abstractmethod
    async def _process_event(self, event: "DomainEvent") -> None:
        """
        Process a single event that passed filtering.

        Implement this method in subclasses to define event processing logic.

        Args:
            event: The event to process (already filtered)
        """
        pass

    async def handle_batch(self, events: Sequence["DomainEvent"]) -> None:
        """
        Handle batch with filtering applied.

        Filters events using should_handle() before processing.

        Args:
            events: Sequence of events to potentially process
        """
        filtered_events = [e for e in events if self.should_handle(e)]
        for event in filtered_events:
            await self._process_event(event)
