"""
Subscriber protocol for the subscription manager.

This module defines the Subscriber protocol that all event subscribers must
implement to work with the SubscriptionManager. It supports both single-event
and batch event processing.

The protocol is designed to be compatible with existing projection patterns
in the codebase while providing additional flexibility for batch processing.

Protocols:
    Subscriber: Main protocol for async event subscribers
    SyncSubscriber: Protocol for synchronous event subscribers
    BatchSubscriber: Protocol for subscribers that prefer batch processing

Base Classes:
    BaseSubscriber: Abstract base class with common functionality
    BatchAwareSubscriber: Base class with batch support and single-event fallback

Example:
    >>> from eventsource.subscriptions.subscriber import Subscriber
    >>>
    >>> class MyProjection:
    ...     def subscribed_to(self) -> list[type[DomainEvent]]:
    ...         return [OrderCreated, OrderShipped]
    ...
    ...     async def handle(self, event: DomainEvent) -> None:
    ...         if isinstance(event, OrderCreated):
    ...             await self._handle_created(event)
    >>>
    >>> # Check protocol compliance at runtime
    >>> assert isinstance(MyProjection(), Subscriber)
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent

logger = logging.getLogger(__name__)


@runtime_checkable
class Subscriber(Protocol):
    """
    Protocol for async event subscribers.

    This is the primary protocol for subscribers in the subscription manager.
    Subscribers declare which event types they handle and provide an async
    handler method for processing events.

    The protocol is runtime-checkable, allowing isinstance() checks to verify
    if an object implements the required interface.

    Methods:
        subscribed_to: Returns list of event types this subscriber handles
        handle: Async method to process a single event

    Example:
        >>> class OrderProjection:
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         # Process the event
        ...         pass
        >>>
        >>> subscriber = OrderProjection()
        >>> assert isinstance(subscriber, Subscriber)
    """

    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """
        Return list of event types this subscriber handles.

        The subscription manager uses this to filter events before delivery.
        Only events of types returned by this method will be delivered to
        the handle() method.

        Returns:
            List of event type classes this subscriber wants to receive
        """
        ...

    async def handle(self, event: "DomainEvent") -> None:
        """
        Handle a single domain event asynchronously.

        This method is called for each event that matches the types
        returned by subscribed_to().

        Args:
            event: The domain event to process

        Raises:
            Exception: Implementation-specific exceptions may be raised.
                      The subscription manager will handle these according
                      to its error handling configuration.
        """
        ...


@runtime_checkable
class SyncSubscriber(Protocol):
    """
    Protocol for synchronous event subscribers.

    Use this for subscribers that do not require async I/O, such as:
    - In-memory state updates
    - Simple logging or metrics collection
    - Test mocks

    Example:
        >>> class MetricsCollector:
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated]
        ...
        ...     def handle(self, event: DomainEvent) -> None:
        ...         metrics.increment("orders_created")
    """

    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """Return list of event types this subscriber handles."""
        ...

    def handle(self, event: "DomainEvent") -> None:
        """
        Handle a single domain event synchronously.

        Args:
            event: The domain event to process
        """
        ...


@runtime_checkable
class BatchSubscriber(Protocol):
    """
    Protocol for subscribers that support batch event processing.

    Batch processing can be more efficient for high-throughput scenarios
    where the subscriber can optimize bulk operations (e.g., batch database
    inserts or bulk API calls).

    A subscriber can implement both Subscriber and BatchSubscriber protocols
    to support both single-event and batch processing modes.

    Example:
        >>> class BulkOrderProjection:
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         # Single event fallback
        ...         await self._process_order(event)
        ...
        ...     async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
        ...         # Efficient batch processing
        ...         await self._bulk_insert_orders(events)
    """

    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """Return list of event types this subscriber handles."""
        ...

    async def handle_batch(self, events: Sequence["DomainEvent"]) -> None:
        """
        Handle a batch of domain events asynchronously.

        This method is called with a sequence of events when the subscription
        manager is in batch processing mode. The subscriber should process
        all events in the batch efficiently.

        The order of events in the batch is preserved and matches the order
        they were stored in the event store.

        Args:
            events: Sequence of domain events to process. The sequence
                   may contain events of different types from those
                   returned by subscribed_to(). May be empty.

        Raises:
            Exception: Implementation-specific exceptions may be raised.
                      If batch processing fails, the subscription manager
                      may retry individual events using single-event handling.
        """
        ...


def supports_batch_handling(subscriber: object) -> bool:
    """
    Check if a subscriber supports batch event handling.

    This function checks for the presence of a handle_batch method on the
    subscriber. It does not verify the method signature or behavior.

    Args:
        subscriber: The subscriber object to check

    Returns:
        True if the subscriber has a handle_batch method
    """
    return hasattr(subscriber, "handle_batch") and callable(
        getattr(subscriber, "handle_batch", None)
    )


def get_subscribed_event_types(subscriber: Subscriber) -> list[type["DomainEvent"]]:
    """
    Get the event types a subscriber handles.

    Convenience function to extract the subscribed event types from a
    subscriber, with validation.

    Args:
        subscriber: The subscriber to query

    Returns:
        List of event type classes

    Raises:
        ValueError: If subscriber returns invalid types
    """
    event_types = subscriber.subscribed_to()
    if not isinstance(event_types, list):
        raise ValueError(f"subscribed_to() must return a list, got {type(event_types).__name__}")
    return event_types


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


__all__ = [
    # Protocols
    "Subscriber",
    "SyncSubscriber",
    "BatchSubscriber",
    # Utility functions
    "supports_batch_handling",
    "get_subscribed_event_types",
    # Base classes
    "BaseSubscriber",
    "BatchAwareSubscriber",
    "FilteringSubscriber",
]
