"""
Subscriber protocols: the contract event subscribers implement to work
with the subscription manager.

These Protocols support both single-event and batch event processing. They
are designed to be compatible with existing projection patterns in the
codebase while providing additional flexibility for batch processing.

Protocols:
    Subscriber: Main protocol for async event subscribers
    SyncSubscriber: Protocol for synchronous event subscribers
    BatchSubscriber: Protocol for subscribers that prefer batch processing

Base classes with real implementation (``BaseSubscriber``,
``BatchAwareSubscriber``, ``FilteringSubscriber``) live in
``eventsource.application.subscriptions.subscriber`` -- ports carry
interfaces only, never implementation.

Example:
    >>> from eventsource.ports.subscribers import Subscriber
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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent


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


__all__ = [
    "Subscriber",
    "SyncSubscriber",
    "BatchSubscriber",
    "supports_batch_handling",
    "get_subscribed_event_types",
]
