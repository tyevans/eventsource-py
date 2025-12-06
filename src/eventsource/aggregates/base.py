"""
Base classes for event-sourced aggregates.

Aggregates are the consistency boundaries in event sourcing.
They maintain their state by applying events and emit new events
when commands are executed.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Generic
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.types import TState

# Type alias for event handler functions
EventHandler = Callable[[DomainEvent], None]


class AggregateRoot(Generic[TState], ABC):
    """
    Base class for event-sourced aggregate roots.

    Aggregates are the primary building blocks in event sourcing. They:
    - Maintain their state by applying events
    - Track uncommitted events that need to be persisted
    - Ensure business rule invariants are maintained
    - Serve as consistency boundaries

    The aggregate uses a generic type parameter `TState` to define the
    shape of its internal state. This state must be a Pydantic BaseModel
    to enable validation and serialization.

    Subclasses must implement:
    - `_apply(event)`: Update state based on event type
    - `_get_initial_state()`: Return initial state for new aggregates

    Example:
        >>> class OrderState(BaseModel):
        ...     order_id: UUID
        ...     status: str = "pending"
        ...     items: list[OrderItem] = []
        ...
        >>> class OrderAggregate(AggregateRoot[OrderState]):
        ...     def _get_initial_state(self) -> OrderState:
        ...         return OrderState(order_id=self.aggregate_id)
        ...
        ...     def _apply(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             self._state = OrderState(
        ...                 order_id=event.order_id,
        ...                 status="created",
        ...             )
        ...         elif isinstance(event, ItemAdded):
        ...             self._state = self._state.model_copy(
        ...                 update={"items": [*self._state.items, event.item]}
        ...             )
        ...
        ...     def create(self, customer_id: UUID) -> None:
        ...         if self.version > 0:
        ...             raise ValueError("Order already created")
        ...         event = OrderCreated(
        ...             aggregate_id=self.aggregate_id,
        ...             aggregate_type="Order",
        ...             event_type="OrderCreated",
        ...             customer_id=customer_id,
        ...             aggregate_version=self.version + 1,
        ...         )
        ...         self.apply_event(event)

    Attributes:
        aggregate_id: Unique identifier for this aggregate instance
        version: Current version (number of events applied)
        _uncommitted_events: Events that haven't been persisted yet
        _state: Current state of the aggregate
    """

    # Class-level aggregate type (subclasses should override)
    aggregate_type: str = "Unknown"

    # Class-level registry of event handlers (populated by decorator)
    _event_handlers: dict[type[DomainEvent], str] = {}

    def __init__(self, aggregate_id: UUID) -> None:
        """
        Initialize aggregate root.

        Args:
            aggregate_id: Unique identifier for this aggregate
        """
        self._aggregate_id = aggregate_id
        self._version = 0
        self._uncommitted_events: list[DomainEvent] = []
        self._state: TState | None = None

    @property
    def aggregate_id(self) -> UUID:
        """Get the unique identifier for this aggregate."""
        return self._aggregate_id

    @property
    def version(self) -> int:
        """Get the current version (number of events applied)."""
        return self._version

    @property
    def state(self) -> TState | None:
        """
        Get the current state of the aggregate.

        Returns None for new aggregates that haven't had any events applied.
        """
        return self._state

    @property
    def uncommitted_events(self) -> list[DomainEvent]:
        """
        Get events that haven't been persisted yet.

        Returns a copy to prevent external modification.
        """
        return self._uncommitted_events.copy()

    @property
    def has_uncommitted_events(self) -> bool:
        """Check if there are events waiting to be persisted."""
        return len(self._uncommitted_events) > 0

    def apply_event(self, event: DomainEvent, is_new: bool = True) -> None:
        """
        Apply an event to the aggregate.

        This method:
        1. Updates the version to match the event's aggregate_version
        2. Calls _apply() to update the state
        3. If is_new=True, adds the event to uncommitted events

        Args:
            event: The domain event to apply
            is_new: Whether this is a new event (True) or replayed from history (False)

        Note:
            When replaying events from history, pass is_new=False to avoid
            adding them to the uncommitted events list.

        Example:
            >>> # New event (will be tracked for persistence)
            >>> aggregate.apply_event(order_created, is_new=True)
            >>>
            >>> # Replayed event (from event store)
            >>> aggregate.apply_event(historic_event, is_new=False)
        """
        # Update version
        self._version = event.aggregate_version

        # Apply the event to update state
        self._apply(event)

        # If this is a new event, track it for persistence
        if is_new:
            self._uncommitted_events.append(event)

    @abstractmethod
    def _apply(self, event: DomainEvent) -> None:
        """
        Apply event to update aggregate state.

        Subclasses must implement this to handle specific event types
        and update the internal state accordingly.

        Args:
            event: The domain event to apply

        Example:
            >>> def _apply(self, event: DomainEvent) -> None:
            ...     if isinstance(event, OrderCreated):
            ...         self._state = OrderState(...)
            ...     elif isinstance(event, OrderShipped):
            ...         self._state = self._state.model_copy(
            ...             update={"status": "shipped"}
            ...         )
        """
        pass

    @abstractmethod
    def _get_initial_state(self) -> TState:
        """
        Get the initial state for a new aggregate.

        Called when the first event is applied to set up the initial state.

        Returns:
            Initial state instance

        Example:
            >>> def _get_initial_state(self) -> OrderState:
            ...     return OrderState(order_id=self.aggregate_id)
        """
        pass

    def mark_events_as_committed(self) -> None:
        """
        Mark all uncommitted events as committed.

        Called by the repository after events have been successfully
        persisted to the event store.

        Example:
            >>> # After persisting events
            >>> await event_store.append_events(...)
            >>> aggregate.mark_events_as_committed()
        """
        self._uncommitted_events.clear()

    def load_from_history(self, events: list[DomainEvent]) -> None:
        """
        Reconstitute aggregate state from event history.

        Replays all events in order to rebuild the aggregate's state.
        Events are applied with is_new=False so they aren't added to
        uncommitted events.

        Args:
            events: List of historical events in chronological order

        Example:
            >>> stream = await event_store.get_events(aggregate_id, "Order")
            >>> aggregate = OrderAggregate(aggregate_id)
            >>> aggregate.load_from_history(stream.events)
            >>> print(f"Order state: {aggregate.state}")
        """
        for event in events:
            self.apply_event(event, is_new=False)

    def get_next_version(self) -> int:
        """
        Get the version number for the next event.

        Useful when creating new events that need the correct
        aggregate_version field.

        Returns:
            Current version + 1
        """
        return self._version + 1

    def clear_uncommitted_events(self) -> list[DomainEvent]:
        """
        Clear and return all uncommitted events.

        This is an alternative to mark_events_as_committed() that also
        returns the events, useful for repositories that need to process
        the events before clearing them.

        Returns:
            List of uncommitted events that were cleared
        """
        events = self._uncommitted_events.copy()
        self._uncommitted_events.clear()
        return events

    def _raise_event(self, event: DomainEvent) -> None:
        """
        Convenience method to create and apply a new event.

        This is an alias for apply_event(event, is_new=True) that makes
        the intent clearer when raising domain events from command methods.

        Args:
            event: The domain event to raise and apply

        Example:
            >>> def create_order(self, customer_id: UUID) -> None:
            ...     event = OrderCreated(
            ...         aggregate_id=self.aggregate_id,
            ...         aggregate_type=self.aggregate_type,
            ...         aggregate_version=self.get_next_version(),
            ...         customer_id=customer_id,
            ...     )
            ...     self._raise_event(event)
        """
        self.apply_event(event, is_new=True)

    def __repr__(self) -> str:
        """String representation of aggregate."""
        return (
            f"{self.__class__.__name__}("
            f"id={self._aggregate_id}, "
            f"version={self._version}, "
            f"uncommitted={len(self._uncommitted_events)})"
        )

    def __eq__(self, other: object) -> bool:
        """Check equality based on aggregate ID."""
        if not isinstance(other, AggregateRoot):
            return NotImplemented
        return self._aggregate_id == other._aggregate_id

    def __hash__(self) -> int:
        """Hash based on aggregate ID."""
        return hash(self._aggregate_id)


class DeclarativeAggregate(AggregateRoot[TState], ABC):
    """
    Aggregate that uses decorators to register event handlers.

    This class provides an alternative to the basic AggregateRoot that
    uses a declarative pattern with the @handles decorator to register
    event handlers, reducing boilerplate in the _apply method.

    Example:
        >>> class OrderAggregate(DeclarativeAggregate[OrderState]):
        ...     aggregate_type = "Order"
        ...
        ...     def _get_initial_state(self) -> OrderState:
        ...         return OrderState(order_id=self.aggregate_id)
        ...
        ...     @handles(OrderCreated)
        ...     def _on_order_created(self, event: OrderCreated) -> None:
        ...         self._state = OrderState(
        ...             order_id=self.aggregate_id,
        ...             customer_id=event.customer_id,
        ...             status="created",
        ...         )
        ...
        ...     @handles(OrderShipped)
        ...     def _on_order_shipped(self, event: OrderShipped) -> None:
        ...         if self._state:
        ...             self._state = self._state.model_copy(
        ...                 update={"status": "shipped"}
        ...             )
    """

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Initialize handler registry for each subclass."""
        super().__init_subclass__(**kwargs)
        # Each subclass gets its own handler registry
        cls._event_handlers = {}
        # Collect handlers from methods with _handles_event_type attribute
        for name in dir(cls):
            try:
                method = getattr(cls, name)
                if hasattr(method, "_handles_event_type"):
                    event_type = method._handles_event_type
                    cls._event_handlers[event_type] = name
            except AttributeError:
                # Some attributes might raise, skip them
                continue

    def _apply(self, event: DomainEvent) -> None:
        """
        Apply event using registered handlers.

        Looks up the handler for the event type and calls it.
        If no handler is found, raises a warning but doesn't fail.
        """
        event_type = type(event)
        handler_name = self._event_handlers.get(event_type)
        if handler_name:
            handler = getattr(self, handler_name)
            handler(event)
        # If no handler found, event is silently ignored
        # This allows for forward compatibility when new events are added


def handles(event_type: type[DomainEvent]) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """
    Decorator to register an event handler method.

    Use this decorator on methods in a DeclarativeAggregate subclass
    to automatically register them as handlers for specific event types.

    Args:
        event_type: The DomainEvent subclass this handler processes

    Returns:
        Decorator function that marks the method as a handler

    Example:
        >>> class OrderAggregate(DeclarativeAggregate[OrderState]):
        ...     @handles(OrderCreated)
        ...     def _on_order_created(self, event: OrderCreated) -> None:
        ...         self._state = OrderState(...)
    """
    def decorator(func: Callable[..., None]) -> Callable[..., None]:
        func._handles_event_type = event_type  # type: ignore[attr-defined]
        return func
    return decorator


__all__ = [
    "AggregateRoot",
    "DeclarativeAggregate",
    "handles",
    "TState",
]
