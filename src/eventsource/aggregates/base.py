"""
Base classes for event-sourced aggregates.

Aggregates are the consistency boundaries in event sourcing.
They maintain their state by applying events and emit new events
when commands are executed.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Generic, cast, get_args, get_origin
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.exceptions import EventVersionError, UnhandledEventError
from eventsource.types import TState

# Type alias for unregistered event handling mode
UnregisteredEventHandling = str  # "ignore" | "warn" | "error"

logger = logging.getLogger(__name__)

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
        aggregate_type: String identifier for this aggregate type (subclasses should override)
        schema_version: Version number for the aggregate's state schema. Increment this
                       when the TState model structure changes in a way that makes
                       old snapshots incompatible. Default is 1.
        version: Current version (number of events applied)
        _uncommitted_events: Events that haven't been persisted yet
        _state: Current state of the aggregate

    Schema Versioning:
        When using snapshots, the schema_version attribute tracks compatibility
        between the aggregate's state model and stored snapshots. If a snapshot's
        schema_version doesn't match the aggregate's schema_version, the snapshot
        is invalidated and a full event replay is performed.

        Example:
            >>> # Initial version
            >>> class OrderAggregate(AggregateRoot[OrderState]):
            ...     schema_version = 1
            ...
            >>> # After adding a new required field to OrderState
            >>> class OrderAggregate(AggregateRoot[OrderState]):
            ...     schema_version = 2  # Increment to invalidate old snapshots

    Snapshot Support:
        Aggregates support snapshotting for performance optimization. When a
        snapshot exists, the aggregate can be restored to a previous state
        without replaying all historical events.

        Key methods (used internally by AggregateRepository):
        - _serialize_state(): Convert state to dictionary for snapshot storage
        - _restore_from_snapshot(): Restore state from snapshot dictionary
        - _get_state_type(): Get the TState type for deserialization

        To enable schema evolution safety, set the schema_version class attribute:

            >>> class OrderAggregate(AggregateRoot[OrderState]):
            ...     schema_version = 1  # Increment when OrderState changes
    """

    # Class-level aggregate type (subclasses should override)
    aggregate_type: str = "Unknown"

    # Class-level schema version for snapshot compatibility
    # Increment when TState structure changes incompatibly
    schema_version: int = 1

    # Class-level configuration for version validation
    # When True, events with incorrect versions will raise EventVersionError
    # When False, version mismatches are logged as warnings but allowed
    validate_versions: bool = True

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
        1. Validates the event version (for new events with validation enabled)
        2. Updates the version to match the event's aggregate_version
        3. Calls _apply() to update the state
        4. If is_new=True, adds the event to uncommitted events

        Args:
            event: The domain event to apply
            is_new: Whether this is a new event (True) or replayed from history (False)

        Raises:
            EventVersionError: If version validation is enabled (validate_versions=True),
                              is_new=True, and the event version doesn't match expected
                              (current version + 1)

        Note:
            When replaying events from history, pass is_new=False to avoid
            adding them to the uncommitted events list and to skip version validation.

        Example:
            >>> # New event (will be tracked for persistence)
            >>> aggregate.apply_event(order_created, is_new=True)
            >>>
            >>> # Replayed event (from event store)
            >>> aggregate.apply_event(historic_event, is_new=False)
        """
        # Validate version for new events (not historical replay)
        if is_new:
            expected_version = self._version + 1
            if event.aggregate_version != expected_version:
                if self.validate_versions:
                    raise EventVersionError(
                        expected_version=expected_version,
                        actual_version=event.aggregate_version,
                        event_id=event.event_id,
                        aggregate_id=self._aggregate_id,
                    )
                else:
                    # Log warning when validation is disabled but versions don't match
                    logger.warning(
                        "Version mismatch (validation disabled): expected %d, got %d "
                        "for aggregate %s, event %s",
                        expected_version,
                        event.aggregate_version,
                        self._aggregate_id,
                        event.event_id,
                        extra={
                            "aggregate_id": str(self._aggregate_id),
                            "expected_version": expected_version,
                            "actual_version": event.aggregate_version,
                            "event_id": str(event.event_id),
                        },
                    )

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

    def _serialize_state(self) -> dict[str, Any]:
        """
        Serialize the current aggregate state for snapshotting.

        Converts the Pydantic state model to a JSON-compatible dictionary
        using model_dump(mode="json"). This ensures all nested models,
        UUIDs, datetimes, and other complex types are properly serialized.

        Returns:
            Dictionary representation of the state, suitable for JSON storage.
            Returns empty dict if state is None (new aggregate).

        Example:
            >>> order = OrderAggregate(uuid4())
            >>> order.create(customer_id=uuid4())
            >>> state_dict = order._serialize_state()
            >>> # state_dict can be stored as JSON in snapshot
        """
        if self._state is None:
            return {}
        return self._state.model_dump(mode="json")

    def _restore_from_snapshot(
        self,
        state_dict: dict[str, Any],
        version: int,
    ) -> None:
        """
        Restore aggregate state from a snapshot.

        Sets the aggregate's internal state and version from snapshot data.
        After calling this method, the aggregate is in the state it was
        when the snapshot was taken. Subsequent events can then be replayed
        to bring it to the current state.

        Args:
            state_dict: Serialized state dictionary from snapshot.
                       Should be the output of _serialize_state().
            version: Aggregate version when snapshot was taken.
                    Events with version > this will be replayed.

        Raises:
            ValidationError: If state_dict doesn't match TState schema.

        Note:
            This method is called by AggregateRepository before replaying
            events since the snapshot. User code should not call this directly.

        Example:
            >>> # Internal use by repository:
            >>> aggregate = OrderAggregate(aggregate_id)
            >>> aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
            >>> aggregate.load_from_history(events_since_snapshot)
        """
        if not state_dict:
            # Empty state - leave as initial
            self._version = version
            return

        state_type = self._get_state_type()
        self._state = state_type.model_validate(state_dict)
        self._version = version

    def _get_state_type(self) -> type[TState]:
        """
        Get the state type (TState) from the Generic parameter.

        Uses Python's typing introspection to extract the concrete type
        used for TState in the subclass. This is needed for deserializing
        snapshot state back into the correct Pydantic model.

        Returns:
            The concrete type used for TState in this aggregate class.

        Raises:
            RuntimeError: If the state type cannot be determined.

        Example:
            >>> class OrderAggregate(AggregateRoot[OrderState]):
            ...     ...
            >>>
            >>> aggregate = OrderAggregate(uuid4())
            >>> state_type = aggregate._get_state_type()
            >>> assert state_type is OrderState
        """
        # Walk up the MRO to find the AggregateRoot parameterization
        for base in type(self).__mro__:
            if not hasattr(base, "__orig_bases__"):
                continue

            for orig_base in base.__orig_bases__:
                origin = get_origin(orig_base)

                # Check if this is a Generic base that's AggregateRoot or subclass
                if origin is None:
                    continue

                # Handle both AggregateRoot and DeclarativeAggregate
                try:
                    if issubclass(origin, AggregateRoot):
                        args = get_args(orig_base)
                        if args:
                            return cast(type[TState], args[0])
                except TypeError:
                    # issubclass can fail for some typing constructs
                    continue

        raise RuntimeError(
            f"Cannot determine state type for {type(self).__name__}. "
            "Ensure the class properly inherits from AggregateRoot[StateType]."
        )

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

    Attributes:
        unregistered_event_handling: Controls behavior when an event has no
            registered handler. Options:
            - "ignore": Silently ignore unhandled events (default, for backwards
              compatibility and forward compatibility with new event types)
            - "warn": Log a warning for unhandled events
            - "error": Raise UnhandledEventError for unhandled events

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

        >>> # For strict mode (raises error on unhandled events):
        >>> class StrictOrderAggregate(DeclarativeAggregate[OrderState]):
        ...     unregistered_event_handling = "error"
        ...     # ... handlers ...

    Example with schema versioning:
        >>> class OrderAggregate(DeclarativeAggregate[OrderState]):
        ...     aggregate_type = "Order"
        ...     schema_version = 2  # Increment when OrderState changes
        ...
        ...     @handles(OrderCreated)
        ...     def _on_order_created(self, event: OrderCreated) -> None:
        ...         ...
    """

    # Class-level configuration for unregistered event handling
    # Options: "ignore" (default), "warn", "error"
    unregistered_event_handling: UnregisteredEventHandling = "ignore"

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
        Behavior for unhandled events depends on unregistered_event_handling setting.

        Raises:
            UnhandledEventError: If unregistered_event_handling="error" and no handler found
        """
        event_type = type(event)
        handler_name = self._event_handlers.get(event_type)
        if handler_name:
            handler = getattr(self, handler_name)
            handler(event)
        else:
            # No handler found - handle based on configuration
            self._handle_unregistered_event(event)

    def _handle_unregistered_event(self, event: DomainEvent) -> None:
        """
        Handle an event that has no registered handler.

        Behavior depends on the unregistered_event_handling class attribute:
        - "ignore": Do nothing (silent)
        - "warn": Log a warning
        - "error": Raise UnhandledEventError

        Args:
            event: The event that has no handler

        Raises:
            UnhandledEventError: If unregistered_event_handling="error"
        """
        event_type = type(event)
        available_handlers = [et.__name__ for et in self._event_handlers]

        if self.unregistered_event_handling == "error":
            raise UnhandledEventError(
                event_type=event_type.__name__,
                event_id=event.event_id,
                handler_class=self.__class__.__name__,
                available_handlers=available_handlers,
            )
        elif self.unregistered_event_handling == "warn":
            logger.warning(
                "No handler registered for event type %s in %s. Available handlers: %s.",
                event_type.__name__,
                self.__class__.__name__,
                ", ".join(available_handlers) if available_handlers else "none",
                extra={
                    "event_type": event_type.__name__,
                    "event_id": str(event.event_id),
                    "handler_class": self.__class__.__name__,
                    "available_handlers": available_handlers,
                },
            )
        # "ignore" mode: do nothing (silent)


__all__ = [
    "AggregateRoot",
    "DeclarativeAggregate",
    "TState",
    "UnregisteredEventHandling",
]
