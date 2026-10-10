"""Base class for event-sourced aggregate roots."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import ClassVar
from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.aggregate.provenance import AggregateProvenanceMixin
from eventsource.domain.aggregate.snapshot import AggregateSnapshotMixin
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import (
    AggregateTypeNotSetError,
    EventVersionError,
)

logger = logging.getLogger(__name__)


class AggregateRoot[TState: BaseModel](
    AggregateSnapshotMixin[TState],
    AggregateProvenanceMixin,
    ABC,
):
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
        >>> @register_event
        ... class OrderCreated(DomainEvent):
        ...     aggregate_type: str = "Order"
        ...     customer_id: UUID
        ...
        >>> class OrderState(BaseModel):
        ...     order_id: UUID
        ...     status: str = "pending"
        ...     items: list[OrderItem] = []
        ...
        >>> class OrderAggregate(AggregateRoot[OrderState]):
        ...     aggregate_type = "Order"
        ...
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
        ...         self.create_event(OrderCreated, customer_id=customer_id)

    Attributes:
        aggregate_id: Unique identifier for this aggregate instance
        aggregate_type: String identifier for this aggregate type (subclasses should override)
        schema_version: Version number for the aggregate's state schema. Increment this
                       when the TState model structure changes in a way that makes
                       old snapshots incompatible. Default is 1.
        version: Current version (number of events applied)
        _uncommitted_events: Events that haven't been persisted yet
        _state: Current state of the aggregate
    """

    # Aggregate type identifier -- REQUIRED. Becomes the stream category;
    # construction raises AggregateTypeNotSetError if a concrete subclass
    # does not set it. (Annotated ClassVar, deliberately no default.)
    aggregate_type: ClassVar[str]

    # Class-level schema version for snapshot compatibility
    # Increment when TState structure changes incompatibly
    schema_version: int = 1

    # Class-level configuration for version validation
    # When True, events with incorrect versions will raise EventVersionError
    # When False, version mismatches are logged as warnings but allowed
    validate_versions: bool = True

    def __init__(self, aggregate_id: UUID) -> None:
        """
        Initialize aggregate root.

        Args:
            aggregate_id: Unique identifier for this aggregate
        """
        if not getattr(type(self), "aggregate_type", None):
            raise AggregateTypeNotSetError(type(self).__name__)
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
            AggregateIdMismatchError: If is_new=True and the event names a different
                              aggregate_id -- it would be appended to a stream this
                              aggregate never reads back
            EventVersionError: If version validation is enabled (validate_versions=True),
                              is_new=True, and the event version doesn't match expected
                              (current version + 1)
        """
        # Validate version for new events (not historical replay)
        if is_new:
            self._reject_foreign_aggregate_id(event, None)
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
        """
        pass

    @abstractmethod
    def _get_initial_state(self) -> TState | None:
        """
        Get the initial state for a new aggregate.

        Called by event handlers to set up initial state when needed.

        Returns:
            Initial state instance, or None for deferred state aggregates
        """
        pass

    def mark_events_as_committed(self) -> None:
        """
        Mark all uncommitted events as committed.

        Called by the repository after events have been successfully
        persisted to the event store.
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


__all__ = ["AggregateRoot"]
