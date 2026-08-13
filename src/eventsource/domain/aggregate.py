"""
Base classes for event-sourced aggregates.

Aggregates are the consistency boundaries in event sourcing.
They maintain their state by applying events and emit new events
when commands are executed.
"""

import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Collection
from typing import Any, ClassVar, TypeVar, cast, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.command import DomainCommand
from eventsource.domain.decorators import discover_handlers
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import (
    AggregateIdMismatchError,
    AggregateNotCreatedError,
    AggregateTypeMismatchError,
    AggregateTypeNotSetError,
    EventVersionError,
    HandlerSignatureError,
    UnhandledEventError,
)
from eventsource.domain.tenant_context import get_current_tenant

# Type alias for unregistered event handling mode
UnregisteredEventHandling = str  # "ignore" | "warn" | "error"

logger = logging.getLogger(__name__)

# Type alias for event handler functions
EventHandler = Callable[[DomainEvent], None]

# Type variable for event types (used by create_event)
TEvent = TypeVar("TEvent", bound=DomainEvent)


class AggregateRoot[TState: BaseModel](ABC):
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
    def _get_initial_state(self) -> TState | None:
        """
        Get the initial state for a new aggregate.

        Called by event handlers to set up initial state when needed.

        Returns:
            Initial state instance, or None for deferred state aggregates

        Example:
            >>> def _get_initial_state(self) -> OrderState:
            ...     return OrderState(order_id=self.aggregate_id)

        Note:
            For DeclarativeAggregate subclasses with requires_creation_event=True,
            this method returns None since state is set by the first event handler.
        """
        pass

    def mark_events_as_committed(self) -> None:
        """
        Mark all uncommitted events as committed.

        Called by the repository after events have been successfully
        persisted to the event store.

        Example:
            >>> # After persisting events
            >>> await event_store.append(...)
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

    def create_event(
        self,
        event_class: type[TEvent],
        *,
        command: object | None = None,
        **kwargs: Any,
    ) -> TEvent:
        """
        Create and apply an event with auto-populated aggregate fields.

        This is a convenience method that eliminates repetitive boilerplate
        when creating events in command methods. It automatically sets:

        - aggregate_id from self.aggregate_id
        - aggregate_type from self.aggregate_type
        - aggregate_version from self.get_next_version()
        - tenant_id from tenant_context (if available and not explicitly set)
        - causation_id, correlation_id, actor_id, tenant_id from command (if provided)

        The event is automatically applied to the aggregate after creation.

        Args:
            event_class: The event class to instantiate
            command: Optional DomainCommand to extract provenance from
            **kwargs: Event-specific fields (can override auto-populated fields)

        Returns:
            The created and applied event

        Example:
            Before (manual approach):
                >>> def ship(self, tracking_number: str) -> None:
                ...     if self.state.status != "paid":
                ...         raise ValueError("Cannot ship unpaid order")
                ...     event = OrderShipped(
                ...         aggregate_id=self.aggregate_id,
                ...         aggregate_type=self.aggregate_type,
                ...         aggregate_version=self.get_next_version(),
                ...         tracking_number=tracking_number,
                ...     )
                ...     self.apply_event(event)

            After (with create_event):
                >>> def ship(self, tracking_number: str, cmd: ShipOrder) -> None:
                ...     if self.state.status != "paid":
                ...         raise ValueError("Cannot ship unpaid order")
                ...     self.create_event(OrderShipped, command=cmd, tracking_number=tracking_number)

        Note:
            Explicit kwargs always override auto-populated values.
            Overriding auto-stamped fields (e.g. `aggregate_version`) is an
            escape hatch for tests and migrations — in normal domain code,
            let the aggregate stamp them. Precedence: explicit kwargs > command > tenant context > auto fields.
        """
        self._reject_divergent_aggregate_type(event_class)

        # Start with auto-populated aggregate fields
        event_kwargs: dict[str, Any] = {
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_version": self.get_next_version(),
        }
        event_kwargs.update(self._provenance_updates(command, kwargs.keys()))
        # User kwargs override auto-populated values
        event_kwargs.update(kwargs)

        # Create and apply the event
        event = event_class(**event_kwargs)
        self._reject_foreign_aggregate_id(event, command)
        self.apply_event(event, is_new=True)

        return event

    def _reject_foreign_aggregate_id(self, event: DomainEvent, command: object) -> None:
        """Raise if the event names an aggregate other than this one.

        Unlike `aggregate_type`, `aggregate_id` is not restamped -- an
        explicitly-supplied id survives to the store, where it decides the
        stream the event lands in. An event emitted here that names another
        aggregate is appended to a stream that disowns it, and no save/load
        round-trip can see it: the emitting aggregate never reads that
        stream, and the named one never receives the event.

        Reading `event.aggregate_id` rather than a per-aggregate declaration
        of what may be targeted is what makes this work for every aggregate
        without opt-in.
        """
        if event.aggregate_id == self.aggregate_id:
            return
        raise AggregateIdMismatchError(
            type(event).__name__,
            event.aggregate_id,
            type(self).__name__,
            self.aggregate_id,
            type(command).__name__ if command is not None else None,
        )

    def _reject_divergent_aggregate_type(self, event_class: type[DomainEvent]) -> None:
        """Raise if the event class declares a different aggregate_type.

        The aggregate is the single source for `aggregate_type` (ADR 0046),
        so this value is about to be overwritten. Overwriting it silently
        turns a wrong declaration into a wrong stream category that no
        round-trip test can see; the declaration is either redundant or a
        bug, and both deserve to be said out loud.
        """
        field = event_class.model_fields.get("aggregate_type")
        declared = getattr(field, "default", None) if field is not None else None
        if isinstance(declared, str) and declared and declared != self.aggregate_type:
            raise AggregateTypeMismatchError(
                event_class.__name__,
                declared,
                type(self).__name__,
                self.aggregate_type,
            )

    def _provenance_updates(
        self,
        command: object,
        explicitly_set: Collection[str],
    ) -> dict[str, Any]:
        """
        Shared stamping semantics for create_event() and DeciderAggregate._stamp().

        Fields listed in explicitly_set are never overwritten. Tenant
        precedence: explicit > DomainCommand.tenant_id > ambient tenant
        context (unconditional fallback regardless of command type).
        Causation/correlation/actor come only from a DomainCommand.
        """
        updates: dict[str, Any] = {}
        if isinstance(command, DomainCommand):
            if "causation_id" not in explicitly_set:
                updates["causation_id"] = command.command_id
            if "correlation_id" not in explicitly_set:
                updates["correlation_id"] = command.correlation_id
            if "actor_id" not in explicitly_set and command.actor_id is not None:
                updates["actor_id"] = command.actor_id
        if "tenant_id" not in explicitly_set:
            tenant: UUID | None = None
            if isinstance(command, DomainCommand) and command.tenant_id is not None:
                tenant = command.tenant_id
            if tenant is None:
                tenant = self._get_tenant_from_context()
            if tenant is not None:
                updates["tenant_id"] = tenant
        return updates

    def _get_tenant_from_context(self) -> UUID | None:
        """
        Get tenant ID from the current tenant context, if any is set.

        Returns:
            Tenant ID from context, or None if no tenant context is set
        """
        return get_current_tenant()

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


class DeclarativeAggregate[TState: BaseModel](AggregateRoot[TState], ABC):
    """
    Aggregate that uses decorators to register event handlers.

    This class provides an alternative to the basic AggregateRoot that
    uses a declarative pattern with the @handles decorator to register
    event handlers, reducing boilerplate in the _apply method.

    Supports deferred state via `requires_creation_event` class attribute.
    When True, the aggregate doesn't require an initial state implementation
    and will raise AggregateNotCreatedError if state is accessed before
    a creation event is applied.

    Attributes:
        requires_creation_event: When True, the aggregate doesn't require
            _get_initial_state() implementation and state access raises
            AggregateNotCreatedError until a creation event is applied.
            Default is False (backward compatible).
        unregistered_event_handling: Controls behavior when an event has no
            registered handler. Options:
            - "error": Raise UnhandledEventError for unhandled events (default).
              An aggregate is the write model — a silently unapplied event means
              command handlers reason over divergent state.
            - "warn": Log a warning for unhandled events
            - "ignore": Silently ignore unhandled events (explicit opt-down, e.g.
              for forward-compat replay of event types added after this
              aggregate's handlers were written)

    Example with deferred state:
        >>> class ExtractionProcess(DeclarativeAggregate[ExtractionState]):
        ...     aggregate_type = "Extraction"
        ...     requires_creation_event = True  # No initial state needed
        ...
        ...     def request(self, page_id: UUID, config: dict) -> None:
        ...         # First event creates the aggregate
        ...         self.create_event(ExtractionRequested, page_id=page_id, config=config)
        ...
        ...     @handles(ExtractionRequested)
        ...     def _on_requested(self, event: ExtractionRequested) -> None:
        ...         self._state = ExtractionState(page_id=event.page_id, status="requested")

    Example with traditional initial state:
        >>> class OrderAggregate(DeclarativeAggregate[OrderState]):
        ...     aggregate_type = "Order"
        ...     # requires_creation_event defaults to False
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

        >>> # Strictness is the default; opt down explicitly for forward-compat
        >>> # replay of event types added after these handlers were written:
        >>> class LenientOrderAggregate(DeclarativeAggregate[OrderState]):
        ...     unregistered_event_handling = "ignore"
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

    # Class-level attribute for deferred state support
    # When True, aggregate doesn't require _get_initial_state() implementation
    requires_creation_event: ClassVar[bool] = False

    # Class-level configuration for unregistered event handling
    # Options: "error" (default), "warn", "ignore"
    unregistered_event_handling: ClassVar[UnregisteredEventHandling] = "error"

    # Per-subclass handler registry, rebuilt by __init_subclass__.
    _event_handlers: ClassVar[dict[type[DomainEvent], str]] = {}

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Discover and validate @handles methods for each subclass."""
        super().__init_subclass__(**kwargs)
        cls._event_handlers = discover_handlers(cls)
        for event_type, name in cls._event_handlers.items():
            method = getattr(cls, name)
            if inspect.iscoroutinefunction(method):
                try:
                    async_params = list(inspect.signature(method).parameters.values())
                    async_param_count = len(async_params) - 1  # exclude self (unbound function)
                except (ValueError, TypeError):
                    async_param_count = 1
                raise HandlerSignatureError(
                    handler_name=name,
                    owner_name=cls.__name__,
                    event_type=event_type,
                    param_count=async_param_count,
                    is_async_required=False,
                    reason=(
                        "aggregate event handlers run synchronously during replay; remove 'async'"
                    ),
                )
            try:
                params = list(inspect.signature(method).parameters.values())
            except (ValueError, TypeError):
                continue
            param_count = len(params) - 1  # exclude self (unbound function)
            if param_count != 1:
                raise HandlerSignatureError(
                    handler_name=name,
                    owner_name=cls.__name__,
                    event_type=event_type,
                    param_count=param_count,
                    is_async_required=False,
                )

    @property
    def state(self) -> TState:
        """
        Get the current state of the aggregate.

        Returns:
            The current aggregate state

        Raises:
            AggregateNotCreatedError: If requires_creation_event=True and
                no events have been applied yet
        """
        if self.requires_creation_event and self._state is None:
            raise AggregateNotCreatedError(
                self.__class__.__name__,
                suggestion=f"Call a creation method on {self.__class__.__name__} first.",
            )
        return cast(TState, self._state)

    @property
    def state_or_none(self) -> TState | None:
        """
        Get the current state without raising on uncreated aggregate.

        This is useful for checking if an aggregate exists or for
        conditional logic based on creation status.

        Returns:
            The current state, or None if aggregate hasn't been created

        Example:
            >>> if order.state_or_none is None:
            ...     order.create(customer_id)
            ... else:
            ...     order.update(...)
        """
        return self._state

    @property
    def is_created(self) -> bool:
        """
        Check if the aggregate has been created (has state).

        Returns:
            True if at least one event has been applied, False otherwise

        Example:
            >>> order = OrderAggregate(order_id)
            >>> assert not order.is_created
            >>> order.create(customer_id)
            >>> assert order.is_created
        """
        return self._state is not None

    def _get_initial_state(self) -> TState | None:
        """
        Get initial state for new aggregate.

        Behavior depends on `requires_creation_event`:

        - False (default): Subclasses must implement this method
        - True: Returns None, state is set by first event handler

        Returns:
            Initial state, or None for deferred state aggregates

        Raises:
            NotImplementedError: If requires_creation_event=False and
                not implemented in subclass
        """
        if self.requires_creation_event:
            return None

        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_initial_state() "
            f"or set requires_creation_event = True"
        )

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
    "UnregisteredEventHandling",
]
