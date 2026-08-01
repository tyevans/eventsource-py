"""Library exceptions for the eventsource package."""

from uuid import UUID


class EventSourceError(Exception):
    """Base exception for eventsource library."""

    pass


class OptimisticLockError(EventSourceError):
    """Raised when there's a version conflict during event append."""

    def __init__(self, aggregate_id: UUID, expected_version: int, actual_version: int) -> None:
        self.aggregate_id = aggregate_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Optimistic lock error for aggregate {aggregate_id}: "
            f"expected version {expected_version}, but current version is {actual_version}"
        )


class EventNotFoundError(EventSourceError):
    """Raised when an event cannot be found."""

    def __init__(self, event_id: UUID) -> None:
        self.event_id = event_id
        super().__init__(f"Event not found: {event_id}")


class ProjectionError(EventSourceError):
    """Raised when a projection fails to process an event."""

    def __init__(self, projection_name: str, event_id: UUID, message: str) -> None:
        self.projection_name = projection_name
        self.event_id = event_id
        super().__init__(f"Projection {projection_name} failed on event {event_id}: {message}")


class AggregateNotFoundError(EventSourceError):
    """Raised when an aggregate cannot be found."""

    def __init__(self, aggregate_id: UUID, aggregate_type: str | None = None) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        type_info = f" of type {aggregate_type}" if aggregate_type else ""
        super().__init__(f"Aggregate{type_info} not found: {aggregate_id}")


class CommandRejectedError(EventSourceError):
    """
    A command was rejected by domain logic.

    Raising this from ``decide()`` (or a command method) is a convention,
    not a requirement — any exception may be used. It gives application
    code one catchable type meaning "the domain said no" as distinct from
    a bug.

    Attributes:
        command: The rejected command object, when provided.
    """

    def __init__(self, message: str, command: object | None = None) -> None:
        self.command = command
        super().__init__(message)


class EventStoreError(EventSourceError):
    """Raised when there's an error in the event store."""

    pass


class EventBusError(EventSourceError):
    """Raised when there's an error in the event bus."""

    pass


class CheckpointError(EventSourceError):
    """Raised when there's an error with checkpoint operations."""

    pass


class SerializationError(EventSourceError):
    """Raised when event serialization or deserialization fails."""

    def __init__(self, event_type: str, message: str) -> None:
        self.event_type = event_type
        super().__init__(f"Serialization error for {event_type}: {message}")


class EventVersionError(EventSourceError):
    """
    Raised when event version validation fails during aggregate event application.

    This error occurs when:
    - An event has a version gap (e.g., jumping from version 2 to version 5)
    - An event has a version regression (e.g., going from version 5 to version 3)
    - An event has an unexpected version number

    This validation helps ensure aggregate state integrity by detecting
    out-of-order or incorrectly versioned events.

    Attributes:
        expected_version: The version that was expected (current version + 1)
        actual_version: The version found in the event
        event_id: ID of the event with invalid version
        aggregate_id: ID of the aggregate being updated
    """

    def __init__(
        self,
        expected_version: int,
        actual_version: int,
        event_id: UUID,
        aggregate_id: UUID,
    ) -> None:
        self.expected_version = expected_version
        self.actual_version = actual_version
        self.event_id = event_id
        self.aggregate_id = aggregate_id
        super().__init__(
            f"Event version mismatch for aggregate {aggregate_id}: "
            f"expected version {expected_version}, got {actual_version} "
            f"(event_id: {event_id})"
        )


class UnhandledEventError(EventSourceError):
    """
    Raised when an event has no registered handler and strict mode is enabled.

    This error occurs in DeclarativeAggregate or DeclarativeProjection when:
    - An event type is applied/processed that has no @handles decorator
    - The class has unregistered_event_handling set to "error"

    This helps catch bugs such as:
    - Typos in handler names
    - Missing @handles decorators
    - State inconsistencies from silently ignored events

    Attributes:
        event_type: The name of the event type that wasn't handled
        event_id: ID of the unhandled event
        handler_class: Name of the aggregate/projection class
        available_handlers: List of event type names that have handlers
    """

    def __init__(
        self,
        event_type: str,
        event_id: UUID,
        handler_class: str,
        available_handlers: list[str],
    ) -> None:
        self.event_type = event_type
        self.event_id = event_id
        self.handler_class = handler_class
        self.available_handlers = available_handlers
        handlers_str = ", ".join(available_handlers) if available_handlers else "none"
        super().__init__(
            f"No handler registered for event type '{event_type}' "
            f"in {handler_class}. "
            f"Available handlers: {handlers_str}. "
            f"Add @handles({event_type}) decorator or set "
            f"unregistered_event_handling='ignore' or 'warn'."
        )


class AggregateNotCreatedError(EventSourceError):
    """
    Raised when accessing state of an aggregate before creation event.

    This error occurs when:
    1. Aggregate has `requires_creation_event = True`
    2. No events have been applied yet
    3. Code attempts to access `aggregate.state`

    Use `aggregate.state_or_none` or `aggregate.is_created` to safely
    check if the aggregate has been created.

    Attributes:
        aggregate_class: Name of the aggregate class that wasn't created
        suggestion: Optional hint for how to resolve the error
    """

    def __init__(self, aggregate_class: str, suggestion: str | None = None) -> None:
        self.aggregate_class = aggregate_class
        self.suggestion = suggestion

        message = f"{aggregate_class} has not been created. Apply a creation event first."
        if suggestion:
            message += f" Hint: {suggestion}"

        super().__init__(message)


class HandlerDispatchError(EventSourceError):
    """
    Raised after a delivery attempt when one or more handlers failed.

    Buses that dispatch a single delivery to multiple handlers must invoke
    every handler for that delivery -- one handler's failure must not skip
    the rest (error isolation). Once all handlers have run, if any failed,
    the bus raises this aggregate error so the caller's no-ack / redelivery
    path is unchanged: the individual failures are isolated from each other,
    but the delivery as a whole is still treated as failed and eligible for
    retry (and eventually the dead letter queue), exactly as if a single
    handler had raised.

    Attributes:
        failures: List of (handler_name, exception) pairs, one per handler
            that raised, in the order handlers were invoked.
    """

    def __init__(self, failures: list[tuple[str, Exception]]) -> None:
        self.failures = failures
        handler_names = ", ".join(name for name, _ in failures)
        super().__init__(f"{len(failures)} handler(s) failed during dispatch: {handler_names}")


class DuplicateEventError(EventSourceError):
    """An event with this event_id already exists in the store."""


class PositionDecodeError(EventSourceError):
    """A persisted position string could not be decoded."""


class PositionForeignError(EventSourceError):
    """Positions from different stores were compared for order."""


# NOTE: intentionally `Exception`, not `EventSourceError` -- this is the
# pre-move contract preserved verbatim from the old eventsource.snapshots
# package. `except EventSourceError` deliberately does not catch snapshot
# errors; rebasing this hierarchy is a breaking change reserved for a
# future major version.
class SnapshotError(Exception):
    """
    Base exception for snapshot-related errors.

    All snapshot exceptions inherit from this class, allowing callers
    to catch all snapshot errors with a single except clause.

    Example:
        >>> try:
        ...     snapshot = await load_snapshot(aggregate_id)
        ... except SnapshotError:
        ...     # Fall back to full event replay
        ...     pass
    """

    pass


class SnapshotDeserializationError(SnapshotError):
    """
    Raised when a snapshot cannot be deserialized.

    This typically occurs when:
    - The state JSON is malformed
    - The state doesn't match the expected Pydantic model
    - Required fields are missing from the state

    The system handles this gracefully by falling back to full event
    replay, which will recreate the snapshot with valid data.

    Attributes:
        aggregate_id: ID of the aggregate whose snapshot failed
        aggregate_type: Type of the aggregate
        original_error: The underlying deserialization error

    Example:
        >>> try:
        ...     state = state_type.model_validate(snapshot.state)
        ... except ValidationError as e:
        ...     raise SnapshotDeserializationError(
        ...         aggregate_id=snapshot.aggregate_id,
        ...         aggregate_type=snapshot.aggregate_type,
        ...         original_error=e,
        ...     )
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        original_error: Exception | None = None,
        message: str | None = None,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.original_error = original_error

        if message:
            self.message = message
        else:
            self.message = f"Failed to deserialize snapshot for {aggregate_type}/{aggregate_id}"
            if original_error:
                self.message += f": {original_error}"

        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"SnapshotDeserializationError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"original_error={self.original_error!r})"
        )


class SnapshotSchemaVersionError(SnapshotError):
    """
    Raised when snapshot schema version doesn't match aggregate schema version.

    This is a normal occurrence during schema evolution. When an aggregate's
    state model changes and its schema_version is incremented, existing
    snapshots become incompatible and must be regenerated.

    The system handles this gracefully by:
    1. Logging the version mismatch
    2. Falling back to full event replay
    3. Optionally creating a new snapshot with the updated schema

    Attributes:
        aggregate_id: ID of the aggregate
        aggregate_type: Type of the aggregate
        snapshot_schema_version: Schema version stored in the snapshot
        expected_schema_version: Schema version expected by the aggregate

    Example:
        >>> if snapshot.schema_version != aggregate_class.schema_version:
        ...     raise SnapshotSchemaVersionError(
        ...         aggregate_id=snapshot.aggregate_id,
        ...         aggregate_type=snapshot.aggregate_type,
        ...         snapshot_schema_version=snapshot.schema_version,
        ...         expected_schema_version=aggregate_class.schema_version,
        ...     )
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        snapshot_schema_version: int,
        expected_schema_version: int,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.snapshot_schema_version = snapshot_schema_version
        self.expected_schema_version = expected_schema_version

        self.message = (
            f"Schema version mismatch for {aggregate_type}/{aggregate_id}: "
            f"snapshot has schema_version={snapshot_schema_version}, "
            f"but aggregate expects schema_version={expected_schema_version}"
        )

        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"SnapshotSchemaVersionError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"snapshot_schema_version={self.snapshot_schema_version}, "
            f"expected_schema_version={self.expected_schema_version})"
        )


class SnapshotNotFoundError(SnapshotError):
    """
    Raised when a snapshot is expected but not found.

    Note: This exception is rarely raised in practice because missing
    snapshots are a normal condition (graceful fallback to event replay).
    It's provided for cases where a snapshot is explicitly required.

    Attributes:
        aggregate_id: ID of the aggregate
        aggregate_type: Type of the aggregate

    Example:
        >>> snapshot = await store.get_snapshot(aggregate_id, "Order")
        >>> if snapshot is None and require_snapshot:
        ...     raise SnapshotNotFoundError(aggregate_id, "Order")
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type

        self.message = f"No snapshot found for {aggregate_type}/{aggregate_id}"

        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"SnapshotNotFoundError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r})"
        )


class LockAcquisitionError(EventSourceError):
    """
    Raised when a lock cannot be acquired.

    Attributes:
        key: The lock key that could not be acquired
        reason: Description of why acquisition failed
        timeout: The timeout value if timeout was the cause
    """

    def __init__(
        self,
        key: str,
        reason: str,
        timeout: float | None = None,
    ):
        self.key = key
        self.reason = reason
        self.timeout = timeout
        super().__init__(f"Failed to acquire lock '{key}': {reason}")


class LockNotHeldError(EventSourceError):
    """
    Raised when attempting to release a lock not held.

    Attributes:
        key: The lock key that was not held
    """

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Lock '{key}' is not held by this session")


# =============================================================================
# Subscription exceptions
# =============================================================================
#
# Merged from the former eventsource.application.subscriptions.exceptions module (ADR
# 0031). SubscriptionError is rebased onto EventSourceError -- a widening
# change: the sole `except SubscriptionError` call site in the repo still
# catches it, and no ErrorClassifier keys on EventSourceError.


class SubscriptionError(EventSourceError):
    """Base exception for subscription-related errors."""

    pass


class SubscriptionConfigError(SubscriptionError):
    """Raised when subscription configuration is invalid."""

    pass


class SubscriptionStateError(SubscriptionError):
    """Raised when an operation is invalid for the current state."""

    pass


class SubscriptionAlreadyExistsError(SubscriptionError):
    """Raised when trying to register a duplicate subscription."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Subscription '{name}' already exists")


class CheckpointNotFoundError(SubscriptionError):
    """Raised when checkpoint is required but not found."""

    def __init__(self, projection_name: str) -> None:
        self.projection_name = projection_name
        super().__init__(
            f"No checkpoint found for '{projection_name}'. "
            "Use start_from='beginning' to start from the beginning, "
            "or ensure the projection name is correct."
        )


class EventStoreConnectionError(SubscriptionError):
    """Raised when unable to connect to the event store."""

    pass


class EventBusConnectionError(SubscriptionError):
    """Raised when unable to connect to the event bus."""

    pass


class TransitionError(SubscriptionError):
    """Raised when catch-up to live transition fails."""

    pass
