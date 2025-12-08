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
