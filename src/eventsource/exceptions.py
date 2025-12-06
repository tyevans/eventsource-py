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
