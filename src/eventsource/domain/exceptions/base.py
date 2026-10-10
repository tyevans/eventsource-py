"""Base and general exceptions for the eventsource package."""

from __future__ import annotations

from uuid import UUID


class EventSourceError(Exception):
    """Base exception for eventsource library."""

    pass


class OptimisticLockError(EventSourceError):
    """Raised when there's a version conflict during event append."""

    def __init__(
        self, aggregate_id: UUID, expected_version: int | str, actual_version: int
    ) -> None:
        """
        Args:
            aggregate_id: The aggregate whose append was rejected
            expected_version: The version the caller required, or the name of
                the non-numeric `ExpectedVersion` kind they used
                (`"no_stream"`, `"stream_exists"`, `"any"`). Rendering the
                kind by name matters: a store that reported `no_stream` as
                the integer `0` told the user it expected a version they
                never wrote.
            actual_version: The stream's current version
        """
        self.aggregate_id = aggregate_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        expectation = (
            f"version {expected_version}"
            if isinstance(expected_version, int)
            else str(expected_version)
        )
        super().__init__(
            f"Optimistic lock error for aggregate {aggregate_id}: "
            f"expected {expectation}, but current version is {actual_version}"
        )


class ProjectionError(EventSourceError):
    """Raised when a projection fails to process an event."""

    def __init__(self, projection_name: str, event_id: UUID, message: str) -> None:
        self.projection_name = projection_name
        self.event_id = event_id
        super().__init__(f"Projection {projection_name} failed on event {event_id}: {message}")


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


class SerializationError(EventSourceError):
    """Raised when event serialization or deserialization fails."""

    def __init__(self, event_type: str, message: str) -> None:
        self.event_type = event_type
        super().__init__(f"Serialization error for {event_type}: {message}")


__all__ = [
    "CommandRejectedError",
    "EventSourceError",
    "OptimisticLockError",
    "ProjectionError",
    "SerializationError",
]
