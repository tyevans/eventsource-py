"""
Infrastructure exception taxonomy (ADR 0041).

These error types describe failures of the *port* contracts — stores,
buses, locks, positions, checkpoints, subscriptions — not domain
concepts. They are rooted in EventSourceError so `except
EventSourceError` still catches everything, but they live in the ports
ring: importable by application and adapters (which raise them) without
polluting the entities ring.
"""

from __future__ import annotations

from eventsource.domain.exceptions import EventSourceError

__all__ = [
    "CheckpointError",
    "CheckpointNotFoundError",
    "EventBusConnectionError",
    "EventStoreConnectionError",
    "LockAcquisitionError",
    "LockNotHeldError",
    "PositionDecodeError",
    "PositionForeignError",
    "SubscriptionAlreadyExistsError",
    "SubscriptionConfigError",
    "SubscriptionError",
    "SubscriptionStateError",
    "TransitionError",
]


class CheckpointError(EventSourceError):
    """Raised when there's an error with checkpoint operations."""

    pass


class PositionDecodeError(EventSourceError):
    """A persisted position string could not be decoded."""


class PositionForeignError(EventSourceError):
    """Positions from different stores were compared for order."""


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
