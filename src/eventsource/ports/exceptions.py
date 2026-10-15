"""Infrastructure exception taxonomy for port contracts (ADR 0041)."""

from __future__ import annotations

from eventsource.domain.exceptions import EventSourceError, EventStoreError

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
    """Base class for checkpoint operation failures (distinct from CheckpointNotFoundError)."""


class PositionDecodeError(EventSourceError):
    """A persisted position string could not be decoded."""


class PositionForeignError(EventSourceError):
    """Positions from different stores were compared for order."""


class LockAcquisitionError(EventSourceError):
    """Raised when a lock cannot be acquired."""

    def __init__(self, key: str, reason: str, timeout: float | None = None):
        self.key = key
        self.reason = reason
        self.timeout = timeout
        super().__init__(f"Failed to acquire lock '{key}': {reason}")


class LockNotHeldError(EventSourceError):
    """Raised when attempting to release a lock not held."""

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Lock '{key}' is not held by this session")


class SubscriptionError(EventSourceError):
    """Base exception for subscription-related errors."""


class SubscriptionConfigError(SubscriptionError):
    """Raised when subscription configuration is invalid."""


class SubscriptionStateError(SubscriptionError):
    """Raised when an operation is invalid for the current state."""


class SubscriptionAlreadyExistsError(SubscriptionError):
    """Raised when trying to register a duplicate subscription."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Subscription '{name}' already exists")


class CheckpointNotFoundError(SubscriptionError):
    """Signals that a checkpoint was required but not found."""

    def __init__(self, projection_name: str) -> None:
        self.projection_name = projection_name
        super().__init__(
            f"No checkpoint found for '{projection_name}'. "
            "Use start_from='beginning' to start from the beginning, "
            "or ensure the projection name is correct."
        )


class EventStoreConnectionError(EventStoreError):
    """Raised when a store adapter cannot reach its backing database."""

    def __init__(self, message: str, *, store: str | None = None) -> None:
        self.store = store
        super().__init__(f"{store}: {message}" if store else message)


class EventBusConnectionError(SubscriptionError):
    """Raised when unable to connect to the event bus."""


class TransitionError(SubscriptionError):
    """Raised when catch-up to live transition fails."""
