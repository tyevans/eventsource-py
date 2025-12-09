"""
Subscription-specific exceptions.

All exceptions inherit from SubscriptionError for easy catching.
This module follows the same patterns as eventsource.exceptions.
"""


class SubscriptionError(Exception):
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
