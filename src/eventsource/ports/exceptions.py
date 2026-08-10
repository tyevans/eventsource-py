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
    """Base class for checkpoint operation failures.

    Nothing raises `CheckpointError` itself, and that is correct: it is the
    family root, meant to be caught rather than raised. `except CheckpointError`
    handles any checkpoint failure without naming each subclass. Adapters raise
    a specific subclass; a bare `CheckpointError` would say only that something
    checkpoint-shaped went wrong.

    Distinct from `CheckpointNotFoundError`, which is a `SubscriptionError`
    under a different root with a different meaning.
    """

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
    """Signals that a checkpoint was required but not found.

    Who raises this:
        Not the library. A subscription's first ever start has no checkpoint
        row, so `StartFromResolver` logs and resolves to `None`, replaying from
        the beginning -- usually what a projection wants.

        This type is published for code that wants the opposite: a caller who
        treats a missing checkpoint as fatal rather than replaying a whole
        store, or a `CheckpointRepository` implementor signalling the condition
        precisely. Note the trade-off it guards against -- a mistyped
        subscription name silently starts a full replay under a new checkpoint
        key instead of resuming the old one.
    """

    def __init__(self, projection_name: str) -> None:
        self.projection_name = projection_name
        super().__init__(
            f"No checkpoint found for '{projection_name}'. "
            "Use start_from='beginning' to start from the beginning, "
            "or ensure the projection name is correct."
        )


class EventStoreConnectionError(EventStoreError):
    """Raised when a store adapter cannot reach its backing database.

    Wraps the driver's own exception (`sqlite3.OperationalError`,
    `asyncpg`/SQLAlchemy connection errors) so callers get a library type
    naming the store, with the original attached as `__cause__`. It was
    previously a `SubscriptionError` subclass, which put a store-connection
    failure under the subscription taxonomy and made
    `except SubscriptionError` the only way to catch it.
    """

    def __init__(self, message: str, *, store: str | None = None) -> None:
        """
        Args:
            message: What failed, in the library's terms
            store: The adapter that failed, e.g. "SQLiteEventStore"
        """
        self.store = store
        super().__init__(f"{store}: {message}" if store else message)


class EventBusConnectionError(SubscriptionError):
    """Raised when unable to connect to the event bus."""

    pass


class TransitionError(SubscriptionError):
    """Raised when catch-up to live transition fails."""

    pass
