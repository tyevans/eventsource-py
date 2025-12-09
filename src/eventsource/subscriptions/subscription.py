"""
Subscription class with state machine for managing subscription lifecycle.

This module provides:
- SubscriptionState: Enum of all possible subscription states
- SubscriptionStatus: Immutable snapshot for health checks
- Subscription: Main class managing state, position tracking, and statistics
- EventHandler/BatchHandler: Type aliases for event handling callables
- RecentErrorInfo: Lightweight error tracking for recent errors

State Machine:
    STARTING -> CATCHING_UP | LIVE | STOPPED | ERROR
    CATCHING_UP -> LIVE | PAUSED | STOPPED | ERROR
    LIVE -> CATCHING_UP | PAUSED | STOPPED | ERROR
    PAUSED -> CATCHING_UP | LIVE | STOPPED | ERROR
    STOPPED -> (terminal)
    ERROR -> STARTING (restart)
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.subscriptions.config import SubscriptionConfig
from eventsource.subscriptions.exceptions import SubscriptionStateError

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent
    from eventsource.protocols import EventSubscriber


logger = logging.getLogger(__name__)


class SubscriptionState(Enum):
    """
    States a subscription can be in during its lifecycle.

    State transitions:
        STARTING -> CATCHING_UP | LIVE | STOPPED | ERROR
        CATCHING_UP -> LIVE | PAUSED | STOPPED | ERROR
        LIVE -> CATCHING_UP | PAUSED | STOPPED | ERROR
        PAUSED -> CATCHING_UP | LIVE | STOPPED | ERROR
        STOPPED -> (terminal)
        ERROR -> STARTING (restart)
    """

    STARTING = "starting"
    """Initial state while reading checkpoint and initializing."""

    CATCHING_UP = "catching_up"
    """Reading historical events from the event store."""

    LIVE = "live"
    """Receiving real-time events from the event bus."""

    PAUSED = "paused"
    """Temporarily paused (backpressure or manual)."""

    STOPPED = "stopped"
    """Cleanly shut down."""

    ERROR = "error"
    """Failed with an unrecoverable error."""


class PauseReason(Enum):
    """
    Reasons why a subscription can be paused.

    Used to track the source of a pause operation for diagnostics
    and to determine resume behavior.
    """

    MANUAL = "manual"
    """User-initiated pause via API call."""

    BACKPRESSURE = "backpressure"
    """Automatic pause due to flow control backpressure."""

    MAINTENANCE = "maintenance"
    """Pause for maintenance operations."""


# Valid state transitions
VALID_TRANSITIONS: dict[SubscriptionState, set[SubscriptionState]] = {
    SubscriptionState.STARTING: {
        SubscriptionState.CATCHING_UP,
        SubscriptionState.LIVE,
        SubscriptionState.STOPPED,
        SubscriptionState.ERROR,
    },
    SubscriptionState.CATCHING_UP: {
        SubscriptionState.LIVE,
        SubscriptionState.PAUSED,
        SubscriptionState.STOPPED,
        SubscriptionState.ERROR,
    },
    SubscriptionState.LIVE: {
        SubscriptionState.CATCHING_UP,  # Falls behind
        SubscriptionState.PAUSED,
        SubscriptionState.STOPPED,
        SubscriptionState.ERROR,
    },
    SubscriptionState.PAUSED: {
        SubscriptionState.CATCHING_UP,
        SubscriptionState.LIVE,
        SubscriptionState.STOPPED,
        SubscriptionState.ERROR,
    },
    SubscriptionState.STOPPED: set(),  # Terminal state
    SubscriptionState.ERROR: {
        SubscriptionState.STARTING,  # Allow restart
    },
}


def is_valid_transition(
    from_state: SubscriptionState,
    to_state: SubscriptionState,
) -> bool:
    """
    Check if a state transition is valid.

    Args:
        from_state: Current state
        to_state: Target state

    Returns:
        True if transition is allowed, False otherwise
    """
    return to_state in VALID_TRANSITIONS.get(from_state, set())


# Type aliases for event handlers
EventHandler = Callable[["DomainEvent"], Awaitable[None]]
"""Async handler for a single event."""

BatchHandler = Callable[[Sequence["DomainEvent"]], Awaitable[None]]
"""Async handler for a batch of events."""


@dataclass
class RecentErrorInfo:
    """
    Lightweight information about a recent processing error.

    Used to track recent errors in the subscription for debugging
    and monitoring without storing full stack traces in memory.

    Attributes:
        event_id: ID of the failed event
        event_type: Type of the failed event
        position: Global position of the failed event
        error_type: Exception class name
        error_message: Error message (truncated)
        timestamp: When the error occurred
        sent_to_dlq: Whether event was sent to DLQ
    """

    event_id: UUID
    event_type: str
    position: int
    error_type: str
    error_message: str
    timestamp: datetime
    sent_to_dlq: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "position": self.position,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "timestamp": self.timestamp.isoformat(),
            "sent_to_dlq": self.sent_to_dlq,
        }


@dataclass(frozen=True)
class SubscriptionStatus:
    """
    Status snapshot for health checks and monitoring.

    This is a point-in-time snapshot of subscription state,
    suitable for serialization and external reporting.

    Attributes:
        name: Subscription name
        state: Current state as string
        position: Last processed global position
        lag_events: Number of events behind
        events_processed: Total events successfully processed
        events_failed: Total events that failed processing
        events_dlq: Total events sent to dead letter queue
        last_processed_at: ISO timestamp of last processed event
        started_at: ISO timestamp when subscription started
        uptime_seconds: Time since subscription started
        error: Error message if in error state
        recent_errors_count: Number of recent errors in buffer
    """

    name: str
    state: str
    position: int
    lag_events: int
    events_processed: int
    events_failed: int
    last_processed_at: str | None
    started_at: str | None
    uptime_seconds: float
    error: str | None = None
    events_dlq: int = 0
    recent_errors_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation of status
        """
        return {
            "name": self.name,
            "state": self.state,
            "position": self.position,
            "lag_events": self.lag_events,
            "events_processed": self.events_processed,
            "events_failed": self.events_failed,
            "events_dlq": self.events_dlq,
            "last_processed_at": self.last_processed_at,
            "started_at": self.started_at,
            "uptime_seconds": self.uptime_seconds,
            "error": self.error,
            "recent_errors_count": self.recent_errors_count,
        }


@dataclass
class Subscription:
    """
    Represents an active subscription to events.

    Manages state machine, position tracking, and statistics for a
    single subscriber. The Subscription itself does not process events;
    it tracks state and delegates to runners.

    Attributes:
        name: Unique name for this subscription (typically class name)
        config: Configuration for this subscription
        subscriber: The event subscriber (projection) to receive events

    Example:
        >>> from eventsource.subscriptions import SubscriptionConfig
        >>> config = SubscriptionConfig()
        >>> subscription = Subscription(
        ...     name="OrderProjection",
        ...     config=config,
        ...     subscriber=my_projection,
        ... )
        >>> await subscription.transition_to(SubscriptionState.CATCHING_UP)
    """

    name: str
    config: SubscriptionConfig
    subscriber: "EventSubscriber" = field(repr=False)

    # State
    state: SubscriptionState = field(default=SubscriptionState.STARTING)
    _previous_state: SubscriptionState | None = field(default=None, repr=False)

    # Position tracking
    last_processed_position: int = field(default=0)
    last_event_id: UUID | None = field(default=None)
    last_event_type: str | None = field(default=None)

    # Statistics
    events_processed: int = field(default=0)
    events_failed: int = field(default=0)
    last_processed_at: datetime | None = field(default=None)
    started_at: datetime | None = field(default=None)

    # Error tracking
    last_error: Exception | None = field(default=None, repr=False)
    last_error_at: datetime | None = field(default=None)
    events_dlq: int = field(default=0)  # Count of events sent to DLQ

    # Pause tracking
    _pause_reason: PauseReason | None = field(default=None, repr=False)
    _state_before_pause: SubscriptionState | None = field(default=None, repr=False)
    _paused_at: datetime | None = field(default=None, repr=False)
    _pause_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    # Internal
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    _max_position: int = field(default=0, repr=False)  # For lag calculation
    _recent_errors: list[RecentErrorInfo] = field(default_factory=list, repr=False)
    _max_recent_errors: int = field(default=100, repr=False)

    def __post_init__(self) -> None:
        """Initialize the subscription."""
        self.started_at = datetime.now(UTC)
        self._recent_errors = []
        # Set pause event initially (not paused)
        self._pause_event.set()

    async def transition_to(self, new_state: SubscriptionState) -> None:
        """
        Transition to a new state.

        Args:
            new_state: The state to transition to

        Raises:
            SubscriptionStateError: If the transition is not valid
        """
        async with self._lock:
            if not is_valid_transition(self.state, new_state):
                valid_targets = VALID_TRANSITIONS.get(self.state, set())
                raise SubscriptionStateError(
                    f"Cannot transition from {self.state.value} to {new_state.value}. "
                    f"Valid transitions: {[s.value for s in valid_targets]}"
                )

            self._previous_state = self.state
            old_state = self.state
            self.state = new_state

            # Log state transitions
            if new_state == SubscriptionState.ERROR:
                logger.error(
                    "Subscription entered error state",
                    extra={
                        "subscription": self.name,
                        "from_state": old_state.value,
                        "to_state": new_state.value,
                        "error": str(self.last_error) if self.last_error else None,
                    },
                )
            else:
                logger.info(
                    "Subscription state changed",
                    extra={
                        "subscription": self.name,
                        "from_state": old_state.value,
                        "to_state": new_state.value,
                    },
                )

    async def record_event_processed(
        self,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Record that an event was successfully processed.

        Args:
            position: Global position of the event
            event_id: UUID of the event
            event_type: Type of the event
        """
        async with self._lock:
            self.last_processed_position = position
            self.last_event_id = event_id
            self.last_event_type = event_type
            self.events_processed += 1
            self.last_processed_at = datetime.now(UTC)

    async def record_event_failed(self, error: Exception) -> None:
        """
        Record that an event processing failed.

        Args:
            error: The exception that occurred
        """
        async with self._lock:
            self.events_failed += 1
            self.last_error = error
            self.last_error_at = datetime.now(UTC)

    async def record_event_error(
        self,
        event_id: UUID,
        event_type: str,
        position: int,
        error: Exception,
        sent_to_dlq: bool = False,
    ) -> None:
        """
        Record detailed error information.

        Args:
            event_id: ID of the failed event
            event_type: Type of the failed event
            position: Global position of the failed event
            error: The exception that occurred
            sent_to_dlq: Whether event was sent to DLQ
        """
        async with self._lock:
            error_info = RecentErrorInfo(
                event_id=event_id,
                event_type=event_type,
                position=position,
                error_type=type(error).__name__,
                error_message=str(error)[:500],  # Truncate long messages
                timestamp=datetime.now(UTC),
                sent_to_dlq=sent_to_dlq,
            )

            self._recent_errors.append(error_info)

            # Keep only recent errors
            if len(self._recent_errors) > self._max_recent_errors:
                self._recent_errors = self._recent_errors[-self._max_recent_errors :]

            self.events_failed += 1
            self.last_error = error
            self.last_error_at = datetime.now(UTC)

            if sent_to_dlq:
                self.events_dlq += 1

    async def update_max_position(self, position: int) -> None:
        """
        Update the known maximum position in the event store.

        Used for calculating lag.

        Args:
            position: Current max position in event store
        """
        async with self._lock:
            self._max_position = position

    @property
    def lag(self) -> int:
        """
        Calculate current lag (events behind).

        Returns:
            Number of events behind the current max position
        """
        return max(0, self._max_position - self.last_processed_position)

    @property
    def is_running(self) -> bool:
        """
        Check if subscription is in a running state.

        Returns:
            True if catching up or live
        """
        return self.state in {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
        }

    @property
    def is_terminal(self) -> bool:
        """
        Check if subscription is in a terminal state.

        Returns:
            True if stopped or in error state
        """
        return self.state in {
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }

    @property
    def uptime_seconds(self) -> float:
        """
        Calculate uptime in seconds.

        Returns:
            Seconds since subscription started
        """
        if self.started_at is None:
            return 0.0
        return (datetime.now(UTC) - self.started_at).total_seconds()

    @property
    def previous_state(self) -> SubscriptionState | None:
        """
        Get the previous state before the last transition.

        Returns:
            Previous state or None if no transition has occurred
        """
        return self._previous_state

    @property
    def recent_errors(self) -> list[RecentErrorInfo]:
        """
        Get list of recent errors.

        Returns:
            List of recent error info objects
        """
        return list(self._recent_errors)

    @property
    def dlq_count(self) -> int:
        """
        Get count of events sent to DLQ.

        Returns:
            Number of events sent to dead letter queue
        """
        return self.events_dlq

    def get_status(self) -> SubscriptionStatus:
        """
        Get a status snapshot for health checks.

        Returns:
            SubscriptionStatus with current state and statistics
        """
        return SubscriptionStatus(
            name=self.name,
            state=self.state.value,
            position=self.last_processed_position,
            lag_events=self.lag,
            events_processed=self.events_processed,
            events_failed=self.events_failed,
            last_processed_at=(
                self.last_processed_at.isoformat() if self.last_processed_at else None
            ),
            started_at=(self.started_at.isoformat() if self.started_at else None),
            uptime_seconds=self.uptime_seconds,
            error=str(self.last_error) if self.last_error else None,
            events_dlq=self.events_dlq,
            recent_errors_count=len(self._recent_errors),
        )

    async def set_error(self, error: Exception) -> None:
        """
        Set error state with the given exception.

        Records the error and transitions to ERROR state.

        Args:
            error: The exception that caused the error
        """
        await self.record_event_failed(error)
        await self.transition_to(SubscriptionState.ERROR)

    async def pause(
        self,
        reason: PauseReason = PauseReason.MANUAL,
    ) -> None:
        """
        Pause event processing.

        The subscription will stop receiving events but maintain
        its position. Call resume() to continue processing.

        Args:
            reason: The reason for pausing (defaults to MANUAL)

        Raises:
            SubscriptionStateError: If not in a pausable state (CATCHING_UP or LIVE)

        Example:
            >>> await subscription.pause()
            >>> # ... later ...
            >>> await subscription.resume()
        """
        async with self._lock:
            if self.state not in (SubscriptionState.CATCHING_UP, SubscriptionState.LIVE):
                raise SubscriptionStateError(
                    f"Cannot pause from state {self.state.value}. "
                    "Subscription must be in CATCHING_UP or LIVE state."
                )

            # Store state before pause for resume
            self._state_before_pause = self.state
            self._pause_reason = reason
            self._paused_at = datetime.now(UTC)

            # Clear pause event to signal runners to pause
            self._pause_event.clear()

        # Transition to PAUSED state (this will acquire lock again)
        await self.transition_to(SubscriptionState.PAUSED)

        logger.info(
            "Subscription paused",
            extra={
                "subscription": self.name,
                "reason": reason.value,
                "position": self.last_processed_position,
                "previous_state": self._state_before_pause.value
                if self._state_before_pause
                else None,
            },
        )

    async def resume(self) -> None:
        """
        Resume event processing.

        Continues from the last processed position. The subscription
        will return to its previous state (CATCHING_UP or LIVE).

        Raises:
            SubscriptionStateError: If not in PAUSED state

        Example:
            >>> await subscription.pause()
            >>> # ... later ...
            >>> await subscription.resume()
        """
        async with self._lock:
            if self.state != SubscriptionState.PAUSED:
                raise SubscriptionStateError(
                    f"Cannot resume from state {self.state.value}. Subscription must be PAUSED."
                )

            # Determine target state for resume
            target_state = self._state_before_pause or SubscriptionState.CATCHING_UP

            # Clear pause tracking
            pause_reason = self._pause_reason
            pause_duration = None
            if self._paused_at:
                pause_duration = (datetime.now(UTC) - self._paused_at).total_seconds()

            self._pause_reason = None
            self._paused_at = None

            # Set pause event to signal runners to resume
            self._pause_event.set()

        # Transition back to previous state
        await self.transition_to(target_state)

        logger.info(
            "Subscription resumed",
            extra={
                "subscription": self.name,
                "reason": pause_reason.value if pause_reason else None,
                "position": self.last_processed_position,
                "resumed_to": target_state.value,
                "pause_duration_seconds": pause_duration,
            },
        )

    async def wait_if_paused(self) -> bool:
        """
        Wait if the subscription is paused.

        This method should be called by runners to check and wait
        for resume if paused. It blocks until the subscription
        is resumed or returns immediately if not paused.

        Returns:
            True if was paused and resumed, False if not paused

        Example:
            >>> # In runner processing loop
            >>> was_paused = await subscription.wait_if_paused()
            >>> if was_paused:
            ...     # Re-check state after resume
            ...     pass
        """
        if self._pause_event.is_set():
            return False

        # Wait for resume
        await self._pause_event.wait()
        return True

    @property
    def is_paused(self) -> bool:
        """
        Check if subscription is currently paused.

        Returns:
            True if in PAUSED state
        """
        return self.state == SubscriptionState.PAUSED

    @property
    def pause_reason(self) -> PauseReason | None:
        """
        Get the reason for the current pause.

        Returns:
            PauseReason if paused, None otherwise
        """
        return self._pause_reason

    @property
    def state_before_pause(self) -> SubscriptionState | None:
        """
        Get the state before the subscription was paused.

        Returns:
            Previous state if paused, None otherwise
        """
        return self._state_before_pause

    @property
    def paused_at(self) -> datetime | None:
        """
        Get the timestamp when the subscription was paused.

        Returns:
            Datetime of pause, None if not paused
        """
        return self._paused_at

    @property
    def pause_duration_seconds(self) -> float | None:
        """
        Get the duration of the current pause in seconds.

        Returns:
            Seconds since pause started, None if not paused
        """
        if self._paused_at is None:
            return None
        return (datetime.now(UTC) - self._paused_at).total_seconds()

    def __str__(self) -> str:
        """String representation."""
        return (
            f"Subscription({self.name}, state={self.state.value}, "
            f"pos={self.last_processed_position})"
        )


__all__ = [
    "SubscriptionState",
    "SubscriptionStatus",
    "Subscription",
    "is_valid_transition",
    "VALID_TRANSITIONS",
    "EventHandler",
    "BatchHandler",
    "RecentErrorInfo",
    "PauseReason",
]
