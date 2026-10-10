"""
Subscription models, state definitions, and status snapshots.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent


def render_position(position: Position | None) -> str | None:
    """Render a position as its opaque token string, or None.

    Span attributes, log `extra` payloads and DLQ records must carry
    primitives, never the value object's repr.
    """
    return position.to_str() if position is not None else None


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
    """Temporarily paused. See `PauseReason` for who asked."""

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
    """Paused because a caller reported downstream pressure.

    The library never pauses for this reason on its own -- delivery is
    sequential, so there is no in-library backpressure to detect. This is
    vocabulary for an application that pauses a subscription because *its*
    downstream is struggling, so the reason is legible in health output."""

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
        position: Global-feed position of the failed event, None if unknown
        error_type: Exception class name
        error_message: Error message (truncated)
        timestamp: When the error occurred
        sent_to_dlq: Whether event was sent to DLQ
    """

    event_id: UUID
    event_type: str
    position: Position | None
    error_type: str
    error_message: str
    timestamp: datetime
    sent_to_dlq: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "position": render_position(self.position),
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
        position: Last processed global-feed position, None if nothing
            has been processed
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
    position: Position | None
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
            "position": render_position(self.position),
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


__all__ = [
    "BatchHandler",
    "EventHandler",
    "PauseReason",
    "RecentErrorInfo",
    "SubscriptionState",
    "SubscriptionStatus",
    "VALID_TRANSITIONS",
    "is_valid_transition",
    "render_position",
]
