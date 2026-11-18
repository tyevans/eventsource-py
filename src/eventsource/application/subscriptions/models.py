"""Subscription models, state definitions, and status snapshots."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent


def render_position(position: Position | None) -> str | None:
    """Render a position as its opaque token string, or None."""
    return position.to_str() if position is not None else None


class SubscriptionState(Enum):
    """States a subscription can be in during its lifecycle."""

    STARTING = "starting"
    CATCHING_UP = "catching_up"
    LIVE = "live"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


class PauseReason(Enum):
    """Reasons why a subscription can be paused for diagnostics and resume behavior."""

    MANUAL = "manual"
    BACKPRESSURE = "backpressure"
    MAINTENANCE = "maintenance"


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
        SubscriptionState.CATCHING_UP,
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
    SubscriptionState.STOPPED: set(),
    SubscriptionState.ERROR: {SubscriptionState.STARTING},
}


def is_valid_transition(from_state: SubscriptionState, to_state: SubscriptionState) -> bool:
    """Check if a state transition from `from_state` to `to_state` is valid."""
    return to_state in VALID_TRANSITIONS.get(from_state, set())


EventHandler = Callable[["DomainEvent"], Awaitable[None]]
BatchHandler = Callable[[Sequence["DomainEvent"]], Awaitable[None]]


@dataclass(frozen=True)
class SubscriptionStatus:
    """Point-in-time status snapshot for health checks and monitoring."""

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

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
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
        }


__all__ = [
    "BatchHandler",
    "EventHandler",
    "PauseReason",
    "SubscriptionState",
    "SubscriptionStatus",
    "VALID_TRANSITIONS",
    "is_valid_transition",
    "render_position",
]
