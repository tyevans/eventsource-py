"""Subscription core dataclass and state machine coordination."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.models import (
    VALID_TRANSITIONS,
    PauseReason,
    RecentErrorInfo,
    SubscriptionState,
    is_valid_transition,
)
from eventsource.application.subscriptions.subscription_pause import SubscriptionPauseMixin
from eventsource.application.subscriptions.subscription_progress import (
    SubscriptionProgressMixin,
)
from eventsource.ports.exceptions import SubscriptionStateError
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.ports.handlers import EventSubscriber

logger = logging.getLogger(__name__)


@dataclass
class Subscription(SubscriptionPauseMixin, SubscriptionProgressMixin):
    """
    Represents an active subscription to events.

    Manages state machine, position tracking, and statistics for a
    single subscriber. The Subscription itself does not process events;
    it tracks state and delegates to runners.
    """

    name: str
    config: SubscriptionConfig
    subscriber: EventSubscriber = field(repr=False)

    # State
    state: SubscriptionState = field(default=SubscriptionState.STARTING)
    _previous_state: SubscriptionState | None = field(default=None, repr=False)

    # Position tracking
    last_processed_position: Position | None = field(default=None)
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
    events_dlq: int = field(default=0)

    # Pause tracking
    _pause_reason: PauseReason | None = field(default=None, repr=False)
    _state_before_pause: SubscriptionState | None = field(default=None, repr=False)
    _paused_at: datetime | None = field(default=None, repr=False)
    _pause_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    # Internal
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    _events_seen: int = field(default=0, repr=False)
    _events_delivered: int = field(default=0, repr=False)
    _recent_errors: list[RecentErrorInfo] = field(default_factory=list, repr=False)
    _max_recent_errors: int = field(default=100, repr=False)

    def __post_init__(self) -> None:
        """Initialize the subscription."""
        self.started_at = datetime.now(UTC)
        self._recent_errors = []
        self._pause_event.set()

    async def transition_to(self, new_state: SubscriptionState) -> None:
        """
        Transition to a new state.

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

    async def set_error(self, error: Exception) -> None:
        """Set error state with the given exception."""
        await self.record_event_failed(error)
        await self.transition_to(SubscriptionState.ERROR)

    @property
    def is_running(self) -> bool:
        """Check if subscription is in a running state."""
        return self.state in {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
        }

    @property
    def is_terminal(self) -> bool:
        """Check if subscription is in a terminal state."""
        return self.state in {
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }

    @property
    def previous_state(self) -> SubscriptionState | None:
        """Get the previous state before the last transition."""
        return self._previous_state


__all__ = ["Subscription"]
