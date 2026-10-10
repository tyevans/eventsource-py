"""Pause, resume, and waiting mixin for Subscription."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.models import (
    PauseReason,
    SubscriptionState,
    render_position,
)
from eventsource.ports.exceptions import SubscriptionStateError
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SubscriptionPauseMixin:
    """Mixin providing pause, resume, and wait logic for Subscription."""

    if TYPE_CHECKING:
        name: str
        state: SubscriptionState
        last_processed_position: Position | None
        _lock: asyncio.Lock
        _pause_reason: PauseReason | None
        _state_before_pause: SubscriptionState | None
        _paused_at: datetime | None
        _pause_event: asyncio.Event

        async def transition_to(self, new_state: SubscriptionState) -> None: ...

    async def pause(
        self,
        reason: PauseReason = PauseReason.MANUAL,
    ) -> None:
        """
        Pause event processing.

        Args:
            reason: The reason for pausing (defaults to MANUAL)

        Raises:
            SubscriptionStateError: If not in a pausable state (CATCHING_UP or LIVE)
        """
        async with self._lock:
            if self.state not in (SubscriptionState.CATCHING_UP, SubscriptionState.LIVE):
                raise SubscriptionStateError(
                    f"Cannot pause from state {self.state.value}. "
                    "Subscription must be in CATCHING_UP or LIVE state."
                )

            self._state_before_pause = self.state
            self._pause_reason = reason
            self._paused_at = datetime.now(UTC)
            self._pause_event.clear()

        await self.transition_to(SubscriptionState.PAUSED)

        logger.info(
            "Subscription paused",
            extra={
                "subscription": self.name,
                "reason": reason.value,
                "position": render_position(self.last_processed_position),
                "previous_state": self._state_before_pause.value
                if self._state_before_pause
                else None,
            },
        )

    async def resume(self) -> None:
        """
        Resume event processing.

        Raises:
            SubscriptionStateError: If not in PAUSED state
        """
        async with self._lock:
            if self.state != SubscriptionState.PAUSED:
                raise SubscriptionStateError(
                    f"Cannot resume from state {self.state.value}. Subscription must be PAUSED."
                )

            target_state = self._state_before_pause or SubscriptionState.CATCHING_UP

            pause_reason = self._pause_reason
            pause_duration = None
            if self._paused_at:
                pause_duration = (datetime.now(UTC) - self._paused_at).total_seconds()

            self._pause_reason = None
            self._paused_at = None
            self._pause_event.set()

        await self.transition_to(target_state)

        logger.info(
            "Subscription resumed",
            extra={
                "subscription": self.name,
                "reason": pause_reason.value if pause_reason else None,
                "position": render_position(self.last_processed_position),
                "resumed_to": target_state.value,
                "pause_duration_seconds": pause_duration,
            },
        )

    async def wait_if_paused(self, stop_signal: asyncio.Event | None = None) -> bool:
        """
        Wait if the subscription is paused.

        Args:
            stop_signal: Optional event that also ends the wait when set.

        Returns:
            True if the wait actually blocked, False if not paused.
        """
        if self._pause_event.is_set():
            return False

        if stop_signal is None:
            await self._pause_event.wait()
            return True

        if stop_signal.is_set():
            return True

        waiters = [
            asyncio.create_task(self._pause_event.wait()),
            asyncio.create_task(stop_signal.wait()),
        ]
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in waiters:
                task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
        return True

    @property
    def is_paused(self) -> bool:
        """Check if subscription is currently paused."""
        return self.state == SubscriptionState.PAUSED

    @property
    def pause_reason(self) -> PauseReason | None:
        """Get the reason for the current pause."""
        return self._pause_reason

    @property
    def state_before_pause(self) -> SubscriptionState | None:
        """Get the state before the subscription was paused."""
        return self._state_before_pause

    @property
    def paused_at(self) -> datetime | None:
        """Get the timestamp when the subscription was paused."""
        return self._paused_at

    @property
    def pause_duration_seconds(self) -> float | None:
        """Get the duration of the current pause in seconds."""
        if self._paused_at is None:
            return None
        return (datetime.now(UTC) - self._paused_at).total_seconds()


__all__ = ["SubscriptionPauseMixin"]
