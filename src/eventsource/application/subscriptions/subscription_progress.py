"""Progress, error, and lag tracking mixin for Subscription."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.subscriptions.models import (
    SubscriptionState,
    SubscriptionStatus,
    render_position,
)
from eventsource.observability.error_summary import error_summary
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    import asyncio


class SubscriptionProgressMixin:
    """Mixin providing event recording, error tracking, and lag calculation."""

    if TYPE_CHECKING:
        name: str
        state: SubscriptionState
        last_processed_position: Position | None
        last_event_id: UUID | None
        last_event_type: str | None
        events_processed: int
        events_failed: int
        last_processed_at: datetime | None
        started_at: datetime | None
        last_error: Exception | None
        last_error_at: datetime | None
        events_dlq: int
        _lock: asyncio.Lock
        _events_seen: int
        _events_delivered: int

    async def record_event_processed(
        self, position: Position | None, event_id: UUID, event_type: str
    ) -> None:
        """Record that an event was successfully processed."""
        async with self._lock:
            self.last_processed_position = position
            self.last_event_id = event_id
            self.last_event_type = event_type
            self.events_processed += 1
            self._events_delivered += 1
            self.last_processed_at = datetime.now(UTC)

    async def advance_past_failed_event(
        self, position: Position | None, event_id: UUID, event_type: str
    ) -> None:
        """Move the position past an event whose handler failed, without counting it processed.

        The event counts as consumed (so lag stays accurate) and the position moves on
        (so a checkpoint can pass it under `continue_on_error`). `events_processed` does
        not change: the failure is already counted in `events_failed`.
        """
        async with self._lock:
            self.last_processed_position = position
            self.last_event_id = event_id
            self.last_event_type = event_type
            self._events_delivered += 1

    async def record_event_failed(self, error: Exception) -> None:
        """Record that an event processing failed."""
        async with self._lock:
            self.events_failed += 1
            self.last_error = error
            self.last_error_at = datetime.now(UTC)

    async def record_events_seen(self, count: int) -> None:
        """Record events observed but not yet delivered."""
        async with self._lock:
            self._events_seen += count

    async def record_events_unseen(self, count: int) -> None:
        """Reconcile the seen-counter when a read batch is abandoned early."""
        async with self._lock:
            self._events_seen = max(0, self._events_seen - count)

    async def reconcile_lag(self, target_lag: int = 0) -> None:
        """Reconcile the seen/delivered counters to match the target lag deficit."""
        async with self._lock:
            self._events_seen = self._events_delivered + max(0, target_lag)

    @property
    def lag(self) -> int:
        """Calculate current lag (events behind)."""
        return max(0, self._events_seen - self._events_delivered)

    @property
    def uptime_seconds(self) -> float:
        """Calculate uptime in seconds."""
        if self.started_at is None:
            return 0.0
        return (datetime.now(UTC) - self.started_at).total_seconds()

    @property
    def dlq_count(self) -> int:
        """Get count of events sent to DLQ."""
        return self.events_dlq

    def get_status(self) -> SubscriptionStatus:
        """Get a status snapshot for health checks."""
        last_proc = self.last_processed_at.isoformat() if self.last_processed_at else None
        started = self.started_at.isoformat() if self.started_at else None
        return SubscriptionStatus(
            name=self.name,
            state=self.state.value,
            position=self.last_processed_position,
            lag_events=self.lag,
            events_processed=self.events_processed,
            events_failed=self.events_failed,
            last_processed_at=last_proc,
            started_at=started,
            uptime_seconds=self.uptime_seconds,
            error=error_summary(self.last_error) if self.last_error else None,
            events_dlq=self.events_dlq,
        )

    def __str__(self) -> str:
        """String representation."""
        pos = render_position(self.last_processed_position) or "-"
        return f"Subscription({self.name}, state={self.state.value}, pos={pos})"


__all__ = ["SubscriptionProgressMixin"]
