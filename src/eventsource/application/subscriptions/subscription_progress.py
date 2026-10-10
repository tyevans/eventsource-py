"""Progress, error, and lag tracking mixin for Subscription."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.subscriptions.models import (
    RecentErrorInfo,
    SubscriptionState,
    SubscriptionStatus,
    render_position,
)
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
        _recent_errors: list[RecentErrorInfo]
        _max_recent_errors: int

    async def record_event_processed(
        self,
        position: Position | None,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Record that an event was successfully processed.

        Args:
            position: Global-feed position of the event, None if the feed
                supplied no position for it
            event_id: UUID of the event
            event_type: Type of the event
        """
        async with self._lock:
            self.last_processed_position = position
            self.last_event_id = event_id
            self.last_event_type = event_type
            self.events_processed += 1
            self._events_delivered += 1
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
        position: Position | None,
        error: Exception,
        sent_to_dlq: bool = False,
    ) -> None:
        """
        Record detailed error information.

        Args:
            event_id: ID of the failed event
            event_type: Type of the failed event
            position: Global-feed position of the failed event, None if unknown
            error: The exception that occurred
            sent_to_dlq: Whether event was sent to DLQ
        """
        async with self._lock:
            error_info = RecentErrorInfo(
                event_id=event_id,
                event_type=event_type,
                position=position,
                error_type=type(error).__name__,
                error_message=str(error)[:500],
                timestamp=datetime.now(UTC),
                sent_to_dlq=sent_to_dlq,
            )

            self._recent_errors.append(error_info)

            if len(self._recent_errors) > self._max_recent_errors:
                self._recent_errors = self._recent_errors[-self._max_recent_errors :]

            self.events_failed += 1
            self.last_error = error
            self.last_error_at = datetime.now(UTC)

            if sent_to_dlq:
                self.events_dlq += 1

    async def record_events_seen(self, count: int) -> None:
        """
        Record events observed but not yet delivered.

        Args:
            count: Number of events observed in this read.
        """
        async with self._lock:
            self._events_seen += count

    async def record_events_unseen(self, count: int) -> None:
        """
        Reconcile the seen-counter when a read batch is abandoned early.

        Args:
            count: Number of events read but not delivered in this batch.
        """
        async with self._lock:
            self._events_seen = max(0, self._events_seen - count)

    async def reconcile_lag(self, target_lag: int = 0) -> None:
        """
        Reconcile the seen/delivered counters to match the target lag deficit.

        Args:
            target_lag: Desired lag deficit (events seen ahead of delivered). Defaults to 0.
        """
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
    def recent_errors(self) -> list[RecentErrorInfo]:
        """Get list of recent errors."""
        return list(self._recent_errors)

    @property
    def dlq_count(self) -> int:
        """Get count of events sent to DLQ."""
        return self.events_dlq

    def get_status(self) -> SubscriptionStatus:
        """Get a status snapshot for health checks."""
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

    def __str__(self) -> str:
        """String representation."""
        return (
            f"Subscription({self.name}, state={self.state.value}, "
            f"pos={render_position(self.last_processed_position) or '-'})"
        )


__all__ = ["SubscriptionProgressMixin"]
