"""Failure tracking operations mixin for DualWriteInterceptor."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.dual_write_types import FailedWrite, FailureStats

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent
    from eventsource.ports import Position

logger = logging.getLogger(__name__)


class DualWriteTrackingMixin:
    """Mixin providing failure recording, statistics, and history management."""

    _tenant_id: UUID
    _failed_writes: list[FailedWrite]
    _affected_aggregates: set[UUID]
    _max_failure_history: int

    def get_failed_writes(self) -> list[FailedWrite]:
        """
        Get the list of failed target writes.

        Returns:
            List of FailedWrite records in chronological order.
        """
        return list(self._failed_writes)

    def get_failure_stats(self) -> FailureStats:
        """
        Get aggregate statistics about write failures.

        Returns:
            FailureStats with summary metrics.
        """
        if not self._failed_writes:
            return FailureStats()

        total_events = sum(len(fw.event_ids) for fw in self._failed_writes)

        return FailureStats(
            total_failures=len(self._failed_writes),
            total_events_failed=total_events,
            first_failure_at=self._failed_writes[0].timestamp,
            last_failure_at=self._failed_writes[-1].timestamp,
            unique_aggregates_affected=len(self._affected_aggregates),
        )

    def clear_failure_history(self) -> int:
        """
        Clear the failure history.

        Useful after background sync has caught up and recovered all failures.

        Returns:
            Number of failure records cleared.
        """
        count = len(self._failed_writes)
        self._failed_writes.clear()
        self._affected_aggregates.clear()
        return count

    def _record_sync_failure(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: Sequence[DomainEvent],
        error: Exception,
        source_position: Position | None,
    ) -> None:
        """
        Record a failed target write for monitoring and recovery.

        Args:
            aggregate_id: The aggregate that was being written to.
            aggregate_type: Type of the aggregate.
            events: The events that failed to write.
            error: The exception that caused the failure.
            source_position: Position of the first event of the successful
                source append; None for a feedless source store.
        """
        failed_write = FailedWrite(
            timestamp=datetime.now(UTC),
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            event_ids=[e.event_id for e in events],
            error_message=str(error),
            source_position=source_position,
        )

        self._failed_writes.append(failed_write)
        self._affected_aggregates.add(aggregate_id)

        # Trim old failures to prevent unbounded growth
        if len(self._failed_writes) > self._max_failure_history:
            removed = self._failed_writes[: -self._max_failure_history]
            self._failed_writes = self._failed_writes[-self._max_failure_history :]
            self._affected_aggregates = {fw.aggregate_id for fw in self._failed_writes}
            logger.debug(f"Trimmed {len(removed)} old failure records for tenant {self._tenant_id}")


__all__ = [
    "DualWriteTrackingMixin",
]
