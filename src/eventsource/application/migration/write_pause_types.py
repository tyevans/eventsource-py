"""
Write pause data models, metrics, and exceptions.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from eventsource.domain.exceptions import EventSourceError


class WritePausedError(EventSourceError):
    """
    Raised when a write operation times out waiting for pause to end.

    This error indicates that a write operation was attempted for a tenant
    whose writes are paused, and the configured timeout was exceeded while
    waiting for the pause to be lifted.

    Attributes:
        tenant_id: The tenant UUID for which writes are paused.
        timeout: The timeout duration in seconds that was exceeded.
        waited_ms: How long the operation actually waited in milliseconds.
    """

    def __init__(
        self,
        tenant_id: UUID,
        timeout: float,
        waited_ms: float | None = None,
    ) -> None:
        """
        Initialize the error.

        Args:
            tenant_id: The tenant UUID for which writes are paused.
            timeout: The timeout duration in seconds.
            waited_ms: How long the operation waited (optional).
        """
        self.tenant_id = tenant_id
        self.timeout = timeout
        self.waited_ms = waited_ms

        waited_info = f" (waited {waited_ms:.2f}ms)" if waited_ms is not None else ""
        super().__init__(
            f"Writes paused for tenant {tenant_id}; timeout after {timeout}s{waited_info}"
        )


@dataclass
class PauseState:
    """
    Internal state for a paused tenant.

    Tracks the pause event, start time, and waiting writer count for
    a single tenant's pause state.

    Attributes:
        event: The asyncio.Event used to signal resume.
        started_at: When the pause began (monotonic time for duration).
        started_at_utc: When the pause began (UTC for logging).
        waiting_count: Number of writers currently waiting.
    """

    event: asyncio.Event = field(default_factory=asyncio.Event)
    started_at: float = field(default_factory=time.perf_counter)
    started_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))
    waiting_count: int = 0


@dataclass(frozen=True)
class PauseMetrics:
    """
    Metrics for a completed pause operation.

    Captures timing and waiter information for observability and
    performance monitoring.

    Attributes:
        tenant_id: The tenant UUID.
        duration_ms: How long the pause lasted in milliseconds.
        started_at: When the pause began (UTC).
        ended_at: When the pause ended (UTC).
        max_waiters: Maximum number of concurrent waiters observed.
        total_waiters: Total number of wait operations during pause.
    """

    tenant_id: UUID
    duration_ms: float
    started_at: datetime
    ended_at: datetime
    max_waiters: int = 0
    total_waiters: int = 0

    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        return self.duration_ms / 1000.0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for logging/serialization.

        Returns:
            Dictionary representation of the metrics.
        """
        return {
            "tenant_id": str(self.tenant_id),
            "duration_ms": self.duration_ms,
            "duration_seconds": self.duration_seconds,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "max_waiters": self.max_waiters,
            "total_waiters": self.total_waiters,
        }


__all__ = [
    "PauseMetrics",
    "PauseState",
    "WritePausedError",
]
