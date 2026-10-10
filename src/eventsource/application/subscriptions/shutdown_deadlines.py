"""
Deadline and timeout adjustment management for graceful shutdown.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
- TASK-0008 (Decompose Monolithic Modules SubscriptionManager and Shutdown)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


class ShutdownDeadlineManager:
    """
    Manages shutdown deadlines and proportional timeout allocation.
    """

    def __init__(self, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self._shutdown_deadline: datetime | None = None

    @property
    def deadline(self) -> datetime | None:
        """Get the current shutdown deadline."""
        return self._shutdown_deadline

    def set_shutdown_deadline(self, deadline: datetime) -> None:
        """
        Set absolute deadline for shutdown completion.

        Args:
            deadline: Absolute datetime by which shutdown must complete.
        """
        if deadline.tzinfo is None:
            logger.warning(
                "Shutdown deadline is not timezone-aware, assuming UTC",
                extra={"deadline": deadline.isoformat()},
            )
            deadline = deadline.replace(tzinfo=UTC)

        self._shutdown_deadline = deadline

        remaining = self.get_remaining_shutdown_time()
        logger.info(
            "Shutdown deadline set",
            extra={
                "deadline": deadline.isoformat(),
                "remaining_seconds": remaining,
            },
        )

    def get_remaining_shutdown_time(self) -> float:
        """
        Get seconds remaining until shutdown deadline.

        If no deadline is set, returns the configured timeout.
        Returns 0 if deadline has passed.
        """
        if self._shutdown_deadline is None:
            return self.timeout

        now = datetime.now(UTC)
        remaining = (self._shutdown_deadline - now).total_seconds()
        return max(0.0, remaining)

    def get_adjusted_timeouts(
        self, drain_timeout: float, checkpoint_timeout: float
    ) -> tuple[float, float, float]:
        """
        Calculate adjusted phase timeouts based on remaining time.

        Returns proportionally adjusted timeouts for:
        - stop phase
        - drain phase
        - checkpoint phase

        Returns:
            Tuple of (stop_timeout, drain_timeout, checkpoint_timeout)
        """
        remaining = self.get_remaining_shutdown_time()

        if remaining <= 0:
            logger.warning("No time remaining for shutdown, returning zero timeouts")
            return (0.0, 0.0, 0.0)

        default_stop_timeout = 5.0
        total_configured = default_stop_timeout + drain_timeout + checkpoint_timeout

        if remaining >= total_configured:
            return (default_stop_timeout, drain_timeout, checkpoint_timeout)

        min_stop = 1.0
        min_checkpoint = 2.0

        if remaining < (min_stop + min_checkpoint):
            logger.warning(
                "Critical time constraint, skipping drain phase",
                extra={"remaining_seconds": remaining},
            )
            return (min(remaining * 0.2, 1.0), 0.0, remaining * 0.8)

        adjusted_stop = max(min_stop, remaining * 0.1)
        adjusted_checkpoint = max(min_checkpoint, remaining * 0.3)
        adjusted_drain = remaining - adjusted_stop - adjusted_checkpoint

        logger.info(
            "Adjusted shutdown timeouts for deadline",
            extra={
                "remaining_seconds": remaining,
                "stop_timeout": adjusted_stop,
                "drain_timeout": adjusted_drain,
                "checkpoint_timeout": adjusted_checkpoint,
            },
        )

        return (adjusted_stop, adjusted_drain, adjusted_checkpoint)

    def reset(self) -> None:
        """Reset deadline state."""
        self._shutdown_deadline = None
