"""
Shutdown models, phases, and results.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ShutdownPhase(Enum):
    """
    Phases of graceful shutdown.

    The shutdown sequence follows these phases:
    1. RUNNING: Normal operation, no shutdown requested
    2. STOPPING: Stop accepting new events
    3. DRAINING: Drain in-flight events
    4. CHECKPOINTING: Save final checkpoints
    5. STOPPED: Graceful shutdown complete
    6. FORCED: Forced shutdown (timeout or double signal)
    """

    RUNNING = "running"
    """Normal operation, no shutdown requested."""

    STOPPING = "stopping"
    """Stop accepting new events."""

    DRAINING = "draining"
    """Draining in-flight events."""

    CHECKPOINTING = "checkpointing"
    """Saving final checkpoints."""

    STOPPED = "stopped"
    """Graceful shutdown complete."""

    FORCED = "forced"
    """Forced shutdown due to timeout or double signal."""


class ShutdownReason(Enum):
    """
    Reason for shutdown initiation.

    Tracks what triggered the shutdown sequence, useful for
    debugging, metrics, and operational visibility.
    """

    SIGNAL_SIGTERM = "signal_sigterm"
    """Shutdown triggered by SIGTERM signal (Kubernetes, container orchestrators)."""

    SIGNAL_SIGINT = "signal_sigint"
    """Shutdown triggered by SIGINT signal (Ctrl+C, interactive termination)."""

    PROGRAMMATIC = "programmatic"
    """Shutdown triggered by application code via request_shutdown()."""

    HEALTH_CHECK = "health_check"
    """Shutdown triggered by health check failure (future)."""

    TIMEOUT = "timeout"
    """Shutdown forced due to overall timeout expiration."""

    DOUBLE_SIGNAL = "double_signal"
    """Shutdown forced by receiving second termination signal."""


@dataclass(frozen=True)
class ShutdownResult:
    """
    Result of shutdown operation.

    Provides comprehensive details about the shutdown process including
    timing, counts, reason, and whether the shutdown was forced.

    Attributes:
        phase: Final shutdown phase reached
        duration_seconds: Total shutdown duration in seconds
        subscriptions_stopped: Number of subscriptions stopped
        events_drained: Number of events drained during shutdown
        checkpoints_saved: Number of checkpoints saved
        forced: True if shutdown was forced (timeout or double signal)
        error: Error message if shutdown failed, None otherwise
        reason: The reason that triggered the shutdown
        in_flight_at_start: Number of in-flight events when shutdown started
        events_not_drained: Number of events that could not be drained
    """

    phase: ShutdownPhase
    duration_seconds: float
    subscriptions_stopped: int
    events_drained: int
    checkpoints_saved: int = 0
    forced: bool = False
    error: str | None = None
    reason: ShutdownReason | None = None
    in_flight_at_start: int = 0
    events_not_drained: int = 0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation of result
        """
        return {
            "phase": self.phase.value,
            "duration_seconds": self.duration_seconds,
            "subscriptions_stopped": self.subscriptions_stopped,
            "events_drained": self.events_drained,
            "checkpoints_saved": self.checkpoints_saved,
            "forced": self.forced,
            "error": self.error,
            "reason": self.reason.value if self.reason else None,
            "in_flight_at_start": self.in_flight_at_start,
            "events_not_drained": self.events_not_drained,
        }


__all__ = [
    "ShutdownPhase",
    "ShutdownReason",
    "ShutdownResult",
]
