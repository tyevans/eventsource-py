"""
Sync lag tracking data models, statistics, and trace constants.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eventsource.ports.migration.models import SyncLag

# Custom attribute keys for sync lag tracing
ATTR_LAG_EVENTS = "eventsource.sync_lag.lag_events"
ATTR_IS_CONVERGED = "eventsource.sync_lag.is_converged"
ATTR_IS_SYNC_READY = "eventsource.sync_lag.is_sync_ready"
ATTR_SYNC_THRESHOLD = "eventsource.sync_lag.sync_threshold"


@dataclass
class LagSample:
    """
    A single lag measurement sample.

    Attributes:
        lag: The SyncLag measurement.
        sampled_at: When the sample was taken.
    """

    lag: SyncLag
    sampled_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class LagStats:
    """
    Aggregate statistics about sync lag over time.

    Provides summary metrics for monitoring sync lag trends.

    Attributes:
        current_lag: Most recent lag measurement (events).
        average_lag: Average lag over the sample window.
        max_lag: Maximum lag observed in the sample window.
        min_lag: Minimum lag observed in the sample window.
        sample_count: Number of samples in the window.
        first_sample_at: Timestamp of the first sample.
        last_sample_at: Timestamp of the most recent sample.
        is_converging: Whether lag is trending downward.
    """

    current_lag: int
    average_lag: float
    max_lag: int
    min_lag: int
    sample_count: int
    first_sample_at: datetime | None
    last_sample_at: datetime | None
    is_converging: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "current_lag": self.current_lag,
            "average_lag": self.average_lag,
            "max_lag": self.max_lag,
            "min_lag": self.min_lag,
            "sample_count": self.sample_count,
            "first_sample_at": (self.first_sample_at.isoformat() if self.first_sample_at else None),
            "last_sample_at": (self.last_sample_at.isoformat() if self.last_sample_at else None),
            "is_converging": self.is_converging,
        }


__all__ = [
    "ATTR_IS_CONVERGED",
    "ATTR_IS_SYNC_READY",
    "ATTR_LAG_EVENTS",
    "ATTR_SYNC_THRESHOLD",
    "LagSample",
    "LagStats",
]
